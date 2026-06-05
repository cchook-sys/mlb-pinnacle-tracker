import os
import time
import httpx
from datetime import datetime, timezone, timedelta
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import pymongo
import certifi

ODDS_API_KEY = "5a02e608035ba7b2c5da994b791fc6f4"
SPORT        = "baseball_mlb"
BOOKMAKER    = "pinnacle"
MARKETS      = "h2h,totals"
ODDS_FORMAT  = "american"
BASE_URL     = "https://api.the-odds-api.com/v4"

MONGO_URI = "mongodb+srv://ccanthook:surfing135%3D@cluster0.cinyz41.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

try:
    client = pymongo.MongoClient(MONGO_URI, tlsCAFile=certifi.where())
    db = client["mlb_tracker"]
    snaps_col = db["snapshots"]
    results_col = db["results"]
    print("MongoDB Connected Successfully")
except Exception as e:
    print(f"MongoDB Connection Failed: {e}")

async def fetch_and_store():
    if not ODDS_API_KEY:
        return

    url = f"{BASE_URL}/sports/{SPORT}/odds/?apiKey={ODDS_API_KEY}&regions=us&markets={MARKETS}&bookmakers={BOOKMAKER}&oddsFormat={ODDS_FORMAT}"

    try:
        async with httpx.AsyncClient(timeout=15) as async_client:
            res = await async_client.get(url)
            if res.status_code != 200:
                return

            games = res.json()
            ts    = int(time.time())
            stored = 0

            for game in games:
                pin = next((b for b in game.get("bookmakers", []) if b["key"] == BOOKMAKER), None)
                if not pin:
                    continue

                totals = next((m for m in pin["markets"] if m["key"] == "totals"), None)
                h2h    = next((m for m in pin["markets"] if m["key"] == "h2h"), None)

                over    = next((o for o in (totals or {}).get("outcomes", []) if o["name"] == "Over"), None)
                ml_home = next((o for o in (h2h or {}).get("outcomes", []) if o["name"] == game["home_team"]), None)

                snap = {
                    "game_id":      game["id"],
                    "home":         game["home_team"],
                    "away":         game["away_team"],
                    "commence_time": game["commence_time"],
                    "ts":           ts,
                    "ts_iso":       datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(),
                    "total":        over["point"] if over else None,
                    "over_juice":   over["price"] if over else None,
                    "ml_home":      ml_home["price"] if ml_home else None,
                }

                last_snap = snaps_col.find_one({"game_id": game["id"]}, sort=[("ts", pymongo.DESCENDING)])
                has_changed = not last_snap or last_snap.get("total") != snap["total"] or last_snap.get("ml_home") != snap["ml_home"]

                if has_changed or (last_snap and (ts - last_snap["ts"]) >= 7200):
                    snaps_col.insert_one(snap)
                    stored += 1

            print(f"Fetch completed. Stored {stored} snapshots.")
    except Exception as e:
        print(f"Fetch job failed: {e}")

async def fetch_and_settle_results():
    if not ODDS_API_KEY:
        return

    url = f"{BASE_URL}/sports/{SPORT}/scores/?apiKey={ODDS_API_KEY}&daysFrom=3"

    try:
        async with httpx.AsyncClient(timeout=15) as async_client:
            res = await async_client.get(url)
            if res.status_code != 200:
                return

            completed_games = res.json()
            settled_count = 0

            for game in completed_games:
                if not game.get("completed", False):
                    continue
                gid = game["id"]
                
                if results_col.find_one({"game_id": gid}):
                    continue

                scores = game.get("scores", [])
                if not scores or len(scores) < 2:
                    continue

                home_score = next((int(s["score"]) for s in scores if s["name"] == game["home_team"]), None)
                away_score = next((int(s["score"]) for s in scores if s["name"] == game["away_team"]), None)
                if home_score is None or away_score is None:
                    continue
                
                total_outcome_score = home_score + away_score

                snaps = list(snaps_col.find({"game_id": gid}, {"_id": 0}).sort("ts", pymongo.ASCENDING))
                if not snaps:
                    continue

                first_snap = snaps[0]
                last_snap  = snaps[-1]

                ml_winner = "HOME" if home_score > away_score else "AWAY"
                opening_total = first_snap.get("total")
                closing_total = last_snap.get("total")

                opening_total_result = "PUSH"
                if opening_total:
                    if total_outcome_score > opening_total:
                        opening_total_result = "OVER"
                    elif total_outcome_score < opening_total:
                        opening_total_result = "UNDER"

                closing_total_result = "PUSH"
                if closing_total:
                    if total_outcome_score > closing_total:
                        closing_total_result = "OVER"
                    elif total_outcome_score < closing_total:
                        closing_total_result = "UNDER"

                result_doc = {
                    "game_id": gid,
                    "home": game["home_team"],
                    "away": game["away_team"],
                    "commence_time": game["commence_time"],
                    "home_score": home_score,
                    "away_score": away_score,
                    "total_score": total_outcome_score,
                    "ml_winner": ml_winner,                
                    "opening_total": opening_total,         
                    "opening_total_result": opening_total_result,
                    "closing_total": closing_total,         
                    "closing_total_result": closing_total_result,
                    "opening_ml_home": first_snap.get("ml_home"),
                    "closing_ml_home": last_snap.get("ml_home"),
                    "updated_at": datetime.now(timezone.utc).isoformat()
                }

                results_col.insert_one(result_doc)
                settled_count += 1

            time_boundary = (datetime.now(timezone.utc) - timedelta(hours=48)).isoformat()
            results_col.delete_many({"commence_time": {"$lt": time_boundary}})
            snaps_col.delete_many({"commence_time": {"$lt": time_boundary}})
            print(f"Settle completed. Added {settled_count} rows. Overdated rows purged.")
    except Exception as e:
        print(f"Settle job failed: {e}")

scheduler = AsyncIOScheduler()

@asynccontextmanager
async def app_lifespan(app_instance: FastAPI):
    await fetch_and_store()
    scheduler.add_job(fetch_and_store, "interval", minutes=10, id="pinnacle_fetch")
    scheduler.add_job(fetch_and_settle_results, "interval", minutes=30, id="results_settle")
    scheduler.start()
    yield
    scheduler.shutdown()

app = FastAPI(title="MLB Pinnacle Tracker", lifespan=app_lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"]
)

@app.get("/")
def root():
    return {"status": "ok", "service": "MLB FastAPI Core V9 Clean"}

@app.get("/games")
async def get_games():
    await fetch_and_store()
    
    now = datetime.now(timezone.utc)
    start_filter = (now - timedelta(hours=12)).isoformat()
    end_filter = (now + timedelta(hours=12)).isoformat()
    
    all_snaps = list(snaps_col.find({
        "commence_time": {"$gte": start_filter, "$lte": end_filter}
    }, {"_id": 0}))

    games = {}
    for s in all_snaps:
        gid = s["game_id"]
        if gid not in games:
            games[gid] = []
        games[gid].append(s)

    result = []
    for gid, snaps in games.items():
        snaps_sorted = sorted(snaps, key=lambda x: x["ts"])
        latest = snaps_sorted[-1]
        first  = snaps_sorted[0]

        total_delta = round(float(latest.get("total", 0)) - float(first.get("total", 0)), 2) if (latest.get("total") is not None and first.get("total") is not None) else 0
        ml_home_delta = int(latest.get("ml_home", 0)) - int(first.get("ml_home", 0)) if (latest.get("ml_home") is not None and first.get("ml_home") is not None) else 0

        result.append({
            "game_id":       gid,
            "home":          latest["home"],
            "away":          latest["away"],
            "commence_time": latest["commence_time"],
            "snapshot_count": len(snaps_sorted),
            "first_snap_ts": first.get("ts_iso"),
            "latest": {
                "ts":          latest.get("ts_iso"),
                "total":       latest.get("total"),
                "ml_home":     latest.get("ml_home"),
            },
            "open": {
                "total":   first.get("total"),
                "ml_home": first.get("ml_home"),
            },
            "delta": {
                "total":   total_delta,
                "ml_home": ml_home_delta,
            },
            "signal": {
                "total": "STEAM_OVER" if total_delta >= 0.5 else ("STEAM_UNDER" if total_delta <= -0.5 else "FLAT"),
                "ml":    "STEAM_HOME" if ml_home_delta <= -15 else ("STEAM_AWAY" if ml_home_delta >= 15 else "FLAT"),
            },
            "history": [
                {
                    "ts":       s.get("ts_iso"),
                    "total":    s.get("total"),
                    "ml_home":  s.get("ml_home"),
                }
                for s in snaps_sorted
            ],
        })

    result.sort(key=lambda x: x["commence_time"])
    return result

@app.get("/analytics/dataset")
async def get_training_dataset():
    await fetch_and_settle_results()
    results = list(results_col.find({}, {"_id": 0}).sort("commence_time", -1))
    dataset = []
    
    for r in results:
        open_t = r.get("opening_total", 0) or 0
        close_t = r.get("closing_total", 0) or 0
        delta_t = round(close_t - open_t, 2)

        dataset.append({
            "game_id": r["game_id"],
            "home": r["home"],
            "away": r["away"],
            "commence_time": r["commence_time"],
            "opening_total": r.get("opening_total"),
            "closing_total": r.get("closing_total"),
            "total_changed_delta": delta_t,
            "opening_ml_home": r.get("opening_ml_home"),
            "closing_ml_home": r.get("closing_ml_home"),
            "final_home_score": r["home_score"],
            "final_away_score": r["away_score"],
            "final_total_score": r["total_score"],
            "ml_winner_result": r["ml_winner"],
            "opening_total_result": r["opening_total_result"],
            "closing_total_result": r["closing_total_result"]
        })
    return dataset
