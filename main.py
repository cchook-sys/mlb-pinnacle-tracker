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
    
    # 💡 【終極大絕招：全自動清空 MongoDB 雲端資料庫】
    # 開機時直接無條件清空 snapshots 與 results，徹底排除舊髒資料的干擾
    snaps_col.delete_many({})
    results_col.delete_many({})
    print("🧹 [資料庫清空令] 已成功將雲端 MongoDB 歷史殘留舊快照與賽果全數洗淨！")
except Exception as e:
    print(f"MongoDB Connection Failed: {e}")

async def fetch_and_store():
    if not ODDS_API_KEY:
        return []
    url = f"{BASE_URL}/sports/{SPORT}/odds/?apiKey={ODDS_API_KEY}&regions=us&markets={MARKETS}&bookmakers={BOOKMAKER}&oddsFormat={ODDS_FORMAT}"
    try:
        async with httpx.AsyncClient(timeout=15) as async_client:
            res = await async_client.get(url)
            if res.status_code != 200:
                return []
            games = res.json()
            ts    = int(time.time())

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
            return games
    except Exception as e:
        print(f"Fetch failed: {e}")
        return []

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
                    if total_outcome_score > opening_total: opening_total_result = "OVER"
                    elif total_outcome_score < opening_total: opening_total_result = "UNDER"

                closing_total_result = "PUSH"
                if closing_total:
                    if total_outcome_score > closing_total: closing_total_result = "OVER"
                    elif total_outcome_score < closing_total: closing_total_result = "UNDER"

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

            # 48小時滾動自動刪除過期賽果數據
            time_boundary = (datetime.now(timezone.utc) - timedelta(hours=48)).isoformat()
            results_col.delete_many({"commence_time": {"$lt": time_boundary}})
            snaps_col.delete_many({"commence_time": {"$lt": time_boundary}})
    except Exception as e:
        print(f"Settle failed: {e}")

scheduler = AsyncIOScheduler()

@asynccontextmanager
async def app_lifespan(app_instance: FastAPI):
    await fetch_and_store()
    scheduler.add_job(fetch_and_store, "interval", minutes=10, id="pinnacle_fetch")
    scheduler.add_job(fetch_and_settle_results, "interval", minutes=30, id="results_settle")
    scheduler.start()
    yield
    scheduler.shutdown()

app = FastAPI(title="MLB Pinnacle Tracker Pure-Live Edition", lifespan=app_lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"]
)

@app.get("/games")
async def get_games():
    # 當有人點擊時，現場強制去敲 odds API 重新向資料庫注入最純淨的當下活水
    await fetch_and_store()
    
    now = datetime.now(timezone.utc)
    # 開放時間過濾窗：從 12 小時前一直到未來 48 小時
    start_filter = (now - timedelta(hours=12)).isoformat()
    end_filter = (now + timedelta(hours=48)).isoformat()
    
    all_snaps = list(snaps_col.find({
        "commence_time": {"$gte": start_filter, "$lte": end_filter}
    }, {"_id": 0}))

    games = {}
    for s in all_snaps:
        gid = s["game_id"]
        if gid not in games:
            games[gid] = []
        games[gid].append(s)

    result_list = []
    for gid, snaps in games.items():
        snaps_sorted = sorted(snaps, key=lambda x: x["ts"])
        latest = snaps_sorted[-1]
        first  = snaps_sorted[0]

        total_delta = round(float(latest.get("total", 0)) - float(first.get("total", 0)), 2) if (latest.get("total") is not None and first.get("total") is not None) else 0
        ml_home_delta = int(latest.get("ml_home", 0)) - int(first.get("ml_home", 0)) if (latest.get("ml_home") is not None and first.get("ml_home") is not None) else 0

        result_list.append({
            "game_id":       gid,
            "home":          latest["home"],
            "away":          latest["away"],
            "commence_time": latest["commence_time"],
            "snapshot_count": len(snaps_sorted),
            "first_snap_ts": first.get("ts_iso"),
            "latest": {"ts": latest.get("ts_iso"), "total": latest.get("total"), "ml_home": latest.get("ml_home")},
            "open": {"total": first.get("total"), "ml_home": first.get("ml_home")},
            "delta": {"total": total_delta, "ml_home": ml_home_delta},
            "signal": {
                "total": "STEAM_OVER" if total_delta >= 0.5 else ("STEAM_UNDER" if total_delta <= -0.5 else "FLAT"),
                "ml":    "STEAM_HOME" if ml_home_delta <= -15 else ("STEAM_AWAY" if ml_home_delta >= 15 else "FLAT"),
            },
            "history": [{"ts": s.get("ts_iso"), "total": s.get("total"), "ml_home": s.get("ml_home")} for s in snaps_sorted],
        })

    result_list.sort(key=lambda x: x["commence_time"])
    
    tw_now = datetime.now(timezone(timedelta(hours=8)))
    timestamp_str = tw_now.strftime("%m/%d %H:%M:%S")
    
    return {
        "system_updated_at": timestamp_str,
        "data": result_list
    }

@app.get("/analytics/dataset")
async def get_training_dataset():
    results = list(results_col.find({}, {"_id": 0}).sort("commence_time", -1))
    dataset = []
    for r in results:
        open_t = r.get("opening_total", 0) or 0
        close_t = r.get("closing_total", 0) or 0
        dataset.append({
            "game_id": r["game_id"], "home": r["home"], "away": r["away"], "commence_time": r["commence_time"],
            "opening_total": r.get("opening_total"), "closing_total": r.get("closing_total"),
            "total_changed_delta": round(close_t - open_t, 2),
            "opening_ml_home": r.get("opening_ml_home"), "closing_ml_home": r.get("closing_ml_home"),
            "final_home_score": r["home_score"], "final_away_score": r["away_score"], "final_total_score": r["total_score"],
            "ml_winner_result": r["ml_winner"], "opening_total_result": r["opening_total_result"], "closing_total_result": r["closing_total_result"]
        })
    return dataset
