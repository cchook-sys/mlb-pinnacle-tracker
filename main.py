"""
MLB Pinnacle Tracker v4
- MongoDB 持久化儲存（快照 + 歷史結算）
- 每 60 分鐘自動抓 Pinnacle 盤口 (已調整為 60 分鐘以節省點數)
- 每天自動抓賽果 + 結算昨日預測
- 昨日記錄保留一天後覆蓋
"""

import os, time, asyncio, logging
from datetime import datetime, timezone, timedelta
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
ODDS_API_KEY = os.environ.get("ODDS_API_KEY")

if not ODDS_API_KEY:
    log.error("CRITICAL: ODDS_API_KEY environment variable is NOT set!")
else:
    log.info(f"Using API Key: {ODDS_API_KEY[:4]}****")

MONGO_URI     = os.getenv("MONGO_URI", "")
SPORT         = "baseball_mlb"
BOOKMAKER     = "pinnacle"
ODDS_BASE     = "https://api.the-odds-api.com/v4"
SCORES_URL    = f"{ODDS_BASE}/sports/{SPORT}/scores/?apiKey={ODDS_API_KEY}&daysFrom=1&dateFormat=iso"
ODDS_URL      = (f"{ODDS_BASE}/sports/{SPORT}/odds/"
                 f"?apiKey={ODDS_API_KEY}&regions=us"
                 f"&markets=h2h,totals,spreads&bookmakers={BOOKMAKER}&oddsFormat=american")

# ── MongoDB ───────────────────────────────────────────────────────────────────
client  = None
db      = None

def get_db():
    return db

# ── Helpers ───────────────────────────────────────────────────────────────────
def utc_now():
    return datetime.now(timezone.utc)

def et_date_str(dt=None):
    if dt is None:
        dt = utc_now()
    et = dt.astimezone(timezone(timedelta(hours=-4)))
    return et.strftime("%Y-%m-%d")

def signal_from_snaps(snaps: list) -> dict:
    if len(snaps) < 2:
        return {"total": "FLAT", "ml": "FLAT", "delta": 0}
    first, last = snaps[0], snaps[-1]
    td = round((last.get("total") or 0) - (first.get("total") or 0), 1)
    md = (last.get("ml_home") or 0) - (first.get("ml_home") or 0)
    total_sig = ("STEAM_OVER" if td >= 0.5 else "LEAN_OVER" if td >= 0.25 else
                 "STEAM_UNDER" if td <= -0.5 else "LEAN_UNDER" if td <= -0.25 else "FLAT")
    ml_sig = ("STEAM_HOME" if md <= -15 else "STEAM_AWAY" if md >= 15 else "FLAT")
    return {"total": total_sig, "ml": ml_sig, "delta": td}

def pick_from_signal(sig: dict, game: dict) -> str | None:
    d = sig.get("delta", 0)
    total = game.get("latest", {}).get("total")
    if d >= 0.5:  return f"OVER {total}"
    if d <= -0.5: return f"UNDER {total}"
    if d >= 0.25: return f"OVER {total} (lean)"
    if d <= -0.25:return f"UNDER {total} (lean)"
    return None

# ── Fetch Odds ────────────────────────────────────────────────────────────────
async def fetch_and_store_odds():
    if not ODDS_API_KEY:
        log.warning("No ODDS_API_KEY")
        return
    try:
        async with httpx.AsyncClient(timeout=20) as client_http:
            r = await client_http.get(ODDS_URL)
            if r.status_code != 200:
                log.error(f"Odds API error: {r.status_code}")
                return
            games = r.json()

        ts    = utc_now()
        today = et_date_str(ts)
        coll  = get_db()["snapshots"]
        stored = 0

        for g in games:
            pin = next((b for b in g.get("bookmakers", []) if b["key"] == BOOKMAKER), None)
            if not pin: continue
            totals = next((m for m in pin["markets"] if m["key"] == "totals"), None)
            h2h    = next((m for m in pin["markets"] if m["key"] == "h2h"), None)
            spreads= next((m for m in pin["markets"] if m["key"] == "spreads"), None)
            over   = next((o for o in (totals or {}).get("outcomes", []) if o["name"] == "Over"), None)
            under  = next((o for o in (totals or {}).get("outcomes", []) if o["name"] == "Under"), None)
            ml_home= next((o for o in (h2h or {}).get("outcomes", []) if o["name"] == g["home_team"]), None)
            ml_away= next((o for o in (h2h or {}).get("outcomes", []) if o["name"] == g["away_team"]), None)
            sp_home= next((o for o in (spreads or {}).get("outcomes", []) if o["name"] == g["home_team"]), None)

            snap = {
                "ts": ts,
                "total": over["point"] if over else None,
                "over_juice": over["price"] if over else None,
                "under_juice": under["price"] if under else None,
                "ml_home": ml_home["price"] if ml_home else None,
                "ml_away": ml_away["price"] if ml_away else None,
                "spread_home": sp_home["point"] if sp_home else None,
            }

            existing = await coll.find_one({"game_id": g["id"], "date": today})
            if existing:
                last_snap = existing["snapshots"][-1] if existing["snapshots"] else {}
                changed = (last_snap.get("total") != snap["total"] or last_snap.get("ml_home") != snap["ml_home"])
                age_mins = (ts - last_snap["ts"].replace(tzinfo=timezone.utc)).total_seconds() / 60 if last_snap.get("ts") else 9999
                if changed or age_mins >= 60: # 條件同步調整為 60 分鐘
                    snaps = existing["snapshots"][-49:] + [snap]
                    await coll.update_one({"_id": existing["_id"]}, {"$set": {"snapshots": snaps, "latest": snap, "signal": signal_from_snaps(snaps), "updated_at": ts}})
                    stored += 1
            else:
                await coll.insert_one({"game_id": g["id"], "date": today, "home": g["home_team"], "away": g["away_team"], "commence_time": g["commence_time"], "snapshots": [snap], "latest": snap, "open": snap, "signal": signal_from_snaps([snap]), "created_at": ts, "updated_at": ts})
                stored += 1
        log.info(f"✅ Stored {stored} snapshots")
    except Exception as e:
        log.error(f"fetch_odds error: {e}")

# ── Fetch Scores & Settle ─────────────────────────────────────────────────────
async def fetch_and_settle():
    if not ODDS_API_KEY: return
    try:
        async with httpx.AsyncClient(timeout=20) as client_http:
            r = await client_http.get(SCORES_URL)
            if r.status_code != 200: return
            scores = r.json()

        ts = utc_now()
        yesterday = et_date_str(ts - timedelta(days=1))
        snaps_coll, hist_coll = get_db()["snapshots"], get_db()["history"]
        settled = 0

        for s in scores:
            if not s.get("completed"): continue
            scores_data = s.get("scores") or []
            if len(scores_data) < 2: continue
            total_runs = sum(int(sc["score"]) for sc in scores_data)
            game_doc = await snaps_coll.find_one({"game_id": s["id"], "date": yesterday}) or await snaps_coll.find_one({"game_id": s["id"]})
            if not game_doc: continue

            snaps = game_doc.get("snapshots", [])
            sig = signal_from_snaps(snaps)
            pick = pick_from_signal(sig, game_doc)
            total = game_doc.get("open", {}).get("total")
            result = None
            if pick and total:
                if "OVER" in pick: result = "WIN" if total_runs > total else "LOSS" if total_runs < total else "PUSH"
                elif "UNDER" in pick: result = "WIN" if total_runs < total else "LOSS" if total_runs > total else "PUSH"

            hist_entry = {
                "game_id": s["id"], "date": yesterday, "home": game_doc["home"], "away": game_doc["away"],
                "commence_time": game_doc["commence_time"], "open_total": total,
                "close_total": game_doc.get("latest", {}).get("total"), "total_delta": sig.get("delta", 0),
                "signal": sig, "pick": pick, "actual_total": total_runs, "result": result, "settled_at": ts
            }
            await hist_coll.update_one({"game_id": s["id"], "date": yesterday}, {"$set": hist_entry}, upsert=True)
            await snaps_coll.update_one({"_id": game_doc["_id"]}, {"$set": {"result": result, "actual_total": total_runs}})
            settled += 1
        log.info(f"✅ Settled {settled} games")
    except Exception as e:
        log.error(f"settle error: {e}")

# ── Scheduler ─────────────────────────────────────────────────────────────────
async def scheduler():
    while True:
        await fetch_and_store_odds()
        await fetch_and_settle()
        # 修改為每 60 分鐘執行一次 (60 * 60 秒)
        await asyncio.sleep(60 * 60)

# ── App Startup ───────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    global client, db
    client = AsyncIOMotorClient(MONGO_URI)
    db = client["mlb_tracker"]
    await db["snapshots"].create_index([("game_id", 1), ("date", 1)], unique=True)
    await db["history"].create_index([("game_id", 1), ("date", 1)], unique=True)
    asyncio.create_task(scheduler())
    yield
    client.close()

app = FastAPI(title="MLB Pinnacle Tracker v4", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.get("/")
async def root(): return {"status": "ok"}

@app.get("/games")
async def get_games():
    today = et_date_str()
    docs = await get_db()["snapshots"].find({"date": today}).to_list(50)
    return docs

@app.get("/history")
async def get_history():
    yesterday = et_date_str(utc_now() - timedelta(days=1))
    return await get_db()["history"].find({"date": yesterday}).to_list(30)

@app.get("/stats")
async def get_stats():
    docs = await get_db()["history"].find({"result": {"$in": ["WIN","LOSS","PUSH"]}}).to_list(200)
    wins = sum(1 for d in docs if d.get("result")=="WIN")
    losses = sum(1 for d in docs if d.get("result")=="LOSS")
    return {"win_rate": round(wins/(wins+losses)*100,1) if (wins+losses)>0 else 0}
