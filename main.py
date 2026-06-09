"""
MLB Pinnacle Tracker v4
- MongoDB 持久化儲存（快照 + 歷史結算）
- 每 60 分鐘自動抓 Pinnacle 盤口
- 每天自動抓賽果 + 結算昨日預測
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
    log.error("CRITICAL: ODDS_API_KEY NOT SET!")

MONGO_URI = os.getenv("MONGO_URI", "")
SPORT = "baseball_mlb"
BOOKMAKER = "pinnacle"
BASE_URL = "https://api.the-odds-api.com/v4/sports/" + SPORT

# ── MongoDB ───────────────────────────────────────────────────────────────────
client, db = None, None
def get_db(): return db

# ── Helpers ───────────────────────────────────────────────────────────────────
def utc_now(): return datetime.now(timezone.utc)
def et_date_str(dt=None):
    if dt is None: dt = utc_now()
    return dt.astimezone(timezone(timedelta(hours=-4))).strftime("%Y-%m-%d")

def signal_from_snaps(snaps: list) -> dict:
    if len(snaps) < 2: return {"total": "FLAT", "ml": "FLAT", "delta": 0}
    first, last = snaps[0], snaps[-1]
    td = round((last.get("total") or 0) - (first.get("total") or 0), 1)
    return {"total": "STEAM" if abs(td)>=0.5 else "FLAT", "ml": "FLAT", "delta": td}

def pick_from_signal(sig: dict, game: dict) -> str | None:
    d = sig.get("delta", 0)
    total = game.get("latest", {}).get("total")
    if d >= 0.25: return f"OVER {total}"
    if d <= -0.25: return f"UNDER {total}"
    return None

# ── Fetch Data ────────────────────────────────────────────────────────────────
async def fetch_api(endpoint: str):
    async with httpx.AsyncClient(timeout=30) as client:
        params = {"apiKey": ODDS_API_KEY, "regions": "us", "markets": "h2h,totals,spreads", "bookmakers": BOOKMAKER, "oddsFormat": "american"}
        url = f"{BASE_URL}/{endpoint}/"
        if endpoint == "scores":
            params = {"apiKey": ODDS_API_KEY, "daysFrom": "1", "dateFormat": "iso"}
        r = await client.get(url, params=params)
        return r

async def fetch_and_store_odds():
    try:
        r = await fetch_api("odds")
        if r.status_code != 200: return
        games = r.json()
        ts, today, coll = utc_now(), et_date_str(), get_db()["snapshots"]
        for g in games:
            pin = next((b for b in g.get("bookmakers", []) if b["key"] == BOOKMAKER), None)
            if not pin: continue
            totals = next((m for m in pin["markets"] if m["key"] == "totals"), None)
            over = next((o for o in (totals or {}).get("outcomes", []) if o["name"] == "Over"), None)
            snap = {"ts": ts, "total": over["point"] if over else None}
            existing = await coll.find_one({"game_id": g["id"], "date": today})
            if existing:
                await coll.update_one({"_id": existing["_id"]}, {"$set": {"snapshots": existing["snapshots"][-49:] + [snap], "latest": snap, "updated_at": ts}})
            else:
                await coll.insert_one({"game_id": g["id"], "date": today, "home": g["home_team"], "away": g["away_team"], "commence_time": g["commence_time"], "snapshots": [snap], "latest": snap})
        log.info("✅ Odds stored")
    except Exception as e: log.error(f"fetch_odds error: {e}")

async def fetch_and_settle():
    try:
        r = await fetch_api("scores")
        if r.status_code != 200: return
        scores = r.json()
        yesterday = et_date_str(utc_now() - timedelta(days=1))
        for s in scores:
            if not s.get("completed"): continue
            game_doc = await get_db()["snapshots"].find_one({"game_id": s["id"], "date": yesterday})
            if game_doc:
                await get_db()["history"].update_one({"game_id": s["id"], "date": yesterday}, {"$set": {"result": "SETTLED"}}, upsert=True)
        log.info("✅ Settled games")
    except Exception as e: log.error(f"settle error: {e}")

# ── Scheduler & App Instance ──────────────────────────────────────────────────
async def scheduler():
    while True:
        await fetch_and_store_odds()
        await fetch_and_settle()
        await asyncio.sleep(60 * 60)

@asynccontextmanager
async def lifespan(app: FastAPI):
    global client, db
    client = AsyncIOMotorClient(MONGO_URI)
    db = client["mlb_tracker"]
    asyncio.create_task(scheduler())
    yield
    client.close()

# 實例化 app (放在所有路由之前)
app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ── Routes (放在檔案最底部) ───────────────────────────────────────────────────
@app.get("/")
async def root(): return {"status": "ok"}

@app.get("/games")
async def get_games(): return await get_db()["snapshots"].find({"date": et_date_str()}).to_list(50)

@app.get("/history")
async def get_history(): return await get_db()["history"].find().to_list(30)

@app.get("/stats")
async def get_stats():
    docs = await get_db()["history"].find({"result": {"$in": ["WIN","LOSS","PUSH"]}}).to_list(200)
    wins = sum(1 for d in docs if d.get("result")=="WIN")
    losses = sum(1 for d in docs if d.get("result")=="LOSS")
    return {"win_rate": round(wins/(wins+losses)*100,1) if (wins+losses)>0 else 0}
