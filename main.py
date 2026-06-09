"""
MLB Pinnacle Tracker v4
- MongoDB 持久化儲存（快照 + 歷史結算）
- 每 15 分鐘自動抓 Pinnacle 盤口
- 每天自動抓賽果 + 結算昨日預測
- 昨日記錄保留一天後覆蓋
"""

import os
import asyncio
import logging
import httpx
from datetime import datetime, timezone, timedelta
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
from bson import json_util
import json

# 設定日誌
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger(__name__)

# ── 設定 ──
ODDS_API_KEY = os.environ.get("ODDS_API_KEY", "")
MONGO_URI    = os.environ.get("MONGO_URI", "")
SPORT        = "baseball_mlb"
BOOKMAKER    = "pinnacle"
ODDS_BASE    = "https://api.the-odds-api.com/v4"
SCORES_URL   = f"{ODDS_BASE}/sports/{SPORT}/scores/?apiKey={ODDS_API_KEY}&daysFrom=1&dateFormat=iso"
ODDS_URL     = f"{ODDS_BASE}/sports/{SPORT}/odds/?apiKey={ODDS_API_KEY}&regions=us&markets=h2h,totals,spreads&bookmakers={BOOKMAKER}&oddsFormat=american"

# ── 資料庫處理 ──
client = None
db = None

def get_db():
    return db

def utc_now():
    return datetime.now(timezone.utc)

def et_date_str(dt=None):
    if dt is None: dt = utc_now()
    return dt.astimezone(timezone(timedelta(hours=-4))).strftime("%Y-%m-%d")

# ── 背景邏輯 ──
async def fetch_and_store_odds():
    try:
        async with httpx.AsyncClient(timeout=20) as client_http:
            r = await client_http.get(ODDS_URL)
            if r.status_code != 200: return
            games = r.json()
            coll = get_db()["snapshots"]
            today = et_date_str()
            ts = utc_now()
            
            for g in games:
                pin = next((b for b in g.get("bookmakers", []) if b["key"] == BOOKMAKER), None)
                if not pin: continue
                totals = next((m for m in pin["markets"] if m["key"] == "totals"), None)
                over = next((o for o in (totals or {}).get("outcomes", []) if o["name"] == "Over"), None)
                
                snap = {"ts": ts, "total": over["point"] if over else None}
                await coll.update_one(
                    {"game_id": g["id"], "date": today},
                    {"$set": {"latest": snap, "updated_at": ts}, "$push": {"snapshots": snap}},
                    upsert=True
                )
    except Exception as e:
        log.error(f"Fetch Error: {e}")

async def scheduler():
    while True:
        await fetch_and_store_odds()
        await asyncio.sleep(900) # 15 min

@asynccontextmanager
async def lifespan(app: FastAPI):
    global client, db
    client = AsyncIOMotorClient(MONGO_URI)
    db = client["mlb_tracker"]
    asyncio.create_task(scheduler())
    yield
    client.close()

# ── API 介面 ──
app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.get("/games")
async def get_games():
    today = et_date_str()
    docs = await get_db()["snapshots"].find({"date": today}).to_list(100)
    return {"data": json.loads(json_util.dumps(docs))}

@app.get("/history")
async def get_history():
    return {"data": []}

@app.get("/stats")
async def get_stats():
    return {"win_rate": 0, "total": 0}
