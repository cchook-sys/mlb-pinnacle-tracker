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
from datetime import datetime
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
from bson import json_util
import json

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

ODDS_API_KEY = os.environ.get("ODDS_API_KEY", "")
MONGO_URI = os.environ.get("MONGO_URI", "")
ODDS_URL = f"https://api.the-odds-api.com/v4/sports/baseball_mlb/odds/?apiKey={ODDS_API_KEY}&regions=us&markets=h2h,totals&bookmakers=pinnacle&oddsFormat=american"

client = None
db = None

async def fetch_and_store_odds():
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.get(ODDS_URL)
            if r.status_code != 200: return
            data = r.json()
            
            coll = db["snapshots"]
            today = datetime.now().strftime("%Y-%m-%d")
            
            for g in data:
                # 提取 Pinnacle 的盤口資料
                pin = next((b for b in g.get("bookmakers", []) if b["key"] == "pinnacle"), None)
                if not pin: continue
                
                markets = pin.get("markets", [])
                totals = next((m for m in markets if m["key"] == "totals"), {"outcomes": []})
                h2h = next((m for m in markets if m["key"] == "h2h"), {"outcomes": []})
                
                # 整理成簡潔結構
                structured_data = {
                    "home": g["home_team"],
                    "away": g["away_team"],
                    "commence_time": g["commence_time"],
                    "total": next((o["point"] for o in totals["outcomes"] if o["name"] == "Over"), 0),
                    "over_price": next((o["price"] for o in totals["outcomes"] if o["name"] == "Over"), 0),
                    "under_price": next((o["price"] for o in totals["outcomes"] if o["name"] == "Under"), 0),
                    "ml_home": next((o["price"] for o in h2h["outcomes"] if o["name"] == g["home_team"]), 0),
                    "ml_away": next((o["price"] for o in h2h["outcomes"] if o["name"] == g["away_team"]), 0),
                }
                
                await coll.update_one(
                    {"game_id": g["id"], "date": today},
                    {"$set": {"data": structured_data, "updated_at": datetime.now()}},
                    upsert=True
                )
    except Exception as e:
        log.error(f"Error: {e}")

@asynccontextmanager
async def lifespan(app: FastAPI):
    global client, db
    client = AsyncIOMotorClient(MONGO_URI)
    db = client["mlb_tracker"]
    asyncio.create_task(scheduler())
    yield
    client.close()

async def scheduler():
    while True:
        await fetch_and_store_odds()
        await asyncio.sleep(900)

app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.get("/games")
async def get_games():
    docs = await db["snapshots"].find({"date": datetime.now().strftime("%Y-%m-%d")}).to_list(100)
    return {"data": json.loads(json_util.dumps(docs))}
