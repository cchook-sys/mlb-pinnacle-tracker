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
ODDS_URL     = f"https://api.the-odds-api.com/v4/sports/baseball_mlb/odds/?apiKey={ODDS_API_KEY}&regions=us&markets=h2h,totals,spreads&bookmakers=pinnacle&oddsFormat=american"

# ── 資料庫處理 ──
client = None
db = None

async def fetch_and_store_odds():
    try:
        log.info("開始抓取盤口數據...")
        async with httpx.AsyncClient(timeout=30) as client_http:
            r = await client_http.get(ODDS_URL)
            if r.status_code != 200:
                log.error(f"API 請求失敗: {r.status_code} - {r.text}")
                return
            
            data = r.json()
            if not data:
                log.warning("API 回傳資料為空")
                return

            coll = db["snapshots"]
            today = datetime.now().strftime("%Y-%m-%d")
            
            # 儲存資料到 MongoDB
            count = 0
            for g in data:
                # 簡單的儲存邏輯
                await coll.update_one(
                    {"game_id": g["id"], "date": today},
                    {"$set": {
                        "home": g["home_team"], 
                        "away": g["away_team"], 
                        "commence_time": g["commence_time"],
                        "latest": g["bookmakers"][0]["markets"],
                        "updated_at": datetime.now()
                    }},
                    upsert=True
                )
                count += 1
            
            log.info(f"✅ 成功儲存 {count} 筆賽事數據")
            
    except Exception as e:
        log.error(f"存儲錯誤: {e}")

@asynccontextmanager
async def lifespan(app: FastAPI):
    global client, db
    client = AsyncIOMotorClient(MONGO_URI)
    db = client["mlb_tracker"]
    log.info("✅ MongoDB 已連線")
    asyncio.create_task(scheduler())
    yield
    client.close()

async def scheduler():
    while True:
        await fetch_and_store_odds()
        await asyncio.sleep(900) # 每 15 分鐘

app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.get("/games")
async def get_games():
    today = datetime.now().strftime("%Y-%m-%d")
    docs = await db["snapshots"].find({"date": today}).to_list(100)
    return {"data": json.loads(json_util.dumps(docs))}
