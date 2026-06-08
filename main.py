from fastapi import FastAPI
from motor.motor_asyncio import AsyncIOMotorClient
import httpx
import asyncio
import os
from datetime import datetime

# 1. 先宣告 app
app = FastAPI()

# 2. 再進行 MongoDB 設定
MONGO_URI = os.environ.get("MONGO_URI")
client = AsyncIOMotorClient(MONGO_URI)
db = client.mlb_tracker
history_col = db.odds_history

# 3. 定義你的任務函式
async def fetch_odds_task():
    # ... (這裡放你的抓取邏輯)
    pass

# 4. 最後才定義事件與路由
@app.on_event("startup")
async def startup_event():
    asyncio.create_task(fetch_odds_task())

@app.get("/")
def read_root():
    return {"status": "ok"}

@app.get("/games")
async def get_games():
    return {"data": []}

@app.get("/trigger-fetch")
async def trigger_fetch():
    return {"message": "正在執行..."}
