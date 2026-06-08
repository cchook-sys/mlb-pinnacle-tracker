import os
import asyncio
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime
from bson import json_util # 確保能正確處理 MongoDB 的特殊格式
import json

app = FastAPI()

# 啟用 CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://cchook-sys.github.io"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# MongoDB 設定
MONGO_URI = os.environ.get("MONGO_URI")
client = AsyncIOMotorClient(MONGO_URI)
db = client.mlb_tracker
history_col = db.odds_history

# 背景任務：每 10 分鐘抓取一次
async def fetch_odds_task():
    while True:
        try:
            print(f"[{datetime.now()}] 開始執行背景抓取...")
            # --- 這裡放入你的實際抓取邏輯 ---
            # 確保資料有寫入 history_col
            # 範例測試寫入：
            # await history_col.insert_one({"away_team": "Dodgers", "home_team": "Giants", "time": datetime.now()})
            print("背景任務執行完畢")
        except Exception as e:
            print(f"背景任務錯誤: {e}")
        await asyncio.sleep(600)

@app.on_event("startup")
async def startup_event():
    print(">>> 系統啟動：開始初始化背景任務...")
    asyncio.create_task(fetch_odds_task())

@app.get("/games")
async def get_games():
    # 撈取最新 5 筆資料
    cursor = history_col.find().sort("_id", -1).limit(5)
    data = await cursor.to_list(length=5)
    
    # 除錯用：在 Render Log 印出撈到的資料
    print(f"後端撈到的資料: {data}")
    
    # 轉換 MongoDB 物件為 JSON 可讀格式
    return {"data": json.loads(json_util.dumps(data))}

@app.get("/trigger-fetch")
async def trigger_fetch():
    return {"message": "正在執行測試抓取..."}
