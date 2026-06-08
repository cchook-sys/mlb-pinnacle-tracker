import os
import asyncio
import httpx
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime
from bson import json_util
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

# MongoDB 連線設定
MONGO_URI = os.environ.get("MONGO_URI")
client = AsyncIOMotorClient(MONGO_URI)
db = client.mlb_tracker
history_col = db.odds_history

# 從 Render 環境變數讀取 API KEY
ODDS_API_KEY = os.environ.get("ODDS_API_KEY")

async def fetch_odds_task():
    while True:
        try:
            print(f"[{datetime.now()}] 開始執行背景抓取...")
            
            if not ODDS_API_KEY:
                print("錯誤：未偵測到 ODDS_API_KEY，請檢查 Render 環境變數")
                return

            async with httpx.AsyncClient() as client_http:
                # 測試 MLB 賠率 API
                url = f"https://api.the-odds-api.com/v4/sports/baseball_mlb/odds/?apiKey={ODDS_API_KEY}&regions=us&markets=h2h"
                response = await client_http.get(url)
                
                if response.status_code == 200:
                    data = response.json()
                    if data:
                        # 將舊資料移除避免重複，或直接插入新資料
                        await history_col.insert_many(data)
                        print(f"成功寫入 {len(data)} 筆資料到 MongoDB")
                    else:
                        print("API 回傳資料為空")
                else:
                    print(f"API 請求失敗，狀態碼: {response.status_code}")
                    
        except Exception as e:
            print(f"背景任務錯誤: {e}")
            
        await asyncio.sleep(600) # 每 10 分鐘抓一次

@app.on_event("startup")
async def startup_event():
    print(">>> 系統啟動：開始初始化背景任務...")
    asyncio.create_task(fetch_odds_task())

@app.get("/games")
async def get_games():
    # 撈取最新資料
    cursor = history_col.find().sort("_id", -1).limit(20)
    data = await cursor.to_list(length=20)
    return {"data": json.loads(json_util.dumps(data))}

@app.get("/")
def read_root():
    return {"message": "MLB Tracker API 運作中"}
