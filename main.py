import os
import asyncio
import httpx
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
from bson import json_util
import json

app = FastAPI()

# 啟用 CORS - 允許你的 GitHub Pages 來源
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://cchook-sys.github.io"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# 使用 Render 環境變數 (請確認在 Render 設定頁 Key 為 ODDS_API_KEY)
ODDS_API_KEY = os.environ.get("ODDS_API_KEY")
MONGO_URI = os.environ.get("MONGO_URI")

client = AsyncIOMotorClient(MONGO_URI)
db = client.mlb_tracker
history_col = db.odds_history

async def fetch_odds_task():
    while True:
        try:
            if ODDS_API_KEY:
                async with httpx.AsyncClient() as client_http:
                    url = f"https://api.the-odds-api.com/v4/sports/baseball_mlb/odds/?apiKey={ODDS_API_KEY}&regions=us&markets=h2h"
                    response = await client_http.get(url)
                    if response.status_code == 200:
                        data = response.json()
                        if data:
                            # 清空舊資料並寫入新資料，確保前端抓到的是最新賠率
                            await history_col.delete_many({})
                            await history_col.insert_many(data)
                            print(f"成功更新 {len(data)} 筆賠率資料")
        except Exception as e:
            print(f"背景任務錯誤: {e}")
        await asyncio.sleep(600) # 每 10 分鐘更新一次

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(fetch_odds_task())

@app.get("/games")
async def get_games():
    cursor = history_col.find().limit(20)
    data = await cursor.to_list(length=20)
    return {"data": json.loads(json_util.dumps(data))}
