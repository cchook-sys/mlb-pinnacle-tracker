import os
import asyncio
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime

# 1. 初始化 FastAPI
app = FastAPI()

# 2. 設定 CORS (解決 GitHub Pages 存取限制)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://cchook-sys.github.io"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 3. MongoDB 連線設定
MONGO_URI = os.environ.get("MONGO_URI")
client = AsyncIOMotorClient(MONGO_URI)
db = client.mlb_tracker
history_col = db.odds_history

# 4. 背景任務邏輯
async def fetch_odds_task():
    while True:
        try:
            print(f"[{datetime.now()}] 開始執行背景抓取...")
            # 在這裡放入你的 API 抓取邏輯 (例如 httpx.get)
            # 範例寫入：
            # await history_col.insert_one({"time": datetime.now(), "data": "sample"})
            print("資料寫入成功")
        except Exception as e:
            print(f"背景任務錯誤: {e}")
        
        # 每 10 分鐘 (600秒) 執行一次
        await asyncio.sleep(600)

# 5. 系統啟動時觸發背景任務
@app.on_event("startup")
async def startup_event():
    print(">>> 系統啟動：開始初始化背景任務...")
    asyncio.create_task(fetch_odds_task())

# 6. API 路由
@app.get("/")
def read_root():
    return {"status": "系統運作正常"}

@app.get("/games")
async def get_games():
    # 這裡從資料庫讀取資料回傳
    cursor = history_col.find().sort("time", -1).limit(10)
    data = await cursor.to_list(length=10)
    return {"data": data}

@app.get("/trigger-fetch")
async def trigger_fetch():
    # 手動測試用
    return {"message": "正在執行測試抓取..."}
