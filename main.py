import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import httpx
from datetime import datetime

# 強制設定時區為台北
os.environ['TZ'] = 'Asia/Taipei'

app = FastAPI()

# 解決 CORS 問題，確保前端能正常讀取數據
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# 你的 API Key
ODDS_API_KEY = "79112bb70773a2cdf998cb3112b18589"

# 1. 定義根目錄路由，解決 404 Not Found 問題
@app.get("/")
async def root():
    return {"status": "API is online", "message": "MLB Tracker Backend"}

# 2. 定義數據接口路由
@app.get("/games")
async def get_games():
    url = f"https://api.the-odds-api.com/v4/sports/baseball_mlb/odds/?apiKey={ODDS_API_KEY}&regions=us&markets=h2h,totals&oddsFormat=american"
    async with httpx.AsyncClient(timeout=20) as client:
        try:
            res = await client.get(url)
            # 使用台北時間格式化
            tw_now = datetime.now().strftime("%H:%M:%S")
            return {
                "system_updated_at": tw_now,
                "data": res.json()
            }
        except Exception as e:
            return {"system_updated_at": "錯誤", "data": []}
