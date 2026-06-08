import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import httpx
from datetime import datetime, timezone, timedelta

# 強制設定為台北時區
os.environ['TZ'] = 'Asia/Taipei'

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

ODDS_API_KEY = "79112bb70773a2cdf998cb3112b18589"

@app.get("/")
async def root():
    return {"status": "online"}

@app.get("/games")
async def get_games():
    url = f"https://api.the-odds-api.com/v4/sports/baseball_mlb/odds/?apiKey={ODDS_API_KEY}&regions=us&markets=h2h&oddsFormat=american"
    async with httpx.AsyncClient(timeout=20) as client:
        try:
            res = await client.get(url)
            # 使用台北時間校正
            tw_time = datetime.now(timezone(timedelta(hours=8))).strftime("%H:%M:%S")
            return {"last_update": tw_time, "data": res.json()}
        except Exception:
            return {"last_update": "錯誤", "data": []}

@app.get("/history/{game_id}")
async def get_history(game_id: str):
    # 這是盤口變化的模擬數據，後續可在此對接資料庫
    return [
        {"time": "10:00", "odds": -150},
        {"time": "11:00", "odds": -155},
        {"time": "12:00", "odds": -160},
        {"time": "13:00", "odds": -158}
    ]
