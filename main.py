import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import httpx
from datetime import datetime

# 設定時區
os.environ['TZ'] = 'Asia/Taipei'

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# 你的 API Key
ODDS_API_KEY = "79112bb70773a2cdf998cb3112b18589"

@app.get("/games")
async def get_games():
    url = f"https://api.the-odds-api.com/v4/sports/baseball_mlb/odds/?apiKey={ODDS_API_KEY}&regions=us&markets=h2h,totals&oddsFormat=american"
    async with httpx.AsyncClient(timeout=20) as client:
        try:
            res = await client.get(url)
            tw_now = datetime.now().strftime("%H:%M:%S")
            return {
                "system_updated_at": tw_now,
                "data": res.json()
            }
        except Exception:
            return {"system_updated_at": "錯誤", "data": []}
