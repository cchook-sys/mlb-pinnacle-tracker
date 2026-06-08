import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import httpx
from datetime import datetime, timezone, timedelta

os.environ['TZ'] = 'Asia/Taipei'
app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

ODDS_API_KEY = "79112bb70773a2cdf998cb3112b18589"

@app.get("/")
def read_root():
    return {"status": "ok"}

@app.get("/games")
async def get_games():
    url = f"https://api.the-odds-api.com/v4/sports/baseball_mlb/odds/?apiKey={ODDS_API_KEY}&regions=us&markets=h2h,totals&oddsFormat=american"
    async with httpx.AsyncClient(timeout=20) as client:
        res = await client.get(url)
        return {"last_update": datetime.now().strftime("%H:%M:%S"), "data": res.json()}
