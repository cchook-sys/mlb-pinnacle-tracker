import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import httpx
from datetime import datetime, timezone, timedelta

os.environ['TZ'] = 'Asia/Taipei'
app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

ODDS_API_KEY = "79112bb70773a2cdf998cb3112b18589"

@app.get("/games")
async def get_games():
    url = f"https://api.the-odds-api.com/v4/sports/baseball_mlb/odds/?apiKey={ODDS_API_KEY}&regions=us&markets=h2h,totals&oddsFormat=american"
    async with httpx.AsyncClient(timeout=20) as client:
        res = await client.get(url)
        tw_time = datetime.now(timezone(timedelta(hours=8))).strftime("%H:%M:%S")
        return {"last_update": tw_time, "data": res.json()}

@app.get("/history/{game_id}")
async def get_history(game_id: str):
    # 此處回傳與照片版面相符的數據格式
    return {
        "lines": [
            {"time": "09:00", "phi": -160, "lad": +150},
            {"time": "12:00", "phi": -170, "lad": +140},
            {"time": "16:00", "phi": -185, "lad": +155}
        ],
        "details": {
            "h2h_phi": -185, "h2h_lad": +155, 
            "total_over": -110, "total_under": -110, 
            "point": 8.5
        }
    }
