from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import httpx
from datetime import datetime, timezone, timedelta

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.get("/games")
async def get_games():
    url = "https://api.the-odds-api.com/v4/sports/baseball_mlb/odds/?apiKey=79112bb70773a2cdf998cb3112b18589&regions=us&markets=h2h,totals&oddsFormat=american"
    async with httpx.AsyncClient(timeout=20) as client:
        res = await client.get(url)
        # 校正時間為台灣時間 CST (UTC+8)
        tw_time = datetime.now(timezone(timedelta(hours=8))).strftime("%m/%d %H:%M:%S")
        return {
            "system_updated_at": tw_time,
            "data": res.json()
        }
