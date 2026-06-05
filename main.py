from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import httpx
from datetime import datetime

# 1. 必須先定義 app
app = FastAPI()

# 2. 設定 CORS
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# 3. 再定義路由
@app.get("/games")
async def get_games():
    url = "https://api.the-odds-api.com/v4/sports/baseball_mlb/odds/?apiKey=79112bb70773a2cdf998cb3112b18589&regions=us&markets=h2h,totals&oddsFormat=american"
    async with httpx.AsyncClient(timeout=20) as client:
        res = await client.get(url)
        return {
            "system_updated_at": datetime.now().strftime("%H:%M:%S"),
            "data": res.json()
        }
