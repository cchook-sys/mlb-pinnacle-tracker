from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import httpx
from datetime import datetime

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.get("/games")
async def get_games():
    # 這是真正的 MLB API 呼叫，確保數據來源正確
    url = "https://api.the-odds-api.com/v4/sports/baseball_mlb/odds/?apiKey=79112bb70773a2cdf998cb3112b18589&regions=us&markets=h2h,totals&oddsFormat=american"
    async with httpx.AsyncClient(timeout=20) as client:
        res = await client.get(url)
        # 直接把 API 抓到的原始 data 包裝起來給前端
        return {
            "system_updated_at": datetime.now().strftime("%H:%M:%S"),
            "data": res.json()
        }
