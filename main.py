from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import httpx

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.get("/")
def read_root():
    return {"message": "API is live"}

@app.get("/games")
async def get_games():
    # 這是最暴力的 API 直接轉發，不經過任何資料庫或時間過濾
    url = "https://api.the-odds-api.com/v4/sports/baseball_mlb/odds/?apiKey=5a02e608035ba7b2c5da994b791fc6f4&regions=us&markets=h2h,totals,spreads&oddsFormat=american"
    async with httpx.AsyncClient(timeout=20) as client:
        res = await client.get(url)
        data = res.json()
        return {"data": data}
