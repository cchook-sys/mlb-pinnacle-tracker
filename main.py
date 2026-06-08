from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import httpx

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# 你的 API Key (請確保這是從 The Odds API 申請的有效 Key)
API_KEY = "79112bb70773a2cdf998cb3112b18589"

@app.get("/")
def read_root():
    return {"status": "ok", "message": "API is running"}

@app.get("/games")
async def get_games():
    # 呼叫 The Odds API 獲取 MLB 賽事賠率
    url = f"https://api.the-odds-api.com/v4/sports/baseball_mlb/odds/?apiKey={API_KEY}&regions=us&markets=h2h&oddsFormat=american"
    
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(url)
            return {"status": "ok", "data": response.json()}
        except Exception as e:
            return {"status": "error", "message": str(e)}
