from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import httpx

app = FastAPI()

# 啟用 CORS 允許前端請求
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

ODDS_API_KEY = "79112bb70773a2cdf998cb3112b18589"

@app.get("/")
def read_root():
    return {"status": "ok"}

@app.get("/games")
async def get_games():
    url = f"https://api.the-odds-api.com/v4/sports/baseball_mlb/odds/?apiKey={ODDS_API_KEY}&regions=us&markets=h2h&oddsFormat=american"
    async with httpx.AsyncClient() as client:
        response = await client.get(url)
        return response.json()
