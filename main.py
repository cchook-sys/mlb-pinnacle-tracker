from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import httpx

app = FastAPI()

# 解決 CORS 跨域問題，允許 GitHub Pages 存取
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# 你的 API Key
ODDS_API_KEY = "79112bb70773a2cdf998cb3112b18589"

@app.get("/")
def read_root():
    return {"status": "ok", "message": "API is running"}

@app.get("/games")
async def get_games():
    url = f"https://api.the-odds-api.com/v4/sports/baseball_mlb/odds/?apiKey={ODDS_API_KEY}&regions=us&markets=h2h&oddsFormat=american"
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            response = await client.get(url)
            if response.status_code == 200:
                return {"status": "ok", "data": response.json()}
            else:
                return {"status": "error", "message": "API error", "code": response.status_code}
        except Exception as e:
            return {"status": "error", "message": str(e)}
