from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://cchook-sys.github.io"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# 新增這一塊來處理根目錄，解決 Not Found 問題
@app.get("/")
async def root():
    return {"message": "MLB Pinnacle Tracker API is running"}

@app.get("/games")
async def get_games():
    return [{"id": "g1", "away_team": "SF", "home_team": "MIL"}]

@app.get("/history/{game_id}")
async def get_history(game_id: str):
    return {
        "labels": ["15:00", "16:00", "17:00"],
        "win_odds": [-200, -210, -220]
    }
