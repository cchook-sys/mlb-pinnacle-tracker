from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://cchook-sys.github.io"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/games")
async def get_games():
    # 確保回傳陣列格式，以便前端 .map() 執行
    return [{"id": "g1", "away_team": "SF", "home_team": "MIL"}]

@app.get("/history/{game_id}")
async def get_history(game_id: str):
    # 確保 key 名稱與前端一致
    return {
        "labels": ["15:00", "16:00", "17:00"],
        "win_odds": [-200, -210, -220]
    }
