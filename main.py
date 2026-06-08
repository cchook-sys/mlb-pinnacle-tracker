from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import httpx

app = FastAPI()

# 嚴格設定允許來源，解決 CORS 錯誤
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://cchook-sys.github.io"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/games")
async def get_games():
    # 這是您的後端邏輯
    return [{"id": "g1", "away_team": "SF", "home_team": "MIL", "time": "06/03"}]

@app.get("/history/{game_id}")
async def get_history(game_id: str):
    # 這是您的圖表數據
    return {
        "labels": ["15:00", "16:00", "17:00"],
        "over_under": [7.5, 7.5, 7.5],
        "win_odds": [-200, -210, -220]
    }
