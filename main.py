import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime, timezone, timedelta

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.get("/games")
async def get_games():
    # 模擬賽事列表數據
    return [
        {"id": "g1", "teams": "Toronto Blue Jays VS Atlanta Braves", "time": "06/03 07:15"},
        {"id": "g2", "teams": "San Francisco Giants VS Milwaukee Brewers", "time": "06/03 07:40"},
        {"id": "g3", "teams": "Chicago White Sox VS Minnesota Twins", "time": "06/03 07:40"}
    ]

@app.get("/history/{game_id}")
async def get_history(game_id: str):
    # 對應圖片的雙軸數據：labels 為時間軸，over_under 為左軸數據，win_odds 為右軸數據
    return {
        "labels": ["15:35", "16:14", "17:06", "18:11", "19:29", "21:00", "22:18", "23:10"],
        "over_under": [7.5, 7.5, 7.5, 7.5, 7.5, 7.5, 7.5, 7.5],
        "win_odds": [-205, -200, -210, -205, -200, -205, -202, -240]
    }
