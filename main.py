from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.get("/games")
async def get_games():
    # 模擬左側賽事列表數據
    return [
        {"id": "s1", "name": "San Francisco Giants VS Milwaukee Brewers", "time": "06/03 07:40", "ou": 0, "h2h": -36, "signal": "進場"}
    ]

@app.get("/chart/{game_id}")
async def get_chart(game_id: str):
    # 模擬右側雙軸數據 (左軸: 大小分, 右軸: 獨贏水位)
    return {
        "labels": ["15:35", "16:01", "17:06", "18:24", "20:08", "22:05", "23:10"],
        "ou_data": [7.5, 7.5, 7.5, 7.5, 7.5, 7.5, 7.5],
        "h2h_data": [-205, -200, -210, -205, -200, -205, -240]
    }
