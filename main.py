import os
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import httpx
from datetime import datetime, timezone, timedelta

# 設定台北時區
os.environ['TZ'] = 'Asia/Taipei'

app = FastAPI()

# 啟用 CORS 確保前端能抓取資料
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# API Key
ODDS_API_KEY = "79112bb70773a2cdf998cb3112b18589"

@app.get("/")
def read_root():
    return {"status": "ok"}

@app.get("/games")
async def get_games():
    """獲取 MLB 比賽列表，包含 H2H (獨贏) 與 Totals (大小分)"""
    url = f"https://api.the-odds-api.com/v4/sports/baseball_mlb/odds/?apiKey={ODDS_API_KEY}&regions=us&markets=h2h,totals&oddsFormat=american"
    async with httpx.AsyncClient(timeout=20) as client:
        try:
            response = await client.get(url)
            response.raise_for_status()
            data = response.json()
            
            # 返回結構化資料
            return {
                "last_update": datetime.now(timezone(timedelta(hours=8))).strftime("%H:%M:%S"),
                "data": data
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

@app.get("/history/{game_id}")
async def get_history(game_id: str):
    """
    這裡對接盤口趨勢數據
    """
    # 範例回傳格式：前端 map 使用的陣列
    return [
        {"time": "15:35", "odds": -200},
        {"time": "17:45", "odds": -210},
        {"time": "20:00", "odds": -220},
        {"time": "23:10", "odds": -240}
    ]

# Render 啟動建議:
# uvicorn main:app --host 0.0.0.0 --port 10000
