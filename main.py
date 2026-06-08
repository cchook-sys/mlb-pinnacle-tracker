import os
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import httpx
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any

# 設定時區為台北
os.environ['TZ'] = 'Asia/Taipei'

app = FastAPI()

# 允許所有來源進行 CORS 請求，確保前端能正常讀取 API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

ODDS_API_KEY = "79112bb70773a2cdf998cb3112b18589"

@app.get("/")
async def root():
    return {"status": "online", "message": "MLB API Service is running"}

@app.get("/games")
async def get_games():
    """獲取當前 MLB 比賽列表與盤口"""
    url = f"https://api.the-odds-api.com/v4/sports/baseball_mlb/odds/?apiKey={ODDS_API_KEY}&regions=us&markets=h2h,totals&oddsFormat=american"
    async with httpx.AsyncClient(timeout=20) as client:
        try:
            response = await client.get(url)
            response.raise_for_status()
            data = response.json()
            
            # 回傳最後更新時間與比賽資料
            tw_time = datetime.now(timezone(timedelta(hours=8))).strftime("%H:%M:%S")
            return {"last_update": tw_time, "data": data}
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

@app.get("/history/{game_id}")
async def get_history(game_id: str):
    """
    獲取指定賽事的歷史水位變化數據
    這裡回傳的是嚴格的陣列格式，供前端 .map() 函數使用
    """
    # 此處邏輯為模擬資料，正式環境可在此對接資料庫
    mock_history = [
        {"time": "09:00", "odds": -160},
        {"time": "11:00", "odds": -165},
        {"time": "13:00", "odds": -175},
        {"time": "15:00", "odds": -185}
    ]
    return mock_history

# 啟動命令說明:
# 在 Render 環境中，請確認啟動命令為:
# uvicorn main:app --host 0.0.0.0 --port $PORT
