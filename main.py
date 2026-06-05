import os
import httpx
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime, timezone, timedelta

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.get("/games")
async def get_games():
    # 這裡直接呼叫 API，不經任何資料庫過濾，完全原汁原味
    url = "https://api.the-odds-api.com/v4/sports/baseball_mlb/odds/?apiKey=5a02e608035ba7b2c5da994b791fc6f4&regions=us&markets=h2h,totals,spreads&oddsFormat=american"
    
    async with httpx.AsyncClient(timeout=20) as client:
        res = await client.get(url)
        data = res.json()
        
        # 顯示 API 到底給了我們什麼，如果不給數據，我們至少能看到原因
        tw_now = datetime.now(timezone(timedelta(hours=8)))
        return {
            "debug_msg": "API_RAW_DUMP",
            "system_updated_at": tw_now.strftime("%m/%d %H:%M:%S"),
            "data": data 
        }
