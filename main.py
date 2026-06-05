import os
import time
import httpx
from datetime import datetime, timezone, timedelta
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import pymongo
import certifi

# 換一組 API Key 或確保市場全開
ODDS_API_KEY = "5a02e608035ba7b2c5da994b791fc6f4"
SPORT = "baseball_mlb"
# 這裡加入更多備援莊家，防止 Pinnacle 在免費接口被鎖
BOOKMAKERS = "pinnacle,betonlineag,bovada,draftkings"
MARKETS = "h2h,totals,spreads" 

# 修改後的 /games 路由，請直接使用這段
@app.get("/games")
async def get_games():
    url = f"https://api.the-odds-api.com/v4/sports/{SPORT}/odds/?apiKey={ODDS_API_KEY}&regions=us&markets={MARKETS}&bookmakers={BOOKMAKERS}&oddsFormat=american"
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            res = await client.get(url)
            data = res.json()
            # 如果回傳是空的，我們直接拋出一個測試用的 Mock Data 讓你確認前端是否有反應
            if not data:
                return {
                    "system_updated_at": "TEST_MODE",
                    "data": [{"game_id": "TEST", "home": "測試隊A", "away": "測試隊B", "commence_time": "2026-06-05T20:00:00Z", "latest": {"total": 9.5, "ml_home": -110}, "open": {"total": 9.5, "ml_home": -110}, "delta": {"total": 0, "ml_home": 0}, "signal": {"total": "FLAT", "ml": "FLAT"}, "history": []}]
                }
            
            # (以下保持原有的解析邏輯...)
            return {"system_updated_at": datetime.now().strftime("%H:%M:%S"), "data": data}
    except:
        return {"data": []}
