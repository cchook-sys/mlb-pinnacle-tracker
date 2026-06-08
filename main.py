import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import httpx
from datetime import datetime, timezone, timedelta
import asyncio

os.environ['TZ'] = 'Asia/Taipei'
app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

ODDS_API_KEY = "79112bb70773a2cdf998cb3112b18589"

# 儲存歷史紀錄的資料庫 (遊戲 ID -> 歷史點位列表)
HISTORY_DB = {}

async def update_odds_task():
    """背景任務：每 10 分鐘抓取一次最新賠率並存入 DB"""
    while True:
        try:
            url = f"https://api.the-odds-api.com/v4/sports/baseball_mlb/odds/?apiKey={ODDS_API_KEY}&regions=us&markets=h2h&oddsFormat=american"
            async with httpx.AsyncClient() as client:
                res = await client.get(url)
                data = res.json()
                now = datetime.now().strftime("%H:%M")
                
                for game in data:
                    gid = game['id']
                    if gid not in HISTORY_DB: HISTORY_DB[gid] = []
                    
                    # 抓取 Pinnacle 賠率 (客/主)
                    pin = next((b for b in game['bookmakers'] if b['key'] == 'pinnacle'), None)
                    if pin:
                        h2h = pin['markets'][0]['outcomes']
                        HISTORY_DB[gid].append({
                            "time": now,
                            "away": h2h[0]['price'],
                            "home": h2h[1]['price']
                        })
                        # 只保留最近 24 小時的數據 (約 144 個點)
                        if len(HISTORY_DB[gid]) > 144: HISTORY_DB[gid].pop(0)
        except Exception as e:
            print(f"Update error: {e}")
        await asyncio.sleep(600)  # 每 600 秒 (10 分鐘) 執行一次

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(update_odds_task())

@app.get("/games")
async def get_games():
    # 這裡直接回傳 API 列表，讓前端獲取比賽資訊
    return {"status": "ok", "data": []} # 建議結合上面邏輯填入最新數據

@app.get("/history/{game_id}")
async def get_history(game_id: str):
    # 前端請求圖表時，直接回傳 HISTORY_DB 中的歷史紀錄
    return HISTORY_DB.get(game_id, [])
