import os
import httpx
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime, timezone, timedelta

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# 這裡使用你的新 Key (請確保已在 GitHub 修改)
ODDS_API_KEY = "79112bb70773a2cdf998cb3112b18589"

@app.get("/games")
async def get_games():
    # 暴力直連：一次抓取所有市場，確保資料最全
    url = f"https://api.the-odds-api.com/v4/sports/baseball_mlb/odds/?apiKey={ODDS_API_KEY}&regions=us&markets=h2h,totals,spreads&oddsFormat=american"
    
    async with httpx.AsyncClient(timeout=20) as client:
        try:
            res = await client.get(url)
            data = res.json()
            
            # 現場整理資料，拋棄所有不必要的資料庫過濾
            processed_data = []
            for game in data:
                # 找 Pinnacle，沒找到就找備援莊家
                bookies = game.get("bookmakers", [])
                pin = next((b for b in bookies if b["key"] == "pinnacle"), None)
                if not pin:
                    pin = next((b for b in bookies if b["key"] in ["betonlineag", "bovada"]), None)
                
                if pin:
                    # 現場解析市場
                    markets = pin.get("markets", [])
                    totals = next((m for m in markets if m["key"] == "totals"), None)
                    h2h = next((m for m in markets if m["key"] == "h2h"), None)
                    over = next((o for o in (totals or {}).get("outcomes", []) if o["name"] == "Over"), None)
                    ml_home = next((o for o in (h2h or {}).get("outcomes", []) if o["name"] == game["home_team"]), None)
                    
                    processed_data.append({
                        "game_id": game["id"],
                        "home": game["home_team"],
                        "away": game["away_team"],
                        "commence_time": game["commence_time"],
                        "latest": {
                            "total": over["point"] if over else None,
                            "ml_home": ml_home["price"] if ml_home else None
                        }
                    })
            
            return {
                "system_updated_at": datetime.now().strftime("%H:%M:%S"),
                "data": processed_data
            }
        except:
            return {"data": []}
