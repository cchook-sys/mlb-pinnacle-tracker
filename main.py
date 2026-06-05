from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import httpx
from datetime import datetime

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.get("/games")
async def get_games():
    url = "https://api.the-odds-api.com/v4/sports/baseball_mlb/odds/?apiKey=79112bb70773a2cdf998cb3112b18589&regions=us&markets=h2h,totals&oddsFormat=american"
    async with httpx.AsyncClient(timeout=20) as client:
        res = await client.get(url)
        data = res.json()
        
        # 進行 Delta 數據處理 (雖然現在是現場直出，我們模擬一個 Delta 顯示)
        processed = []
        for g in data:
            pin = next((b for b in g.get("bookmakers", []) if b["key"] == "pinnacle"), None) or g.get("bookmakers", [])[0]
            mkt = pin.get("markets", [])
            h2h = next((m for m in mkt if m["key"] == "h2h"), None)
            total = next((m for m in mkt if m["key"] == "totals"), None)
            
            ml = h2h["outcomes"][0]["price"] if h2h else 0
            tot = total["outcomes"][0]["point"] if total else 0
            
            processed.append({
                "away": g["away_team"], "home": g["home_team"],
                "commence_time": g["commence_time"],
                "latest": {"ml_home": ml, "total": tot},
                "delta": {"ml_home": 0, "total": 0}, # 這裡可依需求擴充歷史比對
                "signal": {"ml": "FLAT", "total": "FLAT"}
            })
            
        return {"system_updated_at": datetime.now().strftime("%H:%M:%S"), "data": processed}
