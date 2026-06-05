import os
import time
import httpx
from datetime import datetime, timezone, timedelta
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import pymongo
import certifi

ODDS_API_KEY = "5a02e608035ba7b2c5da994b791fc6f4"
SPORT        = "baseball_mlb"
BOOKMAKERS   = ["pinnacle", "betonlineag", "bovada"]
MARKETS      = "h2h,totals,spreads"
ODDS_FORMAT  = "american"
BASE_URL     = "https://api.the-odds-api.com/v4"

MONGO_URI = "mongodb+srv://ccanthook:surfing135%3D@cluster0.cinyz41.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

try:
    client = pymongo.MongoClient(MONGO_URI, tlsCAFile=certifi.where())
    db = client["mlb_tracker"]
    snaps_col = db["snapshots"]
    results_col = db["results"]
    print("MongoDB Connected Successfully")
except Exception as e:
    print(f"MongoDB Connection Failed: {e}")

async def execute_live_crawl():
    if not ODDS_API_KEY: return
    url = f"{BASE_URL}/sports/{SPORT}/odds/?apiKey={ODDS_API_KEY}&regions=us&markets={MARKETS}&bookmakers={','.join(BOOKMAKERS)}&oddsFormat={ODDS_FORMAT}"
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            res = await client.get(url)
            if res.status_code == 200:
                games = res.json()
                ts = int(time.time())
                for game in games:
                    for bm in BOOKMAKERS:
                        bookmaker_data = next((b for b in game.get("bookmakers", []) if b["key"] == bm), None)
                        if bookmaker_data:
                            # 只要有任何市場資料就存入
                            totals = next((m for m in bookmaker_data["markets"] if m["key"] == "totals"), None)
                            h2h    = next((m for m in bookmaker_data["markets"] if m["key"] == "h2h"), None)
                            over   = next((o for o in (totals or {}).get("outcomes", []) if o["name"] == "Over"), None)
                            ml_home = next((o for o in (h2h or {}).get("outcomes", []) if o["name"] == game["home_team"]), None)
                            
                            snap = {
                                "game_id": game["id"],
                                "home": game["home_team"],
                                "away": game["away_team"],
                                "commence_time": game["commence_time"],
                                "total": over["point"] if over else None,
                                "ml_home": ml_home["price"] if ml_home else None,
                                "ts": ts,
                                "ts_iso": datetime.now(timezone.utc).isoformat()
                            }
                            snaps_col.insert_one(snap)
                            break
    except Exception as e:
        print(f"Fetch failed: {e}")

@asynccontextmanager
async def app_lifespan(app: FastAPI):
    scheduler = AsyncIOScheduler()
    scheduler.add_job(execute_live_crawl, "interval", minutes=10)
    scheduler.start()
    yield
    scheduler.shutdown()

app = FastAPI(lifespan=app_lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.get("/games")
async def get_games():
    # 每次呼叫強制現場爬取
    await execute_live_crawl()
    
    # 撈取最近 24 小時資料，完全不設限
    start_filter = (datetime.now(timezone.utc) - timedelta(hours=6)).isoformat()
    raw_data = list(snaps_col.find({"commence_time": {"$gte": start_filter}}, {"_id": 0}))
    
    # 簡單去重整理
    processed = {}
    for item in raw_data:
        processed[item["game_id"]] = item
        
    tw_now = datetime.now(timezone(timedelta(hours=8)))
    return {
        "system_updated_at": tw_now.strftime("%m/%d %H:%M:%S"),
        "data": list(processed.values())
    }
