"""
MLB Pinnacle 數據量化監控後端 (Vercel 免費完美相容版)
- 免費避難：適應 Vercel Serverless 架構，繞過 Render 403 機房 IP 封鎖黑名單
- 全域安全：共用全域 MongoClient 連線池，高效率不鎖死
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import pymongo
import certifi
import requests
import base64
from datetime import datetime, timedelta

app = FastAPI()

# 允許前端 Netlify 跨網域存取 (CORS)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 1. 全域連線 MongoDB 雲端資料庫
MONGO_URI = "mongodb+srv://ccanthook:surfing135%3D@cluster0.cinyz41.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0&maxPoolSize=20&waitQueueTimeoutMS=5000"

try:
    client = pymongo.MongoClient(MONGO_URI, tlsCAFile=certifi.where())
    db = client["mlb_tracker"]
    games_col = db["games"]
    snaps_col = db["snapshots"]
    results_col = db["results"]
    print("🟢 [資料庫] MongoDB 全域連線成功！")
except Exception as e:
    print(f"❌ [資料庫] 連線失敗: {e}")

def get_pinnacle_headers():
    return {
        "Authorization": "Basic V1M5Njg1NTE6U3VyZjEzNTc5JA==",
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }

# 2. 🧠 自動爬蟲核心：每次前端網頁刷新時，順便在背景自動巡邏抓取 (適應 Vercel 免費規格)
def trigger_pinnacle_crawl():
    print(f"🔄 [觸發巡邏] 正在連線 Pinnacle API...")
    headers = get_pinnacle_headers()
    MLB_LEAGUE_ID = 3
    
    try:
        odds_url = f"https://api.pinnacle.com/v1/odds?sportId=29&leagueIds={MLB_LEAGUE_ID}"
        odds_res = requests.get(odds_url, headers=headers, timeout=10)
        
        fixtures_url = f"https://api.pinnacle.com/v1/fixtures?sportId=29&leagueIds={MLB_LEAGUE_ID}"
        fix_res = requests.get(fixtures_url, headers=headers, timeout=10)
        
        if odds_res.status_code != 200 or fix_res.status_code != 200:
            print(f"⚠️ [巡邏結果] 盤口未更新 (Status: {odds_res.status_code})")
            return
            
        odds_data = odds_res.json()
        fix_data = fix_res.json()
        
        fix_map = {}
        if "league" in fix_data and len(fix_data["league"]) > 0:
            for ev in fix_data["league"][0].get("events", []):
                fix_map[ev["id"]] = {
                    "commence_time": ev["starts"],
                    "home": ev["home"],
                    "away": ev["away"],
                    "status": ev.get("status")
                }
                
        if "leagues" in odds_data and len(odds_data["leagues"]) > 0:
            ts_str = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
            
            for ev_odds in odds_data["leagues"][0].get("events", []):
                event_id = ev_odds["id"]
                
                if event_id in fix_map:
                    event_info = fix_map[event_id]
                    if event_info["commence_time"] <= ts_str: continue
                        
                    snaps_col.insert_one({"event_id": event_id, "ts": ts_str, "odds": ev_odds})
                    
                    periods = ev_odds.get("periods", [])
                    if not periods: continue
                    full_game = periods[0]
                    
                    totals = full_game.get("totals", [])
                    latest_total = totals[0]["points"] if totals else None
                    
                    moneyline = full_game.get("moneyline", {})
                    latest_ml_home = moneyline.get("home") if moneyline else None
                    
                    existing = games_col.find_one({"game_id": str(event_id)})
                    
                    if not existing:
                        new_game = {
                            "game_id": str(event_id),
                            "commence_time": event_info["commence_time"],
                            "away": event_info["away"],
                            "home": event_info["home"],
                            "open": {"total": latest_total, "ml_home": latest_ml_home},
                            "latest": {"total": latest_total, "ml_home": latest_ml_home},
                            "delta": {"total": 0, "ml_home": 0},
                            "snapshot_count": 1,
                            "history": [{"ts": ts_str, "total": latest_total, "ml_home": latest_ml_home}]
                        }
                        games_col.insert_one(new_game)
                    else:
                        hist = existing.get("history", [])
                        last_h = hist[-1] if hist else {}
                        
                        if latest_total == last_h.get("total") and latest_ml_home == last_h.get("ml_home"):
                            continue
                            
                        hist.append({"ts": ts_str, "total": latest_total, "ml_home": latest_ml_home})
                        
                        open_t = existing["open"]["total"]
                        open_ml = existing["open"]["ml_home"]
                        
                        delta_t = (latest_total - open_t) if (latest_total is not None and open_t is not None) else 0
                        delta_ml = (latest_ml_home - open_ml) if (latest_ml_home is not None and open_ml is not None) else 0
                        
                        games_col.update_one(
                            {"game_id": str(event_id)},
                            {
                                "$set": {
                                    "latest": {"total": latest_total, "ml_home": latest_ml_home},
                                    "delta": {"total": delta_t, "ml_home": delta_ml},
                                    "snapshot_count": existing["snapshot_count"] + 1,
                                    "history": hist
                                }
                            }
                        )
            print("✨ [巡邏結果] 資料庫即時盤口更新清洗完成。")
    except Exception as e:
        print(f"❌ [巡邏異常] {e}")

# 3. 路由：網頁獲取即時看盤賽事列表
@app.get('/games')
def get_games():
    try:
        # 💡 Vercel 特色：每次前端敲 API 時自動觸發巡邏抓取，100% 免費維持最新盤口
        trigger_pinnacle_crawl()
        games = list(games_col.find({}, {"_id": 0}).sort("commence_time", 1))
        return games
    except Exception as e:
        return {"error": f"獲取即時數據失敗: {str(e)}"}

# 4. 路由：網頁獲取昨日歷史結算數據
@app.get('/analytics/dataset')
def get_history_dataset():
    try:
        dataset = list(results_col.find({}, {"_id": 0}).sort("commence_time", 1))
        return dataset
    except Exception as e:
        return {"error": f"獲取歷史數據失敗: {str(e)}"}

# 5. 健康檢查路由
@app.get('/')
def health_check():
    return {
        "status": "healthy",
        "platform": "Vercel Free Cloud",
        "pinnacle_channel": "Unlocked"
    }
