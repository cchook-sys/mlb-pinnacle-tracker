"""
MLB Pinnacle 數據量化監控後端完全體 (終極打包無誤版)
- 測試解封：暫時移除 /games 的未來 48 小時限制，直接吐出資料庫內所有場次，驗證資料對接
- 全域安全：所有路由共用全域 MongoClient 連線池，徹底根除 Internal Server Error (500)
- 自動排程：啟動 5 秒後自動執行第一次 Pinnacle 爬蟲，隨後每 5 分鐘自動更新
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import pymongo
import certifi
import requests
import base64
from datetime import datetime, timedelta
import os
from apscheduler.schedulers.background import BackgroundScheduler

app = FastAPI()

# 允許前端 Netlify 跨網域存取 (CORS)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 1. 全域連線 MongoDB 雲端資料庫 (共用連線池，高效率不鎖死)
MONGO_URI = "mongodb+srv://ccanthook:surfing135%3D@cluster0.cinyz41.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0&maxPoolSize=20&waitQueueTimeoutMS=5000"
PINNACLE_USER = "WS968551"
PINNACLE_PASS = "Surf13579$"

try:
    client = pymongo.MongoClient(MONGO_URI, tlsCAFile=certifi.where())
    db = client["mlb_tracker"]
    games_col = db["games"]
    snaps_col = db["snapshots"]
    results_col = db["results"]
    print("🟢 [資料庫] MongoDB 全域連線成功，通道已就緒！")
except Exception as e:
    print(f"❌ [資料庫] 連線失敗: {e}")

def get_pinnacle_headers():
    raw_auth = f"{PINNACLE_USER}:{PINNACLE_PASS}"
    encoded_auth = base64.b64encode(raw_auth.encode()).decode()
    return {
        "Authorization": f"Basic {encoded_auth}",
        "Accept": "application/json"
    }

# 2. 🧠 定時爬蟲核心：每 5 分鐘自動執行一次
def fetch_pinnacle_job():
    print(f"🔄 [定時爬蟲] 啟動抓取: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}")
    headers = get_pinnacle_headers()
    MLB_LEAGUE_ID = 3
    
    try:
        odds_url = f"https://api.pinnacle.com/v1/odds?sportId=29&leagueIds={MLB_LEAGUE_ID}"
        odds_res = requests.get(odds_url, headers=headers, timeout=15)
        
        fixtures_url = f"https://api.pinnacle.com/v1/fixtures?sportId=29&leagueIds={MLB_LEAGUE_ID}"
        fix_res = requests.get(fixtures_url, headers=headers, timeout=15)
        
        if odds_res.status_code != 200 or fix_res.status_code != 200:
            print(f"⚠️ [定時爬蟲] 盤口未更新或接口受限 (Odds: {odds_res.status_code}, Fix: {fix_res.status_code})")
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
                    
                    if event_info["commence_time"] <= ts_str:
                        continue
                        
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
            print("✨ [定時爬蟲] 全場次盤口波動增量清洗完成。")
    except Exception as e:
        print(f"❌ [定時爬蟲] 執行發生異常: {e}")

# 3. 啟動背景排程 (延遲 5 秒安全啟動)
scheduler = BackgroundScheduler()
scheduler.add_job(
    fetch_pinnacle_job, 
    'interval', 
    minutes=5, 
    start_date=datetime.now() + timedelta(seconds=5),
    misfire_grace_time=120
)
scheduler.start()

# 4. 路由：網頁獲取即時看盤賽事列表 (💡 解封測試版：直接撈取資料庫內所有現有賽事)
@app.get('/games')
def get_games():
    try:
        # 直接倒出所有資料庫內有的賽事，確保不被時區字串格式卡死空陣列
        games = list(games_col.find({}, {"_id": 0}).sort("commence_time", 1))
        return games
    except Exception as e:
        return {"error": f"獲取即時數據失敗: {str(e)}"}

# 5. 路由：網頁獲取昨日歷史結算數據
@app.get('/analytics/dataset')
def get_history_dataset():
    try:
        dataset = list(results_col.find({}, {"_id": 0}).sort("commence_time", 1))
        return dataset
    except Exception as e:
        return {"error": f"獲取歷史數據失敗: {str(e)}"}

# 6. 健康檢查路由
@app.get('/')
def health_check():
    return {
        "status": "healthy",
        "framework": "FastAPI (Fully Packaged)",
        "monitoring_window": "All Available Database"
    }
