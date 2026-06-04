"""
MLB Pinnacle 數據量化監控後端完全體 (FastAPI 高性能穩定版)
- 架構兼容：全面改用 FastAPI + Uvicorn 引擎，解決 Render 啟動衝突當機問題
- 自動核心：每 5 分鐘自動連線 Pinnacle API 抓取最新即時盤口與建立快照
- API 輸出：提早 48 小時輸出明天 9 場全部即時看盤卡片與歷史賽果
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

# 初始化 FastAPI 引擎
app = FastAPI()

# 允許前端 Netlify 跨網域存取 (CORS)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 1. 連線 MongoDB 雲端資料庫
MONGO_URI = "mongodb+srv://ccanthook:surfing135%3D@cluster0.cinyz41.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
PINNACLE_USER = "WS968551"
PINNACLE_PASS = "Surf13579$"

try:
    client = pymongo.MongoClient(MONGO_URI, tlsCAFile=certifi.where())
    db = client["mlb_tracker"]
    games_col = db["games"]
    snaps_col = db["snapshots"]
    results_col = db["results"]
    print("🟢 [資料庫] MongoDB 雲端連線成功！")
except Exception as e:
    print(f"❌ [資料庫] 連線失敗: {e}")

def get_pinnacle_headers():
    raw_auth = f"{PINNACLE_USER}:{PINNACLE_PASS}"
    encoded_auth = base64.b64encode(raw_auth.encode()).decode()
    return {
        "Authorization": f"Basic {encoded_auth}",
        "Accept": "application/json"
    }

# 2. 🧠 核心定時爬蟲：每 5 分鐘自動執行一次，洗滌盤口並建立快照歷史
def fetch_pinnacle_job():
    print(f"🔄 [定時爬蟲] 啟動！當前標準時間: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}")
    headers = get_pinnacle_headers()
    MLB_LEAGUE_ID = 3
    
    try:
        odds_url = f"https://api.pinnacle.com/v1/odds?sportId=29&leagueIds={MLB_LEAGUE_ID}"
        odds_res = requests.get(odds_url, headers=headers, timeout=15)
        
        fixtures_url = f"https://api.pinnacle.com/v1/fixtures?sportId=29&leagueIds={MLB_LEAGUE_ID}"
        fix_res = requests.get(fixtures_url, headers=headers, timeout=15)
        
        if odds_res.status_code != 200 or fix_res.status_code != 200:
            print(f"⚠️ [定時爬蟲] Pinnacle API 拒絕連線 (Odds: {odds_res.status_code}, Fix: {fix_res.status_code})")
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
            ts_str = datetime.utcnow().isoformat()
            
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
            print("✨ [定時爬蟲] 當前全場次盤口波動增量清洗完成。")
    except Exception as e:
        print(f"❌ [定時爬蟲] 執行發生異常錯誤: {e}")

# 3. 啟動排程排班系統 (每 5 分鐘跑一次)
scheduler = BackgroundScheduler()
scheduler.add_job(fetch_pinnacle_job, 'interval', minutes=5, next_run_time=datetime.now())
scheduler.start()

# 4. 路由：網頁獲取即時看盤賽事列表 (💡 完美擴展至未來 48 小時內)
@app.get('/games')
def get_games():
    try:
        now = datetime.utcnow()
        cutoff_time = now + timedelta(hours=48)
        
        query = {
            "commence_time": {
                "$gte": now.isoformat(),
                "$lte": cutoff_time.isoformat()
            }
        }
        games = list(games_col.find(query, {"_id": 0}).sort("commence_time", 1))
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
        "framework": "FastAPI (Uvicorn Compatible)",
        "scheduler": "running",
        "monitoring_window": "48 Hours"
    }
