"""
MLB Pinnacle 數據量化監控後端 (昨日經典穩定版 - Flask 重組歸位)
- 完美復原：全面回歸最穩定的 Flask + APScheduler 背景實體定時器架構
- 資料庫對接：一律使用原汁原味的欄位名稱，徹底解決 undefined 衝突
- 48小時看盤：完美支持提早抓出明天全場次
"""

from flask import Flask, jsonify
from flask_cors import CORS
import pymongo
import certifi
import requests
import base64
from datetime import datetime, timedelta
import os
from apscheduler.schedulers.background import BackgroundScheduler

app = Flask(__name__)
CORS(app)  # 允許前端 Netlify 跨網域連線

# 1. 連線 MongoDB 雲端資料庫
MONGO_URI = "mongodb+srv://ccanthook:surfing135%3D@cluster0.cinyz41.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
try:
    client = pymongo.MongoClient(MONGO_URI, tlsCAFile=certifi.where())
    db = client["mlb_tracker"]
    games_col = db["games"]       # 即時盤口
    snaps_col = db["snapshots"]   # 原始快照
    results_col = db["results"]   # 歷史結算
    print("🟢 [昨日經典] MongoDB 雲端資料庫連線成功！")
except Exception as e:
    print(f"❌ MongoDB 連線失敗: {e}")

def get_pinnacle_headers():
    # 使用標準編碼，確保密鑰無誤
    raw_auth = "WS968551:Surf13579$"
    encoded_auth = base64.b64encode(raw_auth.encode()).decode()
    return {
        "Authorization": f"Basic {encoded_auth}",
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    }

# 2. 全自動定時爬蟲任務（回到原汁原味經典寫法）
def fetch_pinnacle_job():
    print(f"🔄 [定時爬蟲] 啟動巡邏...")
    headers = get_pinnacle_headers()
    MLB_LEAGUE_ID = 3
    
    try:
        odds_url = f"https://api.pinnacle.com/v1/odds?sportId=29&leagueIds={MLB_LEAGUE_ID}"
        odds_res = requests.get(odds_url, headers=headers, timeout=15)
        
        fixtures_url = f"https://api.pinnacle.com/v1/fixtures?sportId=29&leagueIds={MLB_LEAGUE_ID}"
        fix_res = requests.get(fixtures_url, headers=headers, timeout=15)
        
        if odds_res.status_code != 200 or fix_res.status_code != 200:
            print(f"⚠️ 盤口未更新或接口受限 ({odds_res.status_code})")
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
                        
                        delta_t = (latest_total - open_t) if (latest_total and open_t) else 0
                        delta_ml = (latest_ml_home - open_ml) if (latest_ml_home and open_ml) else 0
                        
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
            print("✨ [定時爬蟲] 即時看盤盤口清洗成功。")
    except Exception as e:
        print(f"❌ 爬蟲任務異常: {e}")

# 啟動背景經典定時器（每 5 分鐘跑一次）
scheduler = BackgroundScheduler()
scheduler.add_job(fetch_pinnacle_job, 'interval', minutes=5, next_run_time=datetime.now())
scheduler.start()

# 3. 路由：獲取即時看盤賽事列表 (拓寬至未來 48 小時，保證看到明天場次)
@app.route('/games', methods=['GET'])
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
        return jsonify(games)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# 4. 路由：獲取昨日歷史結算分析數據集
@app.route('/analytics/dataset', methods=['GET'])
def get_history_dataset():
    try:
        # 完全對應昨日欄位格式直接倒出，洗刷 undefined
        dataset = list(results_col.find({}, {"_id": 0}).sort("commence_time", 1))
        return jsonify(dataset)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/', methods=['GET'])
def health_check():
    return jsonify({"status": "healthy", "version": "Flask Classic Rollback"})

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
