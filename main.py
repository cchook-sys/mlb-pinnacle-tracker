"""
MLB Pinnacle 數據量化監控後端完全體 (48小時擴展 + 定時自動爬蟲版)
- 自動核心：每 5 分鐘自動連線 Pinnacle API 抓取最新即時盤口
- 數據大腦：自動清洗並在 MongoDB 建立歷史波動快照 (Snapshots)
- API 輸出：提供前端 48 小時內所有即時看盤卡片與昨日歷史結算數據
"""

from flask import Flask, jsonify
from flask_cors import CORS
import pymongo
import certifi
import requests
import base64
from datetime import datetime, timedelta
import os
import time
from apscheduler.schedulers.background import BackgroundScheduler

app = Flask(__name__)
CORS(app) # 允許前端 Netlify 跨網域存取

# 1. 配置資料庫與 Pinnacle 帳密安全憑證
MONGO_URI = "mongodb+srv://ccanthook:surfing135%3D@cluster0.cinyz41.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
PINNACLE_USER = "WS968551"
PINNACLE_PASS = "Surf13579$"

try:
    client = pymongo.MongoClient(MONGO_URI, tlsCAFile=certifi.where())
    db = client["mlb_tracker"]
    games_col = db["games"]       # 儲存即時盤口與波動歷史
    snaps_col = db["snapshots"]   # 儲存最原始快照備份
    results_col = db["results"]   # 儲存完賽結算對答案數據
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

# 2. 🧠 核心定時爬蟲：每 5 分鐘自動執行一次，洗滌盤口並寫入資料庫
def fetch_pinnacle_job():
    print(f"🔄 [定時爬蟲] 啟動！當前標準時間: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}")
    headers = get_pinnacle_headers()
    MLB_LEAGUE_ID = 3
    
    try:
        # A. 抓取獨贏盤與大小分盤口賠率 (Straight Odds)
        odds_url = f"https://api.pinnacle.com/v1/odds?sportId=29&leagueIds={MLB_LEAGUE_ID}"
        odds_res = requests.get(odds_url, headers=headers, timeout=15)
        
        # B. 抓取賽事基本資訊 (開賽時間、隊伍名稱)
        fixtures_url = f"https://api.pinnacle.com/v1/fixtures?sportId=29&leagueIds={MLB_LEAGUE_ID}"
        fix_res = requests.get(fixtures_url, headers=headers, timeout=15)
        
        if odds_res.status_code != 200 or fix_res.status_code != 200:
            print(f"⚠️ [定時爬蟲] Pinnacle API 拒絕連線 (Odds: {odds_res.status_code}, Fix: {fix_res.status_code})")
            return
            
        odds_data = odds_res.json()
        fix_data = fix_res.json()
        
        # 建立賽事 ID 對應隊伍與時間的字典
        fix_map = {}
        if "league" in fix_data and len(fix_data["league"]) > 0:
            for ev in fix_data["league"][0].get("events", []):
                fix_map[ev["id"]] = {
                    "commence_time": ev["starts"],
                    "home": ev["home"],
                    "away": ev["away"],
                    "status": ev.get("status")
                }
                
        # 開始洗滌盤口並記錄波動
        if "leagues" in odds_data and len(odds_data["leagues"]) > 0:
            ts_str = datetime.utcnow().isoformat() # 本次抓取的時間戳記
            
            for ev_odds in odds_data["leagues"][0].get("events", []):
                event_id = ev_odds["id"]
                if event_id Ram := fix_map.get(event_id):
                    # 排除已經開賽的現場滾球盤，我們專注監控賽前大資金
                    if Ram["commence_time"] <= ts_str:
                        continue
                        
                    # 備份原始快照到雲端
                    snaps_col.insert_one({"event_id": event_id, "ts": ts_str, "odds": ev_odds})
                    
                    # 提取最新盤口核心數值
                    periods = ev_odds.get("periods", [])
                    if not periods: continue
                    full_game = periods[0] # period 0 代表全場完賽盤口
                    
                    # 提取大小分門檻 (Totals)
                    totals = full_game.get("totals", [])
                    latest_total = totals[0]["points"] if totals else null
                    
                    # 提取主隊獨贏賠率 (Moneyline Home)
                    moneyline = full_game.get("moneyline", {})
                    latest_ml_home = moneyline.get("home") if moneyline else null
                    
                    # 檢查資料庫是否已經存在這場比賽紀錄
                    existing = games_col.find_one({"game_id": str(event_id)})
                    
                    if not existing:
                        # 🌟 第一次偵測到該賽事：記錄為初盤 (Opening Lines)
                        new_game = {
                            "game_id": str(event_id),
                            "commence_time": Ram["commence_time"],
                            "away": Ram["away"],
                            "home": Ram["home"],
                            "open": {"total": latest_total, "ml_home": latest_ml_home},
                            "latest": {"total": latest_total, "ml_home": latest_ml_home},
                            "delta": {"total": 0, "ml_home": 0},
                            "snapshot_count": 1,
                            "history": [{"ts": ts_str, "total": latest_total, "ml_home": latest_ml_home}]
                        }
                        games_col.insert_one(new_game)
                    else:
                        # 🌟 歷史已存在該賽事：檢查盤口是否有發生變動變盤
                        hist = existing.get("history", [])
                        last_h = hist[-1] if hist else {}
                        
                        # 如果最新抓到的數值跟上次完全一樣，跳過不重複寫入，保持圖表乾淨
                        if latest_total == last_h.get("total") and latest_ml_home == last_h.get("ml_home"):
                            continue
                            
                        # 若有變盤，將新數據推進歷史波動陣列中
                        hist.append({"ts": ts_str, "total": latest_total, "ml_home": latest_ml_home})
                        
                        # 計算總波動幅度 (最新盤 - 歷史最一開始的初盤)
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
            print("✨ [定時爬蟲] 當前全場次盤口波動增量清洗完成。")
    except Exception as e:
        print(f"❌ [定時爬蟲] 執行發生異常錯誤: {e}")

# 3. 啟動後台自動排程排班系統 (每 5 分鐘自動抓一次)
scheduler = BackgroundScheduler()
scheduler.add_job(fetch_pinnacle_job, 'interval', minutes=5, next_run_time=datetime.now())
scheduler.start()

# 4. 路由：網頁獲取即時看盤賽事列表 (💡 已成功拓寬至 48 小時範圍)
@app.route('/games', methods=['GET'])
def get_games():
    try:
        now = datetime.utcnow()
        # 成功升級：過濾線拉長到 48 小時，保證明天 9 場賽事順利出盤看得到
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
        return jsonify({"error": f"獲取即時看盤數據失敗: {str(e)}"}), 500

# 5. 路由：網頁獲取昨日歷史結算數據
@app.route('/analytics/dataset', methods=['GET'])
def get_history_dataset():
    try:
        dataset = list(results_col.find({}, {"_id": 0}).sort("commence_time", 1))
        return jsonify(dataset)
    except Exception as e:
        return jsonify({"error": f"獲取歷史結算數據失敗: {str(e)}"}), 500

# 6. 健康檢查路由
@app.route('/', methods=['GET'])
def health_check():
    return jsonify({
        "status": "healthy",
        "scheduler": "running",
        "monitoring_window": "48 Hours",
        "pinnacle_target": "MLB League 3"
    })

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
