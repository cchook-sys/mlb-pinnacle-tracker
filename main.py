"""
MLB Pinnacle 數據量化後端 (CORS 暴力解鎖 & 48h 滾動完全體 V7)
- CORS 終極封印：放棄依賴第三方插件，改用 Flask 內建手動硬塞 Header，保證 100% 解除跨網域阻擋
- 歷史全清空：開機立刻執行一刀切，無條件刪除 06/03（含）前的老舊過期髒資料
- 時間軸降溫：即時監控修正為「當日賽事 + 未來 24 小時」，強行抓出 6 號（明天）場次且不撐爆記憶體
- 自動防爆艙：歷史結算維持 48 小時自動刪除，永遠只滾動留存最新的兩天數據
"""

import os
import time
import requests
from datetime import datetime, timezone, timedelta
from flask import Flask, jsonify, make_response
from flask_cors import CORS
from apscheduler.schedulers.background import BackgroundScheduler
import pymongo
import certifi

app = Flask(__name__)
CORS(app) # 基礎宣告

# ── Config ────────────────────────────────────────────────────────────────────
ODDS_API_KEY = "5a02e608035ba7b2c5da994b791fc6f4"
SPORT        = "baseball_mlb"
BOOKMAKER    = "pinnacle"
MARKETS      = "h2h,totals,spreads"
ODDS_FORMAT  = "american"
BASE_URL     = "https://api.the-odds-api.com/v4"

# ── Database ──────────────────────────────────────────────────────────────────
MONGO_URI = "mongodb+srv://ccanthook:surfing135%3D@cluster0.cinyz41.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

try:
    client = pymongo.MongoClient(MONGO_URI, tlsCAFile=certifi.where())
    db = client["mlb_tracker"]
    snaps_col = db["snapshots"]        
    results_col = db["results"]        
    print("✅ MongoDB 雲端連線成功！")
    
    # 💡 【強制清除舊資料指令】開機時直接把 06/03 號（含）前的舊數據無條件全數 Delete 清空！
    clear_boundary = "2026-06-04T00:00:00Z"
    r_del = results_col.delete_many({"commence_time": {"$lt": clear_boundary}})
    s_del = snaps_col.delete_many({"commence_time": {"$lt": clear_boundary}})
    print(f"🧹 大掃除完成：已刪除 3 號前舊賽果: {r_del.deleted_count} 筆，舊快照: {s_del.deleted_count} 筆。")
except Exception as e:
    print(f"❌ MongoDB 連線失敗: {e}")

# ── 盤口現場自造血機制 ────────────────────────────────────────────────────────
def execute_live_crawl():
    if not ODDS_API_KEY: return []
    url = f"{BASE_URL}/sports/{SPORT}/odds/?apiKey={ODDS_API_KEY}&regions=us&markets={MARKETS}&bookmakers={BOOKMAKER}&oddsFormat={ODDS_FORMAT}"
    try:
        res = requests.get(url, timeout=10)
        if res.status_code != 200: return []
        games = res.json()
        ts = int(time.time())
        
        for game in games:
            pin = next((b for b in game.get("bookmakers", []) if b["key"] == BOOKMAKER), None)
            if not pin: continue

            totals  = next((m for m in pin["markets"] if m["key"] == "totals"),  None)
            h2h     = next((m for m in pin["markets"] if m["key"] == "h2h"),     None)

            over   = next((o for o in (totals  or {}).get("outcomes", []) if o["name"] == "Over"),          None)
            ml_home = next((o for o in (h2h    or {}).get("outcomes", []) if o["name"] == game["home_team"]), None)

            snap = {
                "game_id":      game["id"],
                "home":         game["home_team"],
                "away":         game["away_team"],
                "commence_time": game["commence_time"],
                "ts":           ts,
                "ts_iso":       datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(),
                "total":        over["point"]  if over  else None,
                "over_juice":   over["price"]  if over  else None,
                "ml_home":      ml_home["price"] if ml_home else None,
            }

            last_snap = snaps_col.find_one({"game_id": game["id"]}, sort=[("ts", pymongo.DESCENDING)])
            has_changed = not last_snap or last_snap.get("total") != snap["total"] or last_snap.get("ml_home") != snap["ml_home"]
            
            if has_changed or (last_snap and (ts - last_snap["ts"]) >= 7200):
                snaps_col.insert_one(snap)
        return games
    except Exception as e:
        print(f"❌ 現場造血異常: {e}")
        return []

# ── 完賽賽果沖銷 ＆ 48小時自動滾動刪除 ────────────────────────────────────────
def fetch_and_settle_results_job():
    if not ODDS_API_KEY: return
    print("🔄 [滾動歷史沖銷] 正在拉取完賽賽果...")
    url = f"{BASE_URL}/sports/{SPORT}/scores/?apiKey={ODDS_API_KEY}&daysFrom=3"

    try:
        res = requests.get(url, timeout=10)
        if res.status_code != 200: return
        completed_games = res.json()
        settled_count = 0

        for game in completed_games:
            if not game.get("completed", False): continue
            gid = game["id"]
            
            if results_col.find_one({"game_id": gid}): continue

            scores = game.get("scores", [])
            if not scores or len(scores) < 2: continue

            home_score = next((int(s["score"]) for s in scores if s["name"] == game["home_team"]), None)
            away_score = next((int(s["score"]) for s in scores if s["name"] == game["away_team"]), None)
            if home_score is None or away_score is None: continue
            total_outcome_score = home_score + away_score

            snaps = list(snaps_col.find({"game_id": gid}, {"_id": 0}).sort("ts", pymongo.ASCENDING))
            first = snaps[0] if snaps else {}
            last = snaps[-1] if snaps else {}

            ml_winner = "HOME" if home_score > away_score else "AWAY"
            opening_total = first.get("total") if first.get("total") is not None else (last.get("total") if last.get("total") is not None else 0)
            closing_total = last.get("total") if last.get("total") is not None else opening_total

            opening_total_result = "PUSH"
            if opening_total > 0:
                if total_outcome_score > opening_total: opening_total_result = "OVER"
                elif total_outcome_score < opening_total: opening_total_result = "UNDER"

            closing_total_result = "PUSH"
            if closing_total > 0:
                if total_outcome_score > closing_total: closing_total_result = "OVER"
                elif total_outcome_score < closing_total: closing_total_result = "UNDER"

            result_doc = {
                "game_id": gid,
                "home": game["home_team"],
                "away": game["away_team"],
                "commence_time": game["commence_time"],
                "home_score": home_score,
                "away_score": away_score,
                "total_score": total_outcome_score,
                "ml_winner": ml_winner,
                "opening_total": opening_total,
                "closing_total": closing_total,
                "opening_total_result": opening_total_result,
                "closing_total_result": closing_total_result,
                "opening_ml_home": first.get("ml_home") if first else None,
                "closing_ml_home": last.get("ml_home") if last else None,
                "updated_at": datetime.now(timezone.utc).isoformat()
            }

            results_col.insert_one(result_doc)
            settled_count += 1
        
        # 💡 【精密滾動風控：永不爆艙】只保留 48 小時內賽果，其餘過期場次自動 Delete
        time_boundary = (datetime.now(timezone.utc) - timedelta(hours=48)).isoformat()
        del_results = results_col.delete_many({"commence_time": {"$lt": time_boundary}})
        del_snaps = snaps_col.delete_many({"commence_time": {"$lt": time_boundary}})
        print(f"🎯 滾動清理：已移除了 {del_results.deleted_count} 場過期歷史與 {del_snaps.deleted_count} 條舊快照。")
    except Exception as e:
        print(f"❌ 賽果結算失敗: {e}")

# 定時排程設定
scheduler = BackgroundScheduler()
scheduler.add_job(execute_live_crawl, 'interval', minutes=10, next_run_time=datetime.now())
scheduler.add_job(fetch_and_settle_results_job, 'interval', minutes=30, next_run_time=datetime.now())
scheduler.start()

# ── 路由 1：獲取即時看盤 (💡 當日賽事 + 未來 24 小時，完美逼出 6 號明天場次) ──
@app.route('/games', methods=['GET'])
def get_games():
    try:
        execute_live_crawl()  # 被訪問時強制向 The Odds API 要最新放盤
        
        # 💡 最佳平衡時間窗：從當前 6 小時前開始，一路往後抓 24 小時，強行將明天的比賽拉入列表中！
        now = datetime.now(timezone.utc)
        start_filter = (now - timedelta(hours=6)).isoformat()
        end_filter = (now + timedelta(hours=24)).isoformat()
        
        all_snaps = list(snaps_col.find({
            "commence_time": {"$gte": start_filter, "$lte": end_filter}
        }, {"_id": 0}))
        
        games = {}
        for s in all_snaps:
            gid = s["game_id"]
            if gid not in games: games[gid] = []
            games[gid].append(s)

        result = []
        for gid, snaps in games.items():
            snaps_sorted = sorted(snaps, key=lambda x: x["ts"])
            latest = snaps_sorted[-1]
            first  = snaps_sorted[0]

            total_delta = round(float(latest.get("total", 0)) - float(first.get("total", 0)), 2) if (latest.get("total") is not None and first.get("total") is not None) else 0
            ml_home_delta = int(latest.get("ml_home", 0)) - int(first.get("ml_home", 0)) if (latest.get("ml_home") is not None and first.get("ml_home") is not None) else 0

            result.append({
                "game_id":       gid,
                "home":          latest["home"],
                "away":          latest["away"],
                "commence_time": latest["commence_time"],
                "snapshot_count": len(snaps_sorted),
                "latest":        latest,
                "open":          {"total": first.get("total"), "ml_home": first.get("ml_home")},
                "delta":         {"total": total_delta, "ml_home": ml_home_delta},
                "signal":        {"total": "STEAM_OVER" if total_delta >= 0.5 else ("STEAM_UNDER" if total_delta <= -0.5 else "FLAT")},
                "history":       [{"ts": s["ts_iso"], "total": s.get("total"), "ml_home": s.get("ml_home")} for s in snaps_sorted]
            })
        result.sort(key=lambda x: x["commence_time"])
        
        # 💡 【暴力破解 CORS】建立實體 Response，手工寫死全網域放行，徹底消滅連線被阻擋的錯誤
        resp = make_response(jsonify(result))
        resp.headers["Access-Control-Allow-Origin"] = "*"
        resp.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
        resp.headers["Access-Control-Allow-Headers"] = "*"
        return resp
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ── 路由 2：獲取歷史資料分頁 ────────────────────────────────────────────────────
@app.route('/analytics/dataset', methods=['GET'])
def get_training_dataset():
    try:
        fetch_and_settle_results_job() # 點擊分頁時強制沖銷昨日賽果
        results = list(results_col.find({}, {"_id": 0}).sort("commence_time", -1))
        
        dataset = []
        for r in results:
            open_t = r.get("opening_total", 0) or 0
            close_t = r.get("closing_total", 0) or 0
            delta_t = round(close_t - open_t, 2)

            dataset.append({
                "game_id": r["game_id"],
                "home": r["home"],
                "away": r["away"],
                "commence_time": r["commence_time"],
                "opening_total": r.get("opening_total") if r.get("opening_total") else r.get("closing_total"),
                "closing_total": r.get("closing_total"),
                "total_changed_delta": delta_t,
                "opening_ml_home": r.get("opening_ml_home"),
                "closing_ml_home": r.get("closing_ml_home"),
                "final_home_score": r["home_score"],
                "final_away_score": r["away_score"],
                "final_total_score": r["total_score"],
                "ml_winner_result": r["ml_winner"],
                "opening_total_result": r.get("opening_total_result", "PUSH"),
                "closing_total_result": r.get("closing_total_result", "PUSH")
            })
            
        # 💡 【暴力破解 CORS】手工寫死歷史數據接口的許可證
        resp = make_response(jsonify(dataset))
        resp.headers["Access-Control-Allow-Origin"] = "*"
        resp.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
        resp.headers["Access-Control-Allow-Headers"] = "*"
        return resp
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/')
def root(): 
    resp = make_response(jsonify({"status": "ok", "engine": "MLB Manual CORS Core V7"}))
    resp.headers["Access-Control-Allow-Origin"] = "*"
    return resp

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
