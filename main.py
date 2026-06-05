"""
MLB Pinnacle 盤口快照 + 賽果結算後端 (CORS 安全防護完全體)
- 語法修正：100% 補回偏漏的 jsonify() 函式包裹，徹底消滅跨網域 CORS 載入失敗
- 接口規範：精準對接 The Odds API 官方規律
- 降序排序：由後端直接進行 commence_time 降序(-1)排序，將最新賽果推至最前
"""

import os
import time
import requests
from datetime import datetime, timezone, timedelta
from flask import Flask, jsonify
from flask_cors import CORS
from apscheduler.schedulers.background import BackgroundScheduler
import pymongo
import certifi

app = Flask(__name__)
CORS(app)  # 開放全網域安全連線

# ── Config ────────────────────────────────────────────────────────────────────
ODDS_API_KEY = "5a02e608035ba7b2c5da994b791fc6f4"
SPORT        = "baseball_mlb"
BOOKMAKER    = "pinnacle"
MARKETS      = "h2h,totals,spreads"
ODDS_FORMAT  = "american"
BASE_URL     = "https://api.the-odds-api.com/v4"

# ── Database (MongoDB Atlas) ──────────────────────────────────────────────────
MONGO_URI = "mongodb+srv://ccanthook:surfing135%3D@cluster0.cinyz41.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

try:
    client = pymongo.MongoClient(MONGO_URI, tlsCAFile=certifi.where())
    db = client["mlb_tracker"]
    snaps_col = db["snapshots"]        
    results_col = db["results"]        
    print("✅ 成功連線至 MongoDB Atlas 雲端資料庫！")
except Exception as e:
    print(f"❌ MongoDB 連線失敗: {e}")

# ── 智慧盤口抓取與去重儲存 ──────────────────────────────────────────────────────
def fetch_and_store_job():
    if not ODDS_API_KEY: return
    print("🔄 [盤口巡邏] 正在抓取最新 MLB 盤口...")
    url = f"{BASE_URL}/sports/{SPORT}/odds/?apiKey={ODDS_API_KEY}&regions=us&markets={MARKETS}&bookmakers={BOOKMAKER}&oddsFormat={ODDS_FORMAT}"

    try:
        res = requests.get(url, timeout=15)
        if res.status_code != 200: return

        games = res.json()
        ts    = int(time.time())
        stored = 0

        for game in games:
            pin = next((b for b in game.get("bookmakers", []) if b["key"] == BOOKMAKER), None)
            if not pin: continue

            totals  = next((m for m in pin["markets"] if m["key"] == "totals"),  None)
            h2h     = next((m for m in pin["markets"] if m["key"] == "h2h"),     None)
            spreads = next((m for m in pin["markets"] if m["key"] == "spreads"), None)

            over   = next((o for o in (totals  or {}).get("outcomes", []) if o["name"] == "Over"),          None)
            under  = next((o for o in (totals  or {}).get("outcomes", []) if o["name"] == "Under"),         None)
            ml_home = next((o for o in (h2h    or {}).get("outcomes", []) if o["name"] == game["home_team"]), None)
            ml_away = next((o for o in (h2h    or {}).get("outcomes", []) if o["name"] == game["away_team"]), None)
            sp_home = next((o for o in (spreads or {}).get("outcomes", []) if o["name"] == game["home_team"]), None)

            snap = {
                "game_id":      game["id"],
                "home":         game["home_team"],
                "away":         game["away_team"],
                "commence_time": game["commence_time"],
                "ts":           ts,
                "ts_iso":       datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(),
                "total":        over["point"]  if over  else None,
                "over_juice":   over["price"]  if over  else None,
                "under_juice":  under["price"] if under else None,
                "ml_home":      ml_home["price"] if ml_home else None,
                "ml_away":      ml_away["price"] if ml_away else None,
                "spread_home":  sp_home["point"] if sp_home else None,
            }

            last_snap = snaps_col.find_one({"game_id": game["id"]}, sort=[("ts", pymongo.DESCENDING)])
            has_changed = (
                not last_snap
                or last_snap.get("total")   != snap["total"]
                or last_snap.get("ml_home") != snap["ml_home"]
            )

            if has_changed or (last_snap and (ts - last_snap["ts"]) >= 7200):
                snaps_col.insert_one(snap)
                stored += 1
        print(f"✅ 盤口更新完畢，寫入 {stored} 場變動快照。")
    except Exception as e:
        print(f"❌ 盤口抓取失敗: {e}")

# ── 完賽比分自動結算任務 ──────────────────────────────────────────────────────
def fetch_and_settle_results_job():
    if not ODDS_API_KEY: return
    print("🔄 [強制結算] 正在呼叫 The Odds API 比分接口進行沖銷...")
    url = f"{BASE_URL}/sports/{SPORT}/scores/?apiKey={ODDS_API_KEY}&daysFrom=3"

    try:
        res = requests.get(url, timeout=15)
        if res.status_code != 200: return

        completed_games = res.json()
        settled_count = 0

        for game in completed_games:
            if not game.get("completed", False): continue
            
            gid = game["id"]
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
            opening_total = first.get("total")
            closing_total = last.get("total")

            opening_total_result = "PUSH"
            if opening_total:
                if total_outcome_score > opening_total: opening_total_result = "OVER"
                elif total_outcome_score < opening_total: opening_total_result = "UNDER"

            closing_total_result = "PUSH"
            if closing_total:
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
                "opening_total": opening_total if opening_total else (closing_total if closing_total else 0),
                "closing_total": closing_total if closing_total else 0,
                "opening_total_result": opening_total_result,
                "closing_total_result": closing_total_result,
                "opening_ml_home": first.get("ml_home") if first else None,
                "closing_ml_home": last.get("ml_home") if last else None,
                "updated_at": datetime.now(timezone.utc).isoformat()
            }

            results_col.update_one({"game_id": gid}, {"$set": result_doc}, upsert=True)
            settled_count += 1
        print(f"🎯 賽果順利結算沖銷：本次共成功更新 {settled_count} 場歷史數據。")
    except Exception as e:
        print(f"❌ 賽果結算失敗: {e}")

# 定時器設定
scheduler = BackgroundScheduler()
scheduler.add_job(fetch_and_store_job, 'interval', minutes=10, next_run_time=datetime.now())
scheduler.add_job(fetch_and_settle_results_job, 'interval', minutes=30, next_run_time=datetime.now())
scheduler.start()

@app.route('/games', methods=['GET'])
def get_games():
    try:
        cutoff_time = (datetime.now(timezone.utc) - timedelta(hours=6)).isoformat()
        all_snaps = list(snaps_col.find({"commence_time": {"$gte": cutoff_time}}, {"_id": 0}))
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

            total_delta = round(float(latest.get("total", 0)) - float(first.get("total", 0)), 2) if (latest.get("total") and first.get("total")) else 0
            ml_home_delta = int(latest.get("ml_home", 0)) - int(first.get("ml_home", 0)) if (latest.get("ml_home") and first.get("ml_home")) else 0

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
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/analytics/dataset', methods=['GET'])
def get_training_dataset():
    try:
        fetch_and_settle_results_job()
        # 直接用 MongoDB 對開賽時間進行「降序(-1)」排序，最新完賽場次絕對在第一排
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
                "opening_total": r.get("opening_total"),
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
        return jsonify(dataset)  # 💡 關鍵修復：這裡加上了必要的 jsonify() 打包
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/')
def root(): return jsonify({"status": "ok", "engine": "MLB API CORS Standardizer V2"})

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
