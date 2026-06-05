"""
MLB Pinnacle 盤口監控後端 (禮拜二經典 FastAPI 異步穩定完全體 V8)
- 100% 經典還原：回歸你最穩定的 FastAPI + httpx + AsyncIOScheduler 非同步核心，徹底告別 Flask 的 CORS 噩夢
- 數據防護：保留去重防禦與智能寫入，確保隔天對答案資料最完整、絕不漏場
- 欄位對齊：完全採用你原本的資料庫一級欄位輸出，徹底消除前端 undefined 載入失敗
"""

import os
import time
import httpx
from datetime import datetime, timezone, timedelta
from contextlib import asynccontextmanager

from fastapi import FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import pymongo
import certifi

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

# ── 智慧盤口抓取與去重儲存 (經典異步造血) ───────────────────────────────────────
async def fetch_and_store():
    if not ODDS_API_KEY:
        print("❌ 缺少 ODDS_API_KEY")
        return

    url = f"{BASE_URL}/sports/{SPORT}/odds/?apiKey={ODDS_API_KEY}&regions=us&markets={MARKETS}&bookmakers={BOOKMAKER}&oddsFormat={ODDS_FORMAT}"

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            res = await client.get(url)
            remaining = res.headers.get("x-requests-remaining", "?")
            print(f"[{datetime.now().strftime('%H:%M')}] 盤口 API 回應 {res.status_code} | 剩餘配額: {remaining}")

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
                    or last_snap.get("ml_away") != snap["ml_away"]
                )
                
                is_time_to_force_save = last_snap and (ts - last_snap["ts"]) >= 7200

                if has_changed or is_time_to_force_save:
                    snaps_col.insert_one(snap)
                    stored += 1

            print(f"✅ 智慧儲存：本輪共寫入 {stored}/{len(games)} 場關鍵盤口變動快照。")
    except Exception as e:
        print(f"❌ 盤口抓取失敗: {e}")

# ── 每天自動結算昨日賽果 (經典最穩對答案版) ──────────────────────────────────
async def fetch_and_settle_results():
    if not ODDS_API_KEY: return
    print("🔄 開始執行昨日賽果抓取與自動結算排程...")
    url = f"{BASE_URL}/sports/{SPORT}/scores/?apiKey={ODDS_API_KEY}&daysFrom=3"

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            res = await client.get(url)
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
                if not snaps: continue 

                first_snap = snaps[0]
                last_snap  = snaps[-1]

                ml_winner = "HOME" if home_score > away_score else "AWAY"

                opening_total = first_snap.get("total")
                opening_total_result = "PUSH"
                if opening_total:
                    if total_outcome_score > opening_total: opening_total_result = "OVER"
                    elif total_outcome_score < opening_total: opening_total_result = "UNDER"

                closing_total = last_snap.get("total")
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
                    "opening_total": opening_total,          
                    "opening_total_result": opening_total_result, 
                    "closing_total": closing_total,          
                    "closing_total_result": closing_total_result, 
                    "updated_at": datetime.now(timezone.utc).isoformat()
                }

                results_col.update_one({"game_id": gid}, {"$set": result_doc}, upsert=True)
                settled_count += 1

            print(f"🎯 昨日賽果自動結算完畢！共成功更新/結算 {settled_count} 場比賽。")
    except Exception as e:
        print(f"❌ 賽果結算失敗: {e}")

# ── 排程系統設定 ──────────────────────────────────────────────────────────────
scheduler = AsyncIOScheduler()

@asynccontextmanager
async def lifespan(app: FastAPI):
    await fetch_and_store()
    scheduler.add_job(fetch_and_store, "interval", minutes=10, id="pinnacle_fetch")
    scheduler.add_job(fetch_and_settle_results, "interval", minutes=30, id="results_settle") # 提高對答案頻率
    scheduler.start()
    print("⏰ 雙排程安全防護啟動")
    yield
    scheduler.shutdown()

# ── FastAPI App 宣告 ──────────────────────────────────────────────────────────
app = FastAPI(title="MLB Pinnacle Tracker (FastAPI Complete)", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"]
)

@app.get("/")
def root():
    return {"status": "ok", "service": "MLB FastAPI Core V8 Return"}

# ── 路由 1：獲取即時看盤 (聚焦當日前後 12 小時賽事，杜絕當機) ─────────────────────
@app.get("/games")
async def get_games():
    await fetch_and_store() # 訪問時順便觸發造血
    
    # 鎖定前後 12 小時，維持禮拜二最不當機的極致輕量體
    now = datetime.now(timezone.utc)
    start_filter = (now - timedelta(hours=12)).isoformat()
    end_filter = (now + timedelta(hours=12)).isoformat()
    
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

        total_delta = 0
        if latest.get("total") is not None and first.get("total") is not None:
            total_delta = round(float(latest["total"]) - float(first["total"]), 2)

        ml_home_delta = 0
        if latest.get("ml_home") is not None and first.get("ml_home") is not None:
            ml_home_delta = int(latest["ml_home"]) - int(first["ml_home"])

        total_signal = "FLAT"
        if total_delta != 0:
            if   abs(total_delta) >= 0.5: total_signal = "STEAM_OVER" if total_delta > 0 else "STEAM_UNDER"
            elif abs(total_delta) >= 0.25: total_signal = "LEAN_OVER"  if total_delta > 0 else "LEAN_UNDER"

        ml_signal = "FLAT"
        if ml_home_delta != 0 and abs(ml_home_delta) >= 15:
            ml_signal = "STEAM_HOME" if ml_home_delta < 0 else "STEAM_AWAY"

        result.append({
            "game_id":       gid,
            "home":          latest["home"],
            "away":          latest["away"],
            "commence_time": latest["commence_time"],
            "snapshot_count": len(snaps_sorted),
            "first_snap_ts": first.get("ts_iso"),
            "latest": {
                "ts":          latest.get("ts_iso"),
                "total":       latest.get("total"),
                "over_juice":  latest.get("over_juice"),
                "under_juice": latest.get("under_juice"),
                "ml_home":     latest.get("ml_home"),
                "ml_away":     latest.get("ml_away"),
                "spread_home": latest.get("spread_home"),
            },
            "open": {
                "total":   first.get("total"),
                "ml_home": first.get("ml_home"),
                "ml_away": first.get("ml_away"),
            },
            "delta": {
                "total":   total_delta,
                "ml_home": ml_home_delta,
            },
            "signal": {
                "total": total_signal,
                "ml":    ml_signal,
            },
            "history": [
                {
                    "ts":       s.get("ts_iso"),
                    "total":    s.get("total"),
                    "ml_home":  s.get("ml_home"),
                    "ml_away":  s.get("ml_away"),
                }
                for s in snaps_sorted
            ],
        })

    result.sort(key=lambda x: x["commence_time"])
    return result

# ── 路由 2：獲取歷史資料分頁 (依你的新策略：滾動刪除過期，永遠維持精簡 2 天數據) ──
@app.get("/analytics/dataset")
async def get_training_dataset():
    await fetch_and_settle_results()
    
    # 💡 【空間防爆艙刪除器】：主動計算前 48 小時，開機時滾動刪除過期老舊數據，只留精華兩天
    time_boundary = (datetime.now(timezone.utc) - timedelta(hours=48)).isoformat()
    results_col.delete_many({"commence_time": {"$lt": time_boundary}})
    snaps_col.delete_many({"commence_time": {"$lt": time_boundary}})
    
    # 後端進行降序排列 (-1)，讓最新完賽數據永遠置頂
    results = list(results_col.find({}, {"_id": 0}).sort("commence_time", -1))
    dataset = []
    
    for r in results:
        gid = r["game_id"]
        snaps = list(snaps_col.find({"game_id": gid}, {"_id": 0}).sort("ts", pymongo.ASCENDING))
        
        first = snaps[0] if snaps else {}
        last = snaps[-1] if snaps else {}
        
        dataset.append({
            "game_id": gid,
            "home": r["home"],
            "away": r["away"],
            "commence_time": r["commence_time"],
            "opening_total": first.get("total") if first else r.get("opening_total"),
            "closing_total": last.get("total") if last else r.get("closing_total"),
            "total_changed_delta": round((last.get("total", 0) - first.get("total", 0)), 2) if (snaps and last.get("total") and first.get("total")) else 0,
            "opening_ml_home": first.get("ml_home") if first else r.get("opening_ml_home"),
            "closing_ml_home": last.get("ml_home") if last else r.get("closing_ml_home"),
            "snapshot_records_count": len(snaps),
            "final_home_score": r["home_score"],
            "final_away_score": r["away_score"],
            "final_total_score": r["total_score"],
            "ml_winner_result": r["ml_winner"],
            "opening_total_result": r["opening_total_result"],
            "closing_total_result": r["closing_total_result"]
        })
    return dataset
