"""
MLB Pinnacle 盤口快照 + 賽果結算後端 (MongoDB 終極安全防護免崩潰版)
- 智慧配額防禦：台灣時間中午 12:30 到傍晚 17:30 自動休眠
- 數據去重防禦：盤口未變動時跳過寫入，消滅垃圾數據
- 語法修正防禦：徹底修正 404 與 Exited with status 1 啟動崩潰問題
"""

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
from dotenv import load_dotenv

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────
ODDS_API_KEY = os.getenv("ODDS_API_KEY", "")
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

# ── 智慧配額檢查機制 ──────────────────────────────────────────────────────────
def is_sleep_time():
    tw_time = datetime.now(timezone(timedelta(hours=8)))
    current_hour = tw_time.hour
    current_minute = tw_time.minute
    current_total_minutes = current_hour * 60 + current_minute
    
    if 750 <= current_total_minutes <= 1050:
        return True
    return False

# ── 智慧盤口抓取與去重儲存 ──────────────────────────────────────────────────────
async def fetch_and_store():
    if not ODDS_API_KEY:
        print("❌ 缺少 ODDS_API_KEY")
        return

    if is_sleep_time():
        print(f"💤 [{datetime.now().strftime('%H:%M')}] 進入 MLB 日間無賽事休眠期，自動暫停抓取。")
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

# ── 每天中午自動結算昨日賽果 ──────────────────────────────────────────────────
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
    scheduler.add_job(fetch_and_settle_results, "cron", hour=12, minute=0, id="results_settle")
    scheduler.start()
    print("⏰ 雙排程安全防護啟動")
    yield
    scheduler.shutdown()

# ── FastAPI App 與 CORS 設定 ──────────────────────────────────────────────────
app = FastAPI(title="MLB Pinnacle Tracker (Protected)", lifespan=lifespan)

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
    return {"status": "ok", "service": "MLB Protected Tracker", "database": "MongoDB Atlas"}

@app.get("/games")
def get_games():
    cutoff_time = datetime.fromtimestamp(time.time() - (4 * 3600), tz=timezone.utc).isoformat()
    all_snaps = list(snaps_col.find({"commence_time": {"$gte": cutoff_time}}, {"_id": 0}))

    games = {}
    for s in all_snaps:
        gid = s["game_id"]
        if gid not in games: 
            games[gid] = [] # 💡 這裡已經修正為正確的 Python 換行縮排
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
            if   abs(total_delta) >= 0.5: total_signal = "STEAM_OVER"  if total_delta > 0 else "STEAM_UNDER"
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

@app.get("/analytics/dataset")
def get_training_dataset():
    results = list(results_col.find({}, {"_id": 0}))
    dataset = []
    
    for r in results:
        gid = r["game_id"]
        snaps = list(snaps_col.find({"game_id": gid}, {"_id": 0}).sort("ts", pymongo.ASCENDING))
        if not snaps: continue
        
        first = snaps[0]
        last = snaps[-1]
        
        dataset.append({
            "game_id": gid,
            "home": r["home"],
            "away": r["away"],
            "commence_time": r["commence_time"],
            "opening_total": first.get("total"),
            "closing_total": last.get("total"),
            "total_changed_delta": round((last.get("total", 0) - first.get("total", 0)), 2) if (last.get("total") and first.get("total")) else 0,
            "opening_ml_home": first.get("ml_home"),
            "closing_ml_home": last.get("ml_home"),
            "snapshot_records_count": len(snaps),
            "final_home_score": r["home_score"],
            "final_away_score": r["away_score"],
            "final_total_score": r["total_score"],
            "ml_winner_result": r["ml_winner"],
            "opening_total_result": r["opening_total_result"],
            "closing_total_result": r["closing_total_result"]
        })
    return dataset
