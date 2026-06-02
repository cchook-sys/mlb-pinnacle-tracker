"""
MLB Pinnacle 盤口快照 + 賽果結算後端 (MongoDB 終極防爆防漏配額版)
- 智慧配額防禦：台灣時間中午 12:30 到傍晚 17:30 完全無比賽時段自動休眠，節省 50% 配額
- 數據去重防禦：盤口未變動時跳過寫入，僅在變盤或每 2 小時保底時寫入，確保數據純淨
- 雙排程機制：10分鐘智慧抓盤口 | 每天中午 12:00 結算昨日賽果
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
    """
    智慧休眠判斷：
    MLB 每天最後一場比賽通常在台灣時間中午前打完，傍晚前完全不會有新比賽。
    設定台灣時間 (UTC+8) 中午 12:30 到 下午 17:30 這 5 個小時為自動休眠期。
    """
    # 取得當前台灣時間 (UTC+8)
    tw_time = datetime.now(timezone(timedelta(hours=8)))
    current_hour = tw_time.hour
    current_minute = tw_time.minute
    
    # 轉換成總分鐘數方便比較 (12:30 = 750 分, 17:30 = 1050 分)
    current_total_minutes = current_hour * 60 + current_minute
    
    if 750 <= current_total_minutes <= 1050:
        return True
    return False

# ── 智慧盤口抓取與去重儲存 ──────────────────────────────────────────────────────
async def fetch_and_store():
    """從 The Odds API 抓 Pinnacle 賠率，並透過智慧過濾寫入 MongoDB"""
    if not ODDS_API_KEY:
        print("❌ 缺少 ODDS_API_KEY")
        return

    # 1. 觸發智慧休眠機制，保護配額
    if is_sleep_time():
        print(f"💤 [{datetime.now().strftime('%H:%M')}] 進入 MLB 日間無賽事休眠期，自動暫停抓取以節省 API 配額。")
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

                # 撈出資料庫裡該場比賽的「最後一筆紀錄」進行智慧對比
                last_snap = snaps_col.find_one({"game_id": game["id"]}, sort=[("ts", pymongo.DESCENDING)])
                
                # 判斷盤口是否真的有跳動 (大小分改變、或者獨贏賠率跳動)
                has_changed = (
                    not last_snap
                    or last_snap.get("total")   != snap["total"]
                    or last_snap.get("ml_home") != snap["ml_home"]
                    or last_snap.get("ml_away") != snap["ml_away"]
                )
                
                # 保底機制：如果超過 2 小時 (7200秒) 盤口都沒變，還是強制存一筆做時間軸對齊
                is_time_to_force_save = last_snap and (ts - last_snap["ts"]) >= 7200

                # 💡 只有在「盤口跳動」或「首筆資料」或「2小時保底」時才寫入資料庫！消滅重複垃圾數據
                if has_changed or is_time_to_force_save:
                    snaps_col.insert_one(snap)
                    stored += 1

            print(f"✅ 智慧儲存：本輪共寫入 {stored}/{len(games)} 場關鍵盤口變動快照。")
    except Exception as e:
        print(f"❌ 盤口抓取失敗: {e}")

# ── 每天中午自動結算昨日賽果 ──────────────────────────────────────────────────
async def fetch_and_settle_results():
    """自動抓取最近 3 天內已完賽的 MLB 比分，並計算過盤標籤存入 MongoDB"""
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
    print("⏰ 雙排程安全防護啟動：10分鐘智慧過濾抓盤口 | 每天中午12:00結算昨日賽果")
    yield
    scheduler.shutdown()

# ── FastAPI App ───────────────────────────────────────────────────────────────
app = FastAPI(title="MLB Pinnacle Tracker (Protected)", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── API Endpoints ─────────────────────────────────────────────────────────────
@app.get("/")
def root():
    return {"status": "ok", "service": "MLB Protected Tracker", "database": "MongoDB Atlas"}

@app.get("/games")
def get_games():
    cutoff_time = datetime.fromtimestamp(time.time() - (4 * 3600), tz=timezone.utc).isoformat()
    all_snaps = list(snaps_col.find({"commence_time": {"$gte": cutoff_time}}, {"_id": 0}))

    games: dict[str, list] = {}
    for s in all_snaps:
        gid = s["game_id"]
        if gid not in games: games[gid] = []
        games[gid].append(s)

    result = []
    for gid, snaps in games.items():
        snaps_sorted = sorted(snaps, key=lambda x: x["ts"])
        latest = snaps_sorted[-1]
        first  = snaps_sorted[0]

        total_delta = None
        ml_home_delta = None
        if latest.get("total") is not None and first.get("total") is not None:
            total_delta = round(latest["total"] - first["total"], 1)
        if latest.get("ml_home") is not None and first.get("ml_home") is not None:
            ml_home_delta = latest["ml_home"] - first["ml_home"]

        total_signal = "FLAT"
        if total_delta is not None:
            if   abs(total_delta) >= 0.5: total_signal = "STEAM_OVER"  if total_delta > 0 else "STEAM_UNDER"
            elif abs(total_delta) >= 0.25: total_signal = "LEAN_OVER"  if total_delta > 0 else "LEAN_UNDER"

        ml_signal = "FLAT"
        if ml_home_delta is not None and abs(ml_home_delta) >= 15:
            ml_signal = "STEAM_HOME" if ml_home_delta < 0 else "STEAM_AWAY"

        result.append({
            "game_id":       gid,
            "home":          latest["home"],
            "away":          latest["away"],
            "commence_time": latest["commence_time"],
            "snapshot_count": len(snaps_sorted),
            "first_snap_ts": first["ts_iso"],
            "latest": {
                "ts":          latest["ts_iso"],
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
                    "ts":       s["ts_iso"],
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
    """導出完整的機器學習特徵與標籤數據集"""
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
