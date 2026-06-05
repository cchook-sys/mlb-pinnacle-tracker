"""
MLB Pinnacle 數據量化後端 (週二 FastAPI 經典異步回歸穩定版 V9)
- 100% 還原核心：完全回到週二最穩定的 FastAPI + httpx 異步架構，消滅連線當機 [cite: 1]
- 48h 滾動風控：歷史結算自動比對時間，永遠只留最近 2 天數據，其餘舊場次自動 Delete
- 當日賽事聚焦：/games 路由精準過濾前後 12 小時當日賽事，不撐爆記憶體、不當機
- CORS 徹底開放：全接口加蓋跨網域許可，徹底封印連線阻擋錯誤
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
    db = client["mlb_tracker"] [cite: 2]
    snaps_col = db["snapshots"] [cite: 2]
    results_col = db["results"] [cite: 2]
    print("✅ 成功連線至 MongoDB Atlas 雲端資料庫！")
except Exception as e:
    print(f"❌ MongoDB 連線失敗: {e}")

# ── 智慧盤口抓取與去重儲存 ──────────────────────────────────────────────────────
async def fetch_and_store():
    if not ODDS_API_KEY: return [cite: 3]

    url = f"{BASE_URL}/sports/{SPORT}/odds/?apiKey={ODDS_API_KEY}&regions=us&markets={MARKETS}&bookmakers={BOOKMAKER}&oddsFormat={ODDS_FORMAT}" [cite: 3]

    try:
        async with httpx.AsyncClient(timeout=15) as client: [cite: 16]
            res = await client.get(url) [cite: 16]
            if res.status_code != 200: return

            games = res.json()
            ts    = int(time.time())
            stored = 0

            for game in games:
                pin = next((b for b in game.get("bookmakers", []) if b["key"] == BOOKMAKER), None) [cite: 6]
                if not pin: continue [cite: 6]

                totals  = next((m for m in pin["markets"] if m["key"] == "totals"),  None) [cite: 6]
                h2h     = next((m for m in pin["markets"] if m["key"] == "h2h"),     None) [cite: 6]

                over   = next((o for o in (totals  or {}).get("outcomes", []) if o["name"] == "Over"),          None) [cite: 7]
                ml_home = next((o for o in (h2h    or {}).get("outcomes", []) if o["name"] == game["home_team"]), None) [cite: 8]

                snap = {
                    "game_id":      game["id"], [cite: 9]
                    "home":         game["home_team"], [cite: 9]
                    "away":         game["away_team"], [cite: 9]
                    "commence_time": game["commence_time"], [cite: 10]
                    "ts":           ts,
                    "ts_iso":       datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(),
                    "total":        over["point"]  if over  else None, [cite: 11]
                    "over_juice":   over["price"]  if over  else None, [cite: 11]
                    "ml_home":      ml_home["price"] if ml_home else None, [cite: 11]
                }

                last_snap = snaps_col.find_one({"game_id": game["id"]}, sort=[("ts", pymongo.DESCENDING)]) [cite: 12]
                has_changed = not last_snap or last_snap.get("total") != snap["total"] or last_snap.get("ml_home") != snap["ml_home"]

                if has_changed or (last_snap and (ts - last_snap["ts"]) >= 7200):
                    snaps_col.insert_one(snap) [cite: 15]
                    stored += 1

            print(f"✅ 智慧儲存：本輪共寫入 {stored}/{len(games)} 場關鍵盤口變動快照。")
    except Exception as e:
        print(f"❌ 盤口抓取失敗: {e}")

# ── 完賽賽果結算排程 ＆ 💡 48小時自動滾動刪除 ────────────────────────────────────
async def fetch_and_settle_results():
    if not ODDS_API_KEY: return

    url = f"{BASE_URL}/sports/{SPORT}/scores/?apiKey={ODDS_API_KEY}&daysFrom=3" [cite: 15]

    try:
        async with httpx.AsyncClient(timeout=15) as client: [cite: 16]
            res = await client.get(url) [cite: 16]
            if res.status_code != 200: return

            completed_games = res.json()
            settled_count = 0

            for game in completed_games:
                if not game.get("completed", False): continue [cite: 16]
                gid = game["id"] [cite: 17]
                
                if results_col.find_one({"game_id": gid}): continue

                scores = game.get("scores", []) [cite: 17]
                if not scores or len(scores) < 2: continue [cite: 17]

                home_score = next((int(s["score"]) for s in scores if s["name"] == game["home_team"]), None) [cite: 17]
                away_score = next((int(s["score"]) for s in scores if s["name"] == game["away_team"]), None) [cite: 18]
                if home_score is None or away_score is None: continue
                
                total_outcome_score = home_score + away_score

                snaps = list(snaps_col.find({"game_id": gid}, {"_id": 0}).sort("ts", pymongo.ASCENDING)) [cite: 19]
                if not snaps: continue [cite: 19]

                first_snap = snaps[0] [cite: 19]
                last_snap  = snaps[-1] [cite: 19]

                ml_winner = "HOME" if home_score > away_score else "AWAY"
                opening_total = first_snap.get("total") [cite: 20]
                closing_total = last_snap.get("total") [cite: 21]

                opening_total_result = "PUSH"
                if opening_total:
                    if total_outcome_score > opening_total: opening_total_result = "OVER" [cite: 20]
                    elif total_outcome_score < opening_total: opening_total_result = "UNDER" [cite: 20]

                closing_total_result = "PUSH"
                if closing_total:
                    if total_outcome_score > closing_total: closing_total_result = "OVER" [cite: 21]
                    elif total_outcome_score < closing_total: closing_total_result = "UNDER" [cite: 21]

                result_doc = {
                    "game_id": gid, [cite: 22]
                    "home": game["home_team"], [cite: 22]
                    "away": game["away_team"], [cite: 22]
                    "commence_time": game["commence_time"], [cite: 23]
                    "home_score": home_score, [cite: 23]
                    "away_score": away_score, [cite: 23]
                    "total_score": total_outcome_score, [cite: 23]
                    "ml_winner": ml_winner,                 [cite: 23]
                    "opening_total": opening_total,          [cite: 24]
                    "opening_total_result": opening_total_result, [cite: 24]
                    "closing_total": closing_total,          [cite: 24]
                    "closing_total_result": closing_total_result, [cite: 25]
                    "opening_ml_home": first_snap.get("ml_home"),
                    "closing_ml_home": last_snap.get("ml_home"),
                    "updated_at": datetime.now(timezone.utc).isoformat()
                }

                results_col.insert_one(result_doc)
                settled_count += 1

            # 💡 【精密防爆艙滾動清除】只保留 48 小時內賽果，其餘過期場次自動清空
            time_boundary = (datetime.now(timezone.utc) - timedelta(hours=48)).isoformat()
            del_results = results_col.delete_many({"commence_time": {"$lt": time_boundary}})
            del_snaps = snaps_col.delete_many({"commence_time": {"$lt": time_boundary}})
            print(f"🎯 昨日賽果結算完畢！滾動清空過期賽果: {del_results.deleted_count} 場，舊快照: {del_snaps.deleted_count} 條。")
    except Exception as e:
        print(f"❌ 賽果結算失敗: {e}")

# ── 排程系統設定 ──────────────────────────────────────────────────────────────
scheduler = AsyncIOScheduler()

@asynccontextmanager
async def lifespan(app: FastAPI):
    await fetch_and_store()
    scheduler.add_job(fetch_and_store, "interval", minutes=10, id="pinnacle_fetch")
    scheduler.add_job(fetch_and_settle_results, "interval", minutes=30, id="results_settle")
    scheduler.start()
    print("⏰ 週二異步排程核心滿血回歸")
    yield
    scheduler.shutdown()

# ── FastAPI App 宣告 ──────────────────────────────────────────────────────────
app = FastAPI(title="MLB Pinnacle Tracker (FastAPI Core)", lifespan=lifespan)

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
    return {"status": "ok", "service": "MLB FastAPI Core V9 Return"}

# ── 路由 1：獲取即時看盤 (聚焦當日前後 12 小時賽事，杜絕當機) ─────────────────────
@app.get("/games")
async def get_games():
    await fetch_and_store() 
    
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

        total_delta = round(float(latest.get("total", 0)) - float(first.get("total", 0)), 2) if (latest.get("total") is not None and first.get("total") is not None) else 0 [cite: 28]
        ml_home_delta = int(latest.get("ml_home", 0)) - int(first.get("ml_home", 0)) if (latest.get("ml_home") is not None and first.get("ml_home") is not None) else 0 [cite: 29]

        result.append({
            "game_id":       gid, [cite: 31]
            "home":          latest["home"], [cite: 31]
            "away":          latest["away"], [cite: 31]
            "commence_time": latest["commence_time"], [cite: 31]
            "snapshot_count": len(snaps_sorted), [cite: 31]
            "first_snap_ts": first.get("ts_iso"), [cite: 31]
            "latest": {
                "ts":          latest.get("ts_iso"), [cite: 31]
                "total":       latest.get("total"), [cite: 32]
                "ml_home":     latest.get("ml_home"), [cite: 32]
            },
            "open": {
                "total":   first.get("total"), [cite: 34]
                "ml_home": first.get("ml_home"), [cite: 34]
            },
            "delta": {
                "total":   total_delta, [cite: 34]
                "ml_home": ml_home_delta, [cite: 34]
            },
            "signal": {
                "total": "STEAM_OVER" if total_delta >= 0.5 else ("STEAM_UNDER" if total_delta <= -0.5 else "FLAT"), [cite: 35]
                "ml":    "STEAM_HOME" if ml_home_delta <= -15 else ("STEAM_AWAY" if ml_home_delta >= 15 else "FLAT"), [cite: 35]
            },
            "history": [
                {
                    "ts":       s.get("ts_iso"), [cite: 36]
                    "total":    s.get("total"), [cite: 36]
                    "ml_home":  s.get("ml_home"), [cite: 36]
                }
                for s in snaps_sorted
            ],
        })

    result.sort(key=lambda x: x["commence_time"])
    return result

# ── 路由 2：獲取歷史資料分頁 (後端以降序排序，最新完賽永遠置頂) ─────────────────
@app.get("/analytics/dataset")
async def get_training_dataset():
    await fetch_and_settle_results()
    
    # 按照開賽時間倒序(-1)排，保證最新完賽場次大特寫
    results = list(results_col.find({}, {"_id": 0}).sort("commence_time", -1))
    dataset = []
    
    for r in results:
        dataset.append({
            "game_id": r["game_id"],
            "home": r["home"],
            "away": r["away"],
            "commence_time": r["commence_time"],
            "opening_total": r.get("opening_total"),
            "closing_total": r.get("closing_total"),
            "total_changed_delta": round((r.get("closing_total", 0) - r.get("opening_total", 0)), 2) if (r.get("closing_total") and r.get("opening_total")) else 0,
            "opening_ml_home": r.get("opening_ml_home"),
            "closing_ml_home": r.get("closing_ml_home"),
            "final_home_score": r["home_score"],
            "final_away_score": r["away_score"],
            "final_total_score": r["total_score"],
            "ml_winner_result": r["ml_winner"],
            "opening_total_result": r["opening_total_result"],
            "closing_total_result": r["closing_total_result"]
        })
    return dataset
