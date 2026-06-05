"""
MLB Pinnacle 數據量化後端 (禮拜二 FastAPI 穩定回歸 & 48h 滾動清空版 V8)
- 100% FastAPI 還原：回歸你最穩定的 FastAPI + httpx + AsyncIOScheduler 異步核心，徹底告別 CORS 噩夢
- 空間防爆艙：歷史結算自動比對時間，永遠只滾動留存最新的兩天數據，其餘舊場次自動 Delete 清空
- 大掃除指令：開機立刻執行一刀切，無條件刪除 06/03（含）前的老舊過期髒資料
- 聚焦當日：/games 路由聚焦當日賽事（前後 12 小時窗），保證流暢、絕不當機
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
    
    # 💡 【強制清除舊資料】開機時直接把 06/03 號（含）前的舊數據無條件全數 Delete 清空！
    # 這是為了幫您把舊的空白與 3 號髒資料全部洗掉，重新乾淨累積
    clear_boundary = "2026-06-04T00:00:00Z"
    r_del = results_col.delete_many({"commence_time": {"$lt": clear_boundary}})
    s_del = snaps_col.delete_many({"commence_time": {"$lt": clear_boundary}})
    print(f"🧹 大掃除完成：已刪除 3 號前舊賽果: {r_del.deleted_count} 筆，舊快照: {s_del.deleted_count} 筆。")
except Exception as e:
    print(f"❌ MongoDB 連線失敗: {e}")

# ── 智慧盤口抓取與去重儲存 (經典異步造血) ───────────────────────────────────────
async def fetch_and_store():
    if not ODDS_API_KEY: return

    url = f"{BASE_URL}/sports/{SPORT}/odds/?apiKey={ODDS_API_KEY}&regions=us&markets={MARKETS}&bookmakers={BOOKMAKER}&oddsFormat={ODDS_FORMAT}"

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            res = await client.get(url)
            if res.status_code != 200: return

            games = res.json()
            ts    = int(time.time())
            stored = 0

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
                    stored += 1

            print(f"✅ 智慧儲存：本輪共寫入 {stored}/{len(games)} 場關鍵盤口變動快照。")
    except Exception as e:
        print(f"❌ 盤口抓取失敗: {e}")

# ── 完賽賽果自動結算排程 ＆ 💡 48小時自動滾動刪除防爆艙 ────────────────────────────
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
                
                if results_col.find_one({"game_id": gid}): continue

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
                opening_total = first_snap.get("total") if first_snap.get("total") is not None else (last_snap.get("total") if last_snap.get("total") is not None else 0)
                closing_total = last_snap.get("total") if last_snap.get("total") is not None else opening_total

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
                    "opening_total_result": opening_total_result, 
                    "closing_total": closing_total,          
                    "closing_total_result": closing_total_result, 
                    "opening_ml_home": first_snap.get("ml_home"),
                    "closing_ml_home": last_snap.get("ml_home"),
                    "updated_at": datetime.now(timezone.utc).isoformat()
                }

                results_col.insert_one(result_doc)
                settled_count += 1

            # 💡 【精密滾動風控：永不爆艙】只保留 48 小時內賽果，其餘過期場次自動 Delete
            time_boundary = (datetime.now(timezone.utc) - timedelta(hours=48)).isoformat()
            del_results = results_col.delete_many({"commence_time": {"$lt": time_boundary}})
            del_snaps = snaps_col.delete_many({"commence_time": {"$lt": time_boundary}})
            print(f"🎯 昨日賽果結算完畢！新增 {settled_count} 場。已自動移除了 {del_results.deleted_count} 場過期歷史與 {del_snaps.deleted_count} 條舊快照。")
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
    print("⏰ 雙排程安全防護啟動")
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
    return {"status": "ok", "service": "MLB FastAPI Core V8 Return"}

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

        total_delta = round(float(latest.get("total", 0)) - float(first.get("total", 0)), 2) if (latest.get("total") is not None and first.get("total") is not None) else 0
        ml_home_delta = int(latest.get("ml_home", 0)) - int(first.get("ml_home", 0)) if (latest.get("ml_home") is not None and first.get("ml_home") is not None) else 0

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
                "ml_home":     latest.get("ml_home"),
            },
            "open": {
                "total":   first.get("total"),
                "ml_home": first.get("ml_home"),
            },
            "delta": {
                "total":   total_delta,
                "ml_home": ml_home_delta,
            },
            "signal": {
                "total": "STEAM_OVER" if total_delta >= 0.5 else ("STEAM_UNDER" if total_delta <= -0.5 else "FLAT"),
                "ml":    "STEAM_HOME" if ml_home_delta <= -15 else ("STEAM_AWAY" if ml_home_delta >= 15 else "FLAT"),
            },
            "history": [
                {
                    "ts":       s.get("ts_iso"),
                    "total":    s.get("total"),
                    "ml_home":  s.get("ml_home"),
                }
                for s in snaps_sorted
            ],
        })

    result.sort(key=lambda x: x["commence_time"])
    return result

# ── 路由 2：獲取歷史資料分頁 ────────────────────────────────────────────────────
@app.get("/analytics/dataset")
async def get_training_dataset():
    await fetch_and_settle_results()
    
    # 後端進行降序排列 (-1)，讓最新完賽數據永遠置頂
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
