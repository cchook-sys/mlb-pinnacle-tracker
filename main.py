"""
MLB Pinnacle 盤口快照 + 賽果結算後端 (MongoDB 閉環版)
- 每 10 分鐘自動抓一次 Pinnacle MLB 賠率，永久儲存至 MongoDB
- 每天中午 12:00 自動抓取昨日 MLB 最終比分，自動結算大小分與獨贏盤
- 提供 REST API 給前端儀表板讀取
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
    snaps_col = db["snapshots"]        # 存放盤口快照
    results_col = db["results"]        # 💡 新增：存放最終比賽結果與標籤
    print("✅ 成功連線至 MongoDB Atlas 雲端資料庫！")
except Exception as e:
    print(f"❌ MongoDB 連線失敗: {e}")

# ── 步驟 1：每 10 分鐘抓取盤口快照 ──────────────────────────────────────────────
async def fetch_and_store():
    """從 The Odds API 抓 Pinnacle 賠率，存進 MongoDB"""
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
                changed = (not last_snap or last_snap.get("total") != snap["total"] or last_snap.get("ml_home") != snap["ml_home"])

                if changed or not last_snap or (ts - last_snap["ts"]) >= 500:
                    snaps_col.insert_one(snap)
                    stored += 1

            print(f"✅ 儲存 {stored}/{len(games)} 場盤口快照")
    except Exception as e:
        print(f"❌ 盤口抓取失敗: {e}")

# ── 步驟 2：💡 新增：每天中午自動對答案（抓昨日賽果與結算標籤） ─────────────────────
async def fetch_and_settle_results():
    """自動抓取最近 3 天內已完賽的 MLB 比分，並計算過盤標籤存入 MongoDB"""
    if not ODDS_API_KEY: return
    print("🔄 開始執行昨日賽果抓取與自動結算排程...")

    # 呼叫 The Odds API 的 scores 接口 (daysFrom=3 代表抓三天內)
    url = f"{BASE_URL}/sports/{SPORT}/scores/?apiKey={ODDS_API_KEY}&daysFrom=3"

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            res = await client.get(url)
            if res.status_code != 200:
                print(f"❌ 賽果 API 錯誤: {res.text}")
                return

            completed_games = res.json()
            settled_count = 0

            for game in completed_games:
                if not game.get("completed", False): # 跳過尚未完賽的場次
                    continue
                
                gid = game["id"]
                scores = game.get("scores", [])
                if not scores or len(scores) < 2: continue

                # 解析兩隊最終得分
                home_score = next((int(s["score"]) for s in scores if s["name"] == game["home_team"]), None)
                away_score = next((int(s["score"]) for s in scores if s["name"] == game["away_team"]), None)
                
                if home_score is None or away_score is None: continue
                total_outcome_score = home_score + away_score

                # 找出這場比賽在我們資料庫裡的「初盤」和「終盤」來對答案
                snaps = list(snaps_col.find({"game_id": gid}, {"_id": 0}).sort("ts", pymongo.ASCENDING))
                if not snaps: continue # 如果我們沒抓到過這場比賽的盤口，就跳過

                first_snap = snaps[0]
                last_snap  = snaps[-1]

                # 1. 結算獨贏 ML 贏家 (HOME / AWAY)
                ml_winner = "HOME" if home_score > away_score else "AWAY"

                # 2. 結算初盤總分是大還是小 (OVER / UNDER / PUSH)
                opening_total = first_snap.get("total")
                opening_total_result = "PUSH"
                if opening_total:
                    if total_outcome_score > opening_total: opening_total_result = "OVER"
                    elif total_outcome_score < opening_total: opening_total_result = "UNDER"

                # 3. 結算終盤總分是大還是小
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
                    "ml_winner": ml_winner,                  # 獨贏盤結果
                    "opening_total": opening_total,          # 初盤大小分口
                    "opening_total_result": opening_total_result, # 初盤結算
                    "closing_total": closing_total,          # 終盤大小分口
                    "closing_total_result": closing_total_result, # 終盤結算
                    "updated_at": datetime.now(timezone.utc).isoformat()
                }

                # 寫入或更新結果資料庫 (避免重複寫入)
                results_col.update_one({"game_id": gid}, {"$set": result_doc}, upsert=True)
                settled_count += 1

            print(f"🎯 昨日賽果自動結算完畢！共成功更新/結算 {settled_count} 場比賽。")
    except Exception as e:
        print(f"❌ 賽果結算失敗: {e}")

# ── 排程系統設定 ──────────────────────────────────────────────────────────────
scheduler = AsyncIOScheduler()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # 啟動時立刻抓一次盤口
    await fetch_and_store()
    
    # 排程 1：每 10 分鐘自動抓盤口
    scheduler.add_job(fetch_and_store, "interval", minutes=10, id="pinnacle_fetch")
    
    # 排程 2：💡 新增：每天中午 12:00 自動跑一次賽果對答案
    scheduler.add_job(fetch_and_settle_results, "cron", hour=12, minute=0, id="results_settle")
    
    scheduler.start()
    print("⏰ 雙排程啟動：10分鐘抓盤口 | 每天中午12:00結算昨日賽果")
    yield
    scheduler.shutdown()

# ── FastAPI App ───────────────────────────────────────────────────────────────
app = FastAPI(title="MLB Pinnacle Tracker (Closed-Loop)", lifespan=lifespan)

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
    return {"status": "ok", "service": "MLB Closed-Loop Tracker", "database": "MongoDB Atlas"}

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

# 💡 新增 API 接口：方便你之後隨時把「已經對完答案」的數據下載成 Excel / CSV 來訓練模型
@app.get("/analytics/dataset")
def get_training_dataset():
    """導出完整的機器學習特徵與標籤數據集"""
    results = list(results_col.find({}, {"_id": 0}))
    dataset = []
    
    for r in results:
        gid = r["game_id"]
        # 撈出這場比賽的所有盤口跳動歷史
        snaps = list(snaps_col.find({"game_id": gid}, {"_id": 0}).sort("ts", pymongo.ASCENDING))
        if not snaps: continue
        
        first = snaps[0]
        last = snaps[-1]
        
        dataset.append({
            "game_id": gid,
            "home": r["home"],
            "away": r["away"],
            "commence_time": r["commence_time"],
            # ── 特徵 (Model Features - X) ──
            "opening_total": first.get("total"),
            "closing_total": last.get("total"),
            "total_changed_delta": round((last.get("total", 0) - first.get("total", 0)), 2) if (last.get("total") and first.get("total")) else 0,
            "opening_ml_home": first.get("ml_home"),
            "closing_ml_home": last.get("ml_home"),
            "snapshot_records_count": len(snaps),
            # ── 標籤 (Labels - Y) ──
            "final_home_score": r["home_score"],
            "final_away_score": r["away_score"],
            "final_total_score": r["total_score"],
            "ml_winner_result": r["ml_winner"],
            "opening_total_result": r["opening_total_result"],
            "closing_total_result": r["closing_total_result"]
        })
    return dataset
