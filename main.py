"""
MLB Pinnacle 盤口快照後端 (MongoDB 版)
- 每 10 分鐘自動抓一次 Pinnacle MLB 賠率
- 永久儲存每場比賽的歷史快照至 MongoDB Atlas
- 提供 REST API 給前端儀表板讀取
"""

import os
import time
import httpx
from datetime import datetime, timezone
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import pymongo
import certifi  # 💡 新增：用來解決 SSL handshake failed 的憑證套件
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
    # 💡 新增：tlsCAFile=certifi.where() 強制使用最新的安全憑證連線
    client = pymongo.MongoClient(MONGO_URI, tlsCAFile=certifi.where())
    db = client["mlb_tracker"]
    snaps_col = db["snapshots"]
    print("✅ 成功連線至 MongoDB Atlas 雲端資料庫！")
except Exception as e:
    print(f"❌ MongoDB 連線失敗: {e}")

# ── Fetch + Store ─────────────────────────────────────────────────────────────
async def fetch_and_store():
    """從 The Odds API 抓 Pinnacle 賠率，存進 MongoDB"""
    if not ODDS_API_KEY:
        print("❌ 缺少 ODDS_API_KEY")
        return

    url = (
        f"{BASE_URL}/sports/{SPORT}/odds/"
        f"?apiKey={ODDS_API_KEY}"
        f"&regions=us"
        f"&markets={MARKETS}"
        f"&bookmakers={BOOKMAKER}"
        f"&oddsFormat={ODDS_FORMAT}"
    )

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            res = await client.get(url)
            remaining = res.headers.get("x-requests-remaining", "?")
            print(f"[{datetime.now().strftime('%H:%M')}] API 回應 {res.status_code} | 剩餘請求: {remaining}")

            if res.status_code != 200:
                print(f"❌ API 錯誤: {res.text}")
                return

            games = res.json()
            ts    = int(time.time())
            stored = 0

            for game in games:
                pin    = next((b for b in game.get("bookmakers", []) if b["key"] == BOOKMAKER), None)
                if not pin:
                    continue

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
                    # Totals
                    "total":        over["point"]  if over  else None,
                    "over_juice":   over["price"]  if over  else None,
                    "under_juice":  under["price"] if under else None,
                    # ML
                    "ml_home":      ml_home["price"] if ml_home else None,
                    "ml_away":      ml_away["price"] if ml_away else None,
                    # Spread
                    "spread_home":  sp_home["point"] if sp_home else None,
                }

                last_snap = snaps_col.find_one({"game_id": game["id"]}, sort=[("ts", pymongo.DESCENDING)])

                changed = (
                    not last_snap
                    or last_snap.get("total")   != snap["total"]
                    or last_snap.get("ml_home") != snap["ml_home"]
                    or last_snap.get("ml_away") != snap["ml_away"]
                )

                if changed or not last_snap or (ts - last_snap["ts"]) >= 500:
                    snaps_col.insert_one(snap)
                    stored += 1

            print(f"✅ 儲存 {stored}/{len(games)} 場快照至 MongoDB")

    except Exception as e:
        print(f"❌ fetch 失敗: {e}")

# ── Scheduler ─────────────────────────────────────────────────────────────────
scheduler = AsyncIOScheduler()

@asynccontextmanager
async def lifespan(app: FastAPI):
    await fetch_and_store()
    scheduler.add_job(fetch_and_store, "interval", minutes=10, id="pinnacle_fetch")
    scheduler.start()
    print("⏰ 排程啟動：每 10 分鐘自動抓取並寫入 MongoDB")
    yield
    scheduler.shutdown()

# ── FastAPI App ───────────────────────────────────────────────────────────────
app = FastAPI(title="MLB Pinnacle Tracker (MongoDB)", lifespan=lifespan)

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
    return {"status": "ok", "service": "MLB Pinnacle Tracker", "database": "MongoDB Atlas"}

@app.get("/games")
def get_games():
    cutoff_time = datetime.fromtimestamp(time.time() - (4 * 3600), tz=timezone.utc).isoformat()
    all_snaps = list(snaps_col.find({"commence_time": {"$gte": cutoff_time}}, {"_id": 0}))

    games: dict[str, list] = {}
    for s in all_snaps:
        gid = s["game_id"]
        if gid not in games:
            games[gid] = []
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
