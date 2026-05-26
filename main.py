"""
MLB Pinnacle 盤⼝快照後端
- 每 30 分鐘⾃動抓⼀次 Pinnacle MLB 賠率
- 儲存每場比賽的歷史快照
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
from tinydb import TinyDB, Query
from dotenv import load_dotenv
load_dotenv()
# ── Config ────────────────────────────────────────────────────────────────────
ODDS_API_KEY = os.getenv("ODDS_API_KEY", "")
SPORT = "baseball_mlb"
BOOKMAKER = "pinnacle"
MARKETS = "h2h,totals,spreads"
ODDS_FORMAT = "american"
BASE_URL = "https://api.the-odds-api.com/v4"
# ── Database (TinyDB = single JSON file, zero config) ─────────────────────────
db = TinyDB("/data/snapshots.json")
snaps_tbl = db.table("snapshots") # 每次快照的原始數據
history_tbl = db.table("history") # 每場比賽的移動歷史
# ── Fetch + Store ─────────────────────────────────────────────────────────────
async def fetch_and_store():
"""從 The Odds API 抓 Pinnacle 賠率，存進 TinyDB"""
if not ODDS_API_KEY:
print(" 缺少 ODDS_API_KEY")
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
print(f"[{datetime.now().strftime('%H:%M')}] API 回應 {res.status_code} | 剩餘請求: if res.status_code != 200:
print(f" API 錯誤: {res.text}")
return
games = res.json()
ts = int(time.time())
stored = 0
for game in games:
pin = next((b for b in game.get("bookmakers", []) if b["key"] == BOOKMAKER), if not pin:
continue
totals = next((m for m in pin["markets"] if m["key"] == "totals"), None)
h2h = next((m for m in pin["markets"] if m["key"] == "h2h"), None)
spreads = next((m for m in pin["markets"] if m["key"] == "spreads"), None)
over = next((o for o in (totals or {}).get("outcomes", []) if o["name"] == under = next((o for o in (totals or {}).get("outcomes", []) if o["name"] == ml_home = next((o for o in (h2h or {}).get("outcomes", []) if o["name"] == ml_away = next((o for o in (h2h or {}).get("outcomes", []) if o["name"] == sp_home = next((o for o in (spreads or {}).get("outcomes", []) if o["name"] == snap = {
"game_id": game["id"],
"home": game["home_team"],
"away": game["away_team"],
"commence_time": game["commence_time"],
"ts": ts,
"ts_iso": datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(),
# Totals
"total": over["point"] if over else None,
"over_juice": over["price"] if over else None,
"under_juice": under["price"] if under else None,
# ML
"ml_home": ml_home["price"] if ml_home else None,
"ml_away": ml_away["price"] if ml_away else None,
# Spread
"spread_home": sp_home["point"] if sp_home else None,
}
# 只在數據有變化時才存（避免重複快照）
G = Query()
last = (
snaps_tbl.search(G.game_id == game["id"])
)
last = sorted(last, key=lambda x: x["ts"])[-1] if last else None
changed = (
not last
or last.get("total") != snap["total"]
or last.get("ml_home") != snap["ml_home"]
or last.get("ml_away") != snap["ml_away"]
)
# 每 30 分鐘強制存⼀次（即使沒變化，留下時間軸記錄）
force = not last or (ts - last["ts"]) >= 1800
if changed or force:
snaps_tbl.insert(snap)
stored += 1
print(f" 儲存 {stored}/{len(games)} 場快照")
except Exception as e:
print(f" fetch 失敗: {e}")
# ── Scheduler ─────────────────────────────────────────────────────────────────
scheduler = AsyncIOScheduler()
@asynccontextmanager
async def lifespan(app: FastAPI):
# 啟動時立刻抓⼀次
await fetch_and_store()
# 之後每 30 分鐘
scheduler.add_job(fetch_and_store, "interval", minutes=30, id="pinnacle_fetch")
scheduler.start()
print(" 排程啟動：每 30 分鐘抓⼀次 Pinnacle")
yield
scheduler.shutdown()
# ── FastAPI App ───────────────────────────────────────────────────────────────
app = FastAPI(title="MLB Pinnacle Tracker", lifespan=lifespan)
app.add_middleware(
CORSMiddleware,
allow_origins=["*"], # 前端 Artifact 呼叫需要
allow_methods=["GET"],
allow_headers=["*"],
)
# ── API Endpoints ─────────────────────────────────────────────────────────────
@app.get("/")
def root():
return {"status": "ok", "service": "MLB Pinnacle Tracker"}
@app.get("/games")
def get_games():
"""
回傳今天所有比賽 + 每場的完整快照歷史
前端⽤這個畫移動曲線和計算破⼝
"""
G = Query()
all_snaps = snaps_tbl.all()
# 按 game_id 分組
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
first = snaps_sorted[0]
# 計算移動幅度
total_delta = None
ml_home_delta = None
if latest.get("total") is not None and first.get("total") is not None:
total_delta = round(latest["total"] - first["total"], 1)
if latest.get("ml_home") is not None and first.get("ml_home") is not None:
ml_home_delta = latest["ml_home"] - first["ml_home"]
# 破⼝信號
total_signal = "FLAT"
if total_delta is not None:
if abs(total_delta) >= 0.5: total_signal = "STEAM_OVER" if total_delta > 0 else elif abs(total_delta) >= 0.25: total_signal = "LEAN_OVER" if total_delta > 0 else ml_signal = "FLAT"
if ml_home_delta is not None and abs(ml_home_delta) >= 15:
ml_signal = "STEAM_HOME" if ml_home_delta < 0 else "STEAM_AWAY"
result.append({
"game_id": gid,
"home": latest["home"],
"away": latest["away"],
"commence_time": latest["commence_time"],
"snapshot_count": len(snaps_sorted),
"first_snap_ts": first["ts_iso"],
"latest": {
"ts": latest["ts_iso"],
"total": latest.get("total"),
"over_juice": latest.get("over_juice"),
"under_juice": latest.get("under_juice"),
"ml_home": latest.get("ml_home"),
"ml_away": latest.get("ml_away"),
"spread_home": latest.get("spread_home"),
},
"open": {
"total": first.get("total"),
"ml_home": first.get("ml_home"),
"ml_away": first.get("ml_away"),
},
"delta": {
"total": total_delta,
"ml_home": ml_home_delta,
},
"signal": {
"total": total_signal,
"ml": ml_signal,
},
# 完整時間序列，前端畫折線圖⽤
"history": [
{
"ts": s["ts_iso"],
"total": s.get("total"),
"ml_home": s.get("ml_home"),
"ml_away": s.get("ml_away"),
}
for s in snaps_sorted
],
})
# 按開賽時間排序
result.sort(key=lambda x: x["commence_time"])
return result
@app.get("/games/{game_id}/history")
def get_game_history(game_id: str):
"""單場比賽的完整快照歷史"""
G = Query()
snaps = snaps_tbl.search(G.game_id == game_id)
return sorted(snaps, key=lambda x: x["ts"])
@app.delete("/snapshots/old")
def purge_old_snapshots(days: int = 3):
"""清除 N 天前的快照（節省空間）"""
cutoff = int(time.time()) - (days * 86400)
G = Query()
removed = snaps_tbl.remove(G.ts < cutoff)
return {"removed": len(removed)}
