"""
MLB Pinnacle Tracker v4
- MongoDB 持久化儲存（快照 + 歷史結算）
- 每 15 分鐘自動抓 Pinnacle 盤口
- 每天自動抓賽果 + 結算昨日預測
- 昨日記錄保留一天後覆蓋
"""

import os, time, asyncio, logging
from datetime import datetime, timezone, timedelta
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
# 使用 os.environ.get 強制從環境變數讀取
ODDS_API_KEY  = os.environ.get("ODDS_API_KEY")

if not ODDS_API_KEY:
    log.error("CRITICAL: ODDS_API_KEY environment variable is NOT set!")
else:
    log.info(f"Using API Key: {ODDS_API_KEY[:4]}****")

MONGO_URI     = os.getenv("MONGO_URI", "")
SPORT         = "baseball_mlb"
BOOKMAKER     = "pinnacle"
ODDS_BASE     = "https://api.the-odds-api.com/v4"
SCORES_URL    = f"{ODDS_BASE}/sports/{SPORT}/scores/?apiKey={ODDS_API_KEY}&daysFrom=1&dateFormat=iso"
ODDS_URL      = (f"{ODDS_BASE}/sports/{SPORT}/odds/"
                  f"?apiKey={ODDS_API_KEY}&regions=us"
                  f"&markets=h2h,totals,spreads&bookmakers={BOOKMAKER}&oddsFormat=american")

# ── MongoDB ───────────────────────────────────────────────────────────────────
client  = None
db      = None

def get_db():
    return db

# ── Helpers ───────────────────────────────────────────────────────────────────
def utc_now():
    return datetime.now(timezone.utc)

def et_date_str(dt=None):
    """Return YYYY-MM-DD in ET timezone"""
    if dt is None:
        dt = utc_now()
    et = dt.astimezone(timezone(timedelta(hours=-4)))
    return et.strftime("%Y-%m-%d")

def signal_from_snaps(snaps: list) -> dict:
    """Calculate break signal from snapshot history"""
    if len(snaps) < 2:
        return {"total": "FLAT", "ml": "FLAT", "delta": 0}
    first, last = snaps[0], snaps[-1]
    td = round((last.get("total") or 0) - (first.get("total") or 0), 1)
    md = (last.get("ml_home") or 0) - (first.get("ml_home") or 0)
    total_sig = ("STEAM_OVER" if td >= 0.5 else
                 "LEAN_OVER"  if td >= 0.25 else
                 "STEAM_UNDER" if td <= -0.5 else
                 "LEAN_UNDER"  if td <= -0.25 else "FLAT")
    ml_sig = ("STEAM_HOME" if md <= -15 else
              "STEAM_AWAY" if md >= 15 else "FLAT")
    return {"total": total_sig, "ml": ml_sig, "delta": td}

def pick_from_signal(sig: dict, game: dict) -> str | None:
    """Derive bet pick from signal"""
    d = sig.get("delta", 0)
    total = game.get("latest", {}).get("total")
    if d >= 0.5:  return f"OVER {total}"
    if d <= -0.5: return f"UNDER {total}"
    if d >= 0.25: return f"OVER {total} (lean)"
    if d <= -0.25:return f"UNDER {total} (lean)"
    return None

# ── Fetch Odds ────────────────────────────────────────────────────────────────
async def fetch_and_store_odds():
    """Fetch Pinnacle odds and store snapshots to MongoDB"""
    if not ODDS_API_KEY:
        log.warning("No ODDS_API_KEY")
        return
    try:
        async with httpx.AsyncClient(timeout=20) as client_http:
            r = await client_http.get(ODDS_URL)
            remaining = r.headers.get("x-requests-remaining", "?")
            log.info(f"Odds API → {r.status_code} | remaining: {remaining}")
            if r.status_code != 200:
                log.error(f"Odds API error: {r.text[:200]}")
                return
            games = r.json()

        ts    = utc_now()
        today = et_date_str(ts)
        coll  = get_db()["snapshots"]
        stored = 0

        for g in games:
            pin    = next((b for b in g.get("bookmakers", []) if b["key"] == BOOKMAKER), None)
            if not pin:
                continue
            totals = next((m for m in pin["markets"] if m["key"] == "totals"), None)
            h2h    = next((m for m in pin["markets"] if m["key"] == "h2h"), None)
            spreads= next((m for m in pin["markets"] if m["key"] == "spreads"), None)

            over   = next((o for o in (totals or {}).get("outcomes", []) if o["name"] == "Over"), None)
            under  = next((o for o in (totals or {}).get("outcomes", []) if o["name"] == "Under"), None)
            ml_home= next((o for o in (h2h or {}).get("outcomes", []) if o["name"] == g["home_team"]), None)
            ml_away= next((o for o in (h2h or {}).get("outcomes", []) if o["name"] == g["away_team"]), None)
            sp_home= next((o for o in (spreads or {}).get("outcomes", []) if o["name"] == g["home_team"]), None)

            snap = {
                "ts":          ts,
                "total":       over["point"]  if over  else None,
                "over_juice":  over["price"]  if over  else None,
                "under_juice": under["price"] if under else None,
                "ml_home":     ml_home["price"] if ml_home else None,
                "ml_away":     ml_away["price"] if ml_away else None,
                "spread_home": sp_home["point"] if sp_home else None,
            }

            # Upsert game doc with new snapshot
            existing = await coll.find_one({"game_id": g["id"], "date": today})
            if existing:
                last_snap = existing["snapshots"][-1] if existing["snapshots"] else {}
                changed = (last_snap.get("total") != snap["total"] or
                           last_snap.get("ml_home") != snap["ml_home"])
                age_mins = (ts - last_snap["ts"].replace(tzinfo=timezone.utc)).total_seconds() / 60 if last_snap.get("ts") else 9999
                force = age_mins >= 15

                if changed or force:
                    snaps = existing["snapshots"][-49:] + [snap]  # keep last 50
                    sig = signal_from_snaps(snaps)
                    await coll.update_one(
                        {"_id": existing["_id"]},
                        {"$set": {
                            "snapshots":  snaps,
                            "latest":     snap,
                            "signal":     sig,
                            "updated_at": ts,
                        }}
                    )
                    stored += 1
            else:
                sig = signal_from_snaps([snap])
                await coll.insert_one({
                    "game_id":      g["id"],
                    "date":         today,
                    "home":         g["home_team"],
                    "away":         g["away_team"],
                    "commence_time":g["commence_time"],
                    "snapshots":    [snap],
                    "latest":       snap,
                    "open":         snap,
                    "signal":       sig,
                    "created_at":   ts,
                    "updated_at":   ts,
                })
                stored += 1

        log.info(f"✅ Stored {stored}/{len(games)} snapshots | date={today}")

    except Exception as e:
        log.error(f"fetch_odds error: {e}")

# ── Fetch Scores & Settle ─────────────────────────────────────────────────────
async def fetch_and_settle():
    """Fetch game scores and settle yesterday's predictions"""
    if not ODDS_API_KEY:
        return
    try:
        async with httpx.AsyncClient(timeout=20) as client_http:
            r = await client_http.get(SCORES_URL)
            if r.status_code != 200:
                log.warning(f"Scores API error: {r.status_code}")
                return
            scores = r.json()

        ts       = utc_now()
        yesterday = et_date_str(ts - timedelta(days=1))
        snaps_coll= get_db()["snapshots"]
        hist_coll = get_db()["history"]
        settled   = 0

        for s in scores:
            if not s.get("completed"):
                continue
            scores_data = s.get("scores") or []
            if len(scores_data) < 2:
                continue

            # Get total runs
            try:
                total_runs = sum(int(sc["score"]) for sc in scores_data)
            except Exception:
                continue

            # Find our game record from yesterday
            game_doc = await snaps_coll.find_one({
                "game_id": s["id"],
                "date":    yesterday
            })
            if not game_doc:
                # Try today too (late games)
                game_doc = await snaps_coll.find_one({"game_id": s["id"]})

            if not game_doc:
                continue

            snaps  = game_doc.get("snapshots", [])
            sig    = signal_from_snaps(snaps)
            pick   = pick_from_signal(sig, game_doc)
            total  = game_doc.get("open", {}).get("total")
            result = None

            if pick and total:
                if "OVER" in pick:
                    result = "WIN" if total_runs > total else "LOSS" if total_runs < total else "PUSH"
                elif "UNDER" in pick:
                    result = "WIN" if total_runs < total else "LOSS" if total_runs > total else "PUSH"

            hist_entry = {
                "game_id":      s["id"],
                "date":         yesterday,
                "home":         game_doc["home"],
                "away":         game_doc["away"],
                "commence_time":game_doc["commence_time"],
                "open_total":   total,
                "close_total":  game_doc.get("latest", {}).get("total"),
                "total_delta":  sig.get("delta", 0),
                "signal":       sig,
                "pick":         pick,
                "actual_total": total_runs,
                "result":       result,
                "settled_at":   ts,
            }

            # Upsert history
            await hist_coll.update_one(
                {"game_id": s["id"], "date": yesterday},
                {"$set": hist_entry},
                upsert=True
            )
            # Update snapshot with result
            await snaps_coll.update_one(
                {"_id": game_doc["_id"]},
                {"$set": {"result": result, "actual_total": total_runs}}
            )
            settled += 1

        # Clean up history older than 2 days
        cutoff = et_date_str(ts - timedelta(days=2))
        del_result = await hist_coll.delete_many({"date": {"$lt": cutoff}})
        log.info(f"✅ Settled {settled} games | deleted {del_result.deleted_count} old records")

    except Exception as e:
        log.error(f"settle error: {e}")

# ── Scheduler ─────────────────────────────────────────────────────────────────
async def scheduler():
    """Run every 15 minutes"""
    while True:
        await fetch_and_store_odds()
        # Settle once per cycle (cheap, idempotent)
        await fetch_and_settle()
        await asyncio.sleep(15 * 60)

# ── App Startup ───────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    global client, db
    client = AsyncIOMotorClient(MONGO_URI)
    db     = client["mlb_tracker"]
    # Indexes
    await db["snapshots"].create_index([("game_id", 1), ("date", 1)], unique=True)
    await db["history"].create_index([("game_id", 1), ("date", 1)], unique=True)
    log.info("✅ MongoDB connected")
    # First fetch immediately
    await fetch_and_store_odds()
    await fetch_and_settle()
    # Start scheduler
    asyncio.create_task(scheduler())
    log.info("⏰ Scheduler started: every 15 min")
    yield
    client.close()

app = FastAPI(title="MLB Pinnacle Tracker v4", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["GET", "POST"], allow_headers=["*"])

# ── Endpoints ─────────────────────────────────────────────────────────────────
@app.get("/")
async def root():
    return {"status": "ok", "version": "4.0", "service": "MLB Pinnacle Tracker"}

@app.get("/games")
async def get_games():
    """Today's games with full snapshot history"""
    today = et_date_str()
    coll  = get_db()["snapshots"]
    docs  = await coll.find({"date": today}).sort("commence_time", 1).to_list(50)
    result = []
    for d in docs:
        snaps = d.get("snapshots", [])
        first = snaps[0]  if snaps else {}
        last  = snaps[-1] if snaps else {}
        sig   = signal_from_snaps(snaps)
        result.append({
            "game_id":       d["game_id"],
            "home":          d["home"],
            "away":          d["away"],
            "commence_time": d["commence_time"],
            "snapshot_count": len(snaps),
            "open":  {"total": first.get("total"), "ml_home": first.get("ml_home"), "ml_away": first.get("ml_away")},
            "latest":{"total": last.get("total"),  "over_juice": last.get("over_juice"), "under_juice": last.get("under_juice"), "ml_home": last.get("ml_home"), "ml_away": last.get("ml_away"), "spread_home": last.get("spread_home")},
            "delta": {"total": sig["delta"], "ml": 0},
            "signal":{"total": sig["total"], "ml": sig["ml"]},
            "history":[{"ts": s["ts"].isoformat() if hasattr(s["ts"],"isoformat") else str(s["ts"]), "total": s.get("total"), "over_juice": s.get("over_juice"), "under_juice": s.get("under_juice"), "ml_home": s.get("ml_home"), "ml_away": s.get("ml_away")} for s in snaps],
            "result":        d.get("result"),
            "actual_total":  d.get("actual_total"),
        })
    return result

@app.get("/history")
async def get_history():
    """Yesterday's settlement results"""
    yesterday = et_date_str(utc_now() - timedelta(days=1))
    coll      = get_db()["history"]
    docs      = await coll.find({"date": yesterday}).sort("commence_time", 1).to_list(30)
    result    = []
    for d in docs:
        result.append({
            "game_id
