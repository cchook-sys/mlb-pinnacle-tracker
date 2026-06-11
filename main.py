"""
MLB Pinnacle Tracker v4 - Final Clean Version
- MongoDB 持久化快照
- 每 60 分鐘自動抓 Pinnacle 盤口
- 自動賽果結算 + 昨日歷史
"""

import os, asyncio, logging
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
ODDS_API_KEY = os.getenv("ODDS_API_KEY", "")
MONGO_URI    = os.getenv("MONGO_URI", "")
BOOKMAKER    = "pinnacle"
SPORT        = "baseball_mlb"
ODDS_BASE    = f"https://api.the-odds-api.com/v4/sports/{SPORT}"

client_db = None
db        = None

def get_db():
    return db

# ── Helpers ───────────────────────────────────────────────────────────────────
def utc_now():
    return datetime.now(timezone.utc)

def et_date_str(dt=None):
    if dt is None:
        dt = utc_now()
    return dt.astimezone(timezone(timedelta(hours=-4))).strftime("%Y-%m-%d")

def signal_from_snaps(snaps: list) -> dict:
    valid = [s for s in snaps if s.get("total") is not None]
    if len(valid) < 2:
        return {"total": "FLAT", "ml": "FLAT", "delta": 0}
    td = round(valid[-1]["total"] - valid[0]["total"], 1)
    ts = ("STEAM_OVER"  if td >= 0.5  else
          "LEAN_OVER"   if td >= 0.25 else
          "STEAM_UNDER" if td <= -0.5 else
          "LEAN_UNDER"  if td <= -0.25 else "FLAT")
    mld = (valid[-1].get("ml_home") or 0) - (valid[0].get("ml_home") or 0)
    ms  = ("STEAM_HOME" if mld <= -15 else "STEAM_AWAY" if mld >= 15 else "FLAT")
    return {"total": ts, "ml": ms, "delta": td}

def pick_from_signal(sig, game):
    d     = sig.get("delta", 0)
    total = game.get("latest", {}).get("total")
    if d >= 0.25:  return f"OVER {total}"
    if d <= -0.25: return f"UNDER {total}"
    return None

def serialize(doc):
    """Remove _id and convert datetime for JSON"""
    doc.pop("_id", None)
    for k, v in doc.items():
        if isinstance(v, datetime):
            doc[k] = v.isoformat()
        elif isinstance(v, list):
            for i, item in enumerate(v):
                if isinstance(item, dict):
                    for kk, vv in item.items():
                        if isinstance(vv, datetime):
                            item[kk] = vv.isoformat()
    return doc

# ── Fetch Odds ────────────────────────────────────────────────────────────────
async def fetch_and_store_odds():
    if not ODDS_API_KEY:
        log.error("ODDS_API_KEY not set")
        return
    try:
        url = (f"{ODDS_BASE}/odds/"
               f"?apiKey={ODDS_API_KEY}&regions=us"
               f"&markets=h2h,totals,spreads&bookmakers={BOOKMAKER}&oddsFormat=american")
        async with httpx.AsyncClient(timeout=25) as c:
            r = await c.get(url)
        rem = r.headers.get("x-requests-remaining", "?")
        log.info(f"Odds API {r.status_code} | remaining={rem}")
        if r.status_code != 200:
            log.error(f"Odds error: {r.text[:200]}")
            return

        games = r.json()
        ts    = utc_now()
        today = et_date_str(ts)
        coll  = get_db()["snapshots"]
        stored = 0

        for g in games:
            pin     = next((b for b in g.get("bookmakers", []) if b["key"] == BOOKMAKER), None)
            if not pin:
                continue
            totals  = next((m for m in pin["markets"] if m["key"] == "totals"),  None)
            h2h     = next((m for m in pin["markets"] if m["key"] == "h2h"),     None)
            spreads = next((m for m in pin["markets"] if m["key"] == "spreads"), None)

            over    = next((o for o in (totals  or {}).get("outcomes", []) if o["name"] == "Over"),           None)
            under   = next((o for o in (totals  or {}).get("outcomes", []) if o["name"] == "Under"),          None)
            ml_home = next((o for o in (h2h     or {}).get("outcomes", []) if o["name"] == g["home_team"]),   None)
            ml_away = next((o for o in (h2h     or {}).get("outcomes", []) if o["name"] == g["away_team"]),   None)
            sp_home = next((o for o in (spreads or {}).get("outcomes", []) if o["name"] == g["home_team"]),   None)

            snap = {
                "ts":          ts,
                "total":       over["point"]    if over    else None,
                "over_juice":  over["price"]    if over    else None,
                "under_juice": under["price"]   if under   else None,
                "ml_home":     ml_home["price"] if ml_home else None,
                "ml_away":     ml_away["price"] if ml_away else None,
                "spread_home": sp_home["point"] if sp_home else None,
            }

            existing = await coll.find_one({"game_id": g["id"], "date": today})
            if existing:
                prev_snaps = existing.get("snapshots", [])
                last = prev_snaps[-1] if prev_snaps else {}
                changed  = last.get("total") != snap["total"] or last.get("ml_home") != snap["ml_home"]
                age_mins = ((ts - last["ts"].replace(tzinfo=timezone.utc)).total_seconds() / 60
                            if isinstance(last.get("ts"), datetime) else 999)
                if changed or age_mins >= 15:
                    new_snaps = prev_snaps[-49:] + [snap]
                    sig = signal_from_snaps(new_snaps)
                    await coll.update_one(
                        {"_id": existing["_id"]},
                        {"$set": {
                            "snapshots":  new_snaps,
                            "latest":     snap,
                            "signal":     sig,
                            "updated_at": ts,
                        }}
                    )
                    stored += 1
            else:
                sig = signal_from_snaps([snap])
                await coll.insert_one({
                    "game_id":       g["id"],
                    "date":          today,
                    "home":          g["home_team"],
                    "away":          g["away_team"],
                    "commence_time": g["commence_time"],
                    "snapshots":     [snap],
                    "open":          snap,
                    "latest":        snap,
                    "signal":        sig,
                    "created_at":    ts,
                    "updated_at":    ts,
                })
                stored += 1

        log.info(f"✅ Stored {stored}/{len(games)} | date={today}")

    except Exception as e:
        log.error(f"fetch_odds error: {e}")

# ── Fetch Scores & Settle ─────────────────────────────────────────────────────
async def fetch_and_settle():
    if not ODDS_API_KEY:
        return
    try:
        url = (f"{ODDS_BASE}/scores/"
               f"?apiKey={ODDS_API_KEY}&daysFrom=1&dateFormat=iso")
        async with httpx.AsyncClient(timeout=25) as c:
            r = await c.get(url)
        if r.status_code != 200:
            return

        scores    = r.json()
        ts        = utc_now()
        yesterday = et_date_str(ts - timedelta(days=1))
        snaps_col = get_db()["snapshots"]
        hist_col  = get_db()["history"]
        settled   = 0

        for s in scores:
            if not s.get("completed"):
                continue
            score_data = s.get("scores") or []
            if len(score_data) < 2:
                continue
            try:
                total_runs = sum(int(sc["score"]) for sc in score_data)
            except Exception:
                continue

            game_doc = await snaps_col.find_one({"game_id": s["id"], "date": yesterday})
            if not game_doc:
                game_doc = await snaps_col.find_one({"game_id": s["id"]})
            if not game_doc:
                continue

            snaps  = game_doc.get("snapshots", [])
            sig    = signal_from_snaps(snaps)
            pick   = pick_from_signal(sig, game_doc)
            total  = (game_doc.get("open") or {}).get("total")
            result = None

            if pick and total:
                if "OVER"  in pick: result = "WIN" if total_runs > total else "LOSS" if total_runs < total else "PUSH"
                if "UNDER" in pick: result = "WIN" if total_runs < total else "LOSS" if total_runs > total else "PUSH"

            entry = {
                "game_id":       s["id"],
                "date":          yesterday,
                "home":          game_doc["home"],
                "away":          game_doc["away"],
                "commence_time": game_doc["commence_time"],
                "open_total":    total,
                "close_total":   (game_doc.get("latest") or {}).get("total"),
                "total_delta":   sig.get("delta", 0),
                "signal":        sig,
                "pick":          pick,
                "actual_total":  total_runs,
                "result":        result,
                "settled_at":    ts,
            }
            await hist_col.update_one(
                {"game_id": s["id"], "date": yesterday},
                {"$set": entry},
                upsert=True
            )
            await snaps_col.update_one(
                {"_id": game_doc["_id"]},
                {"$set": {"result": result, "actual_total": total_runs}}
            )
            settled += 1

        # Delete history older than 2 days
        cutoff = et_date_str(ts - timedelta(days=2))
        await hist_col.delete_many({"date": {"$lt": cutoff}})
        log.info(f"✅ Settled {settled} games")

    except Exception as e:
        log.error(f"settle error: {e}")

# ── Scheduler ─────────────────────────────────────────────────────────────────
async def scheduler():
    while True:
        await fetch_and_store_odds()
        await fetch_and_settle()
        await asyncio.sleep(60 * 60)   # 60 分鐘

# ── App ───────────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    global client_db, db
    if MONGO_URI:
        client_db = AsyncIOMotorClient(MONGO_URI)
        db        = client_db["mlb_tracker"]
        await db["snapshots"].create_index([("game_id", 1), ("date", 1)], unique=True)
        await db["history"].create_index([("game_id", 1), ("date", 1)],   unique=True)
        log.info("✅ MongoDB connected")
    else:
        log.error("MONGO_URI not set")

    # First run immediately
    await fetch_and_store_odds()
    await fetch_and_settle()
    asyncio.create_task(scheduler())
    log.info("⏰ Scheduler: every 15 min")
    yield
    if client_db:
        client_db.close()

app = FastAPI(title="MLB Pinnacle Tracker v4", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# ── Routes ────────────────────────────────────────────────────────────────────
@app.get("/")
async def root():
    return {"status": "ok", "version": "4.0"}

@app.get("/games")
async def get_games():
    today = et_date_str()
    docs  = await get_db()["snapshots"].find({"date": today}).sort("commence_time", 1).to_list(50)
    result = []
    for d in docs:
        snaps = d.get("snapshots", [])
        first = snaps[0]  if snaps else {}
        last  = snaps[-1] if snaps else {}
        sig   = signal_from_snaps(snaps)
        hist  = []
        for s in snaps:
            ts_val = s.get("ts")
            hist.append({
                "ts":          ts_val.isoformat() if isinstance(ts_val, datetime) else str(ts_val),
                "total":       s.get("total"),
                "over_juice":  s.get("over_juice"),
                "under_juice": s.get("under_juice"),
                "ml_home":     s.get("ml_home"),
                "ml_away":     s.get("ml_away"),
            })
        result.append({
            "game_id":        d["game_id"],
            "home":           d["home"],
            "away":           d["away"],
            "commence_time":  d["commence_time"],
            "snapshot_count": len(snaps),
            "open":  {"total": first.get("total"), "ml_home": first.get("ml_home"), "ml_away": first.get("ml_away")},
            "latest":{"total": last.get("total"),  "over_juice": last.get("over_juice"), "under_juice": last.get("under_juice"), "ml_home": last.get("ml_home"), "ml_away": last.get("ml_away"), "spread_home": last.get("spread_home")},
            "delta": {"total": sig["delta"]},
            "signal":{"total": sig["total"], "ml": sig["ml"]},
            "history": hist,
            "result":         d.get("result"),
            "actual_total":   d.get("actual_total"),
        })
    return result

@app.get("/history")
async def get_history():
    yesterday = et_date_str(utc_now() - timedelta(days=1))
    docs      = await get_db()["history"].find({"date": yesterday}).sort("commence_time", 1).to_list(30)
    result    = []
    for d in docs:
        result.append({
            "game_id":       d["game_id"],
            "home":          d["home"],
            "away":          d["away"],
            "commence_time": d["commence_time"],
            "date":          d["date"],
            "open_total":    d.get("open_total"),
            "close_total":   d.get("close_total"),
            "total_delta":   d.get("total_delta", 0),
            "pick":          d.get("pick"),
            "actual_total":  d.get("actual_total"),
            "result":        d.get("result"),
            "signal":        d.get("signal", {}),
        })
    return result

@app.get("/stats")
async def get_stats():
    docs   = await get_db()["history"].find({"result": {"$in": ["WIN","LOSS","PUSH"]}}).to_list(200)
    wins   = sum(1 for d in docs if d.get("result") == "WIN")
    losses = sum(1 for d in docs if d.get("result") == "LOSS")
    pushes = sum(1 for d in docs if d.get("result") == "PUSH")
    total  = wins + losses
    return {
        "wins": wins, "losses": losses, "pushes": pushes,
        "total": total + pushes,
        "win_rate": round(wins / total * 100, 1) if total > 0 else 0,
        "roi": round((wins * 0.91 - losses) / total * 100, 1) if total > 0 else 0,
    }
