"""
MLB Pinnacle Tracker v4.1
- 智慧排程：ET 08:00–23:00 每 30 分鐘抓取，其餘時間睡眠
- 盤口沒變動不存快照（節省 MongoDB 空間）
- GET /refresh 可手動喚醒立即抓取
- MongoDB 持久化 + 自動賽果結算
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

# 排程設定（ET 時間）
ACTIVE_START = 8   # 08:00 ET 開始
ACTIVE_END   = 23  # 23:00 ET 結束
FETCH_INTERVAL_MINS = 30  # 主動抓取間隔（分鐘）

client_db = None
db        = None
_last_fetch_ts = None  # 上次成功抓取時間

def get_db():
    return db

# ── Helpers ───────────────────────────────────────────────────────────────────
def utc_now():
    return datetime.now(timezone.utc)

def et_now():
    return utc_now().astimezone(timezone(timedelta(hours=-4)))

def et_date_str(dt=None):
    if dt is None:
        dt = utc_now()
    return dt.astimezone(timezone(timedelta(hours=-4))).strftime("%Y-%m-%d")

def is_active_hours() -> bool:
    """ET 08:00–23:00 為活躍時段"""
    h = et_now().hour
    return ACTIVE_START <= h < ACTIVE_END

def signal_from_snaps(snaps: list) -> dict:
    valid = [s for s in snaps if s.get("total") is not None]
    if len(valid) < 2:
        return {"total": "FLAT", "ml": "FLAT", "delta": 0}
    td = round(valid[-1]["total"] - valid[0]["total"], 1)
    ts = ("STEAM_OVER"  if td >= 1.0  else
          "LEAN_OVER"   if td >= 0.5 else
          "STEAM_UNDER" if td <= -1.0 else
          "LEAN_UNDER"  if td <= -0.5 else "FLAT")
    mld = (valid[-1].get("ml_home") or 0) - (valid[0].get("ml_home") or 0)
    ms  = ("STEAM_HOME" if mld <= -15 else "STEAM_AWAY" if mld >= 15 else "FLAT")
    return {"total": ts, "ml": ms, "delta": td}

def pick_from_signal(sig, game):
    d     = sig.get("delta", 0)
    total = game.get("latest", {}).get("total")
    if d >= 0.25:  return f"OVER {total}"
    if d <= -0.25: return f"UNDER {total}"
    return None

# ── Fetch Odds ────────────────────────────────────────────────────────────────
async def fetch_and_store_odds(force: bool = False) -> dict:
    """
    抓取 Pinnacle 盤口並存入 MongoDB
    force=True 時跳過時段限制（手動觸發）
    回傳 {"stored": int, "skipped": int, "remaining": str}
    """
    global _last_fetch_ts

    if not ODDS_API_KEY:
        return {"error": "ODDS_API_KEY not set"}

    if not force and not is_active_hours():
        h = et_now().hour
        log.info(f"😴 睡眠時段 ET {h:02d}:00，跳過抓取")
        return {"skipped": "sleep_hours", "et_hour": h}

    try:
        url = (f"{ODDS_BASE}/odds/"
               f"?apiKey={ODDS_API_KEY}&regions=us"
               f"&markets=h2h,totals,spreads&bookmakers={BOOKMAKER}&oddsFormat=american")

        async with httpx.AsyncClient(timeout=25) as c:
            r = await c.get(url)

        remaining = r.headers.get("x-requests-remaining", "?")
        log.info(f"Odds API {r.status_code} | remaining={remaining} | force={force}")

        if r.status_code != 200:
            log.error(f"Odds error: {r.text[:200]}")
            return {"error": f"API {r.status_code}"}

        games   = r.json()
        ts      = utc_now()
        today   = et_date_str(ts)
        coll    = get_db()["snapshots"]
        stored  = 0
        skipped = 0

        for g in games:
            pin     = next((b for b in g.get("bookmakers", []) if b["key"] == BOOKMAKER), None)
            if not pin:
                continue
            totals  = next((m for m in pin["markets"] if m["key"] == "totals"),  None)
            h2h     = next((m for m in pin["markets"] if m["key"] == "h2h"),     None)
            spreads = next((m for m in pin["markets"] if m["key"] == "spreads"), None)

            over    = next((o for o in (totals  or {}).get("outcomes", []) if o["name"] == "Over"),          None)
            under   = next((o for o in (totals  or {}).get("outcomes", []) if o["name"] == "Under"),         None)
            ml_home = next((o for o in (h2h     or {}).get("outcomes", []) if o["name"] == g["home_team"]), None)
            ml_away = next((o for o in (h2h     or {}).get("outcomes", []) if o["name"] == g["away_team"]), None)
            sp_home = next((o for o in (spreads or {}).get("outcomes", []) if o["name"] == g["home_team"]), None)

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

                # ── 只有數據真的變動才存新快照 ──────────────────────────────
                total_changed   = last.get("total")   != snap["total"]
                ml_changed      = last.get("ml_home") != snap["ml_home"]
                juice_changed   = last.get("over_juice") != snap["over_juice"]
                anything_changed = total_changed or ml_changed or juice_changed

                if not anything_changed:
                    skipped += 1
                    continue  # 跳過，不浪費儲存空間

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

        _last_fetch_ts = ts
        log.info(f"✅ stored={stored} skipped(no change)={skipped} total={len(games)} remaining={remaining}")
        return {"stored": stored, "skipped_no_change": skipped, "total_games": len(games), "remaining": remaining}

    except Exception as e:
        log.error(f"fetch_odds error: {e}")
        return {"error": str(e)}

# ── Fetch Scores & Settle ─────────────────────────────────────────────────────
async def fetch_and_settle():
    if not ODDS_API_KEY:
        return
    try:
        url = f"{ODDS_BASE}/scores/?apiKey={ODDS_API_KEY}&daysFrom=1&dateFormat=iso"
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

        # 清除 2 天前舊記錄
        cutoff = et_date_str(ts - timedelta(days=2))
        await hist_col.delete_many({"date": {"$lt": cutoff}})
        log.info(f"✅ Settled {settled} games | cutoff={cutoff}")

    except Exception as e:
        log.error(f"settle error: {e}")

# ── Smart Scheduler ───────────────────────────────────────────────────────────
async def scheduler():
    """
    智慧排程：
    - ET 08:00–23:00：每 30 分鐘抓取
    - ET 00:00–08:00：睡眠，每 5 分鐘檢查是否到了活躍時段
    """
    while True:
        if is_active_hours():
            await fetch_and_store_odds()
            await fetch_and_settle()
            log.info(f"⏰ 下次抓取：{FETCH_INTERVAL_MINS} 分鐘後")
            await asyncio.sleep(FETCH_INTERVAL_MINS * 60)
        else:
            et = et_now()
            # 計算到下一個活躍時段還有多久
            if et.hour < ACTIVE_START:
                mins_to_active = (ACTIVE_START - et.hour) * 60 - et.minute
            else:
                # 超過 23:00，等到明天 08:00
                mins_to_active = (24 - et.hour + ACTIVE_START) * 60 - et.minute

            log.info(f"😴 睡眠中 ET {et.strftime('%H:%M')}，距離活躍時段 {mins_to_active} 分鐘")
            # 最多等 5 分鐘就重新檢查（避免錯過活躍時段開始）
            await asyncio.sleep(min(5 * 60, mins_to_active * 60))

# ── App Startup ───────────────────────────────────────────────────────────────
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
        log.error("MONGO_URI not set!")

    # 啟動時立刻抓一次（不管時段）
    await fetch_and_store_odds(force=True)
    await fetch_and_settle()

    asyncio.create_task(scheduler())
    log.info(f"⏰ Scheduler: active ET {ACTIVE_START:02d}:00–{ACTIVE_END:02d}:00, interval={FETCH_INTERVAL_MINS}min")
    yield
    if client_db:
        client_db.close()

app = FastAPI(title="MLB Pinnacle Tracker v4.1", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# ── Routes ────────────────────────────────────────────────────────────────────
@app.get("/")
async def root():
    et   = et_now()
    active = is_active_hours()
    return {
        "status":      "ok",
        "version":     "4.1",
        "et_time":     et.strftime("%Y-%m-%d %H:%M ET"),
        "active":      active,
        "schedule":    f"ET {ACTIVE_START:02d}:00–{ACTIVE_END:02d}:00 every {FETCH_INTERVAL_MINS}min",
        "last_fetch":  _last_fetch_ts.isoformat() if _last_fetch_ts else None,
    }

@app.get("/refresh")
async def manual_refresh():
    """手動觸發立即抓取（前端按更新時呼叫，跳過時段限制）"""
    log.info("🖱️ 手動觸發抓取")
    result = await fetch_and_store_odds(force=True)
    await fetch_and_settle()
    return {"triggered": True, **result}

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
            "open":    {"total": first.get("total"), "ml_home": first.get("ml_home"), "ml_away": first.get("ml_away")},
            "latest":  {"total": last.get("total"),  "over_juice": last.get("over_juice"),
                        "under_juice": last.get("under_juice"), "ml_home": last.get("ml_home"),
                        "ml_away": last.get("ml_away"), "spread_home": last.get("spread_home")},
            "delta":   {"total": sig["delta"]},
            "signal":  {"total": sig["total"], "ml": sig["ml"]},
            "history": hist,
            "result":        d.get("result"),
            "actual_total":  d.get("actual_total"),
        })
    return result


@app.get("/history")
async def get_history():
    """
    昨日結算：
    - 移動 ≥ 1.0 顯示（蒸汽）
    - 移動 ≥ 1.5 標記為推薦（大蒸汽，真正建議進場）
    """
    yesterday = et_date_str(utc_now() - timedelta(days=1))
    docs      = await get_db()["history"].find({"date": yesterday}).sort("commence_time", 1).to_list(30)
    result    = []
    for d in docs:
        delta = d.get("total_delta", 0)
        pick  = d.get("pick")
        if abs(delta) >= 1.0 and pick:
            result.append({
                "game_id":       d["game_id"],
                "home":          d["home"],
                "away":          d["away"],
                "commence_time": d["commence_time"],
                "date":          d["date"],
                "open_total":    d.get("open_total"),
                "close_total":   d.get("close_total"),
                "total_delta":   delta,
                "pick":          pick,
                "actual_total":  d.get("actual_total"),
                "result":        d.get("result"),
                "signal":        d.get("signal", {}),
                "recommended":   abs(delta) >= 1.5,  # 移動 ≥ 1.5 才是真正推薦
                "grade":         "⚡ 推薦" if abs(delta) >= 1.5 else "🔥 蒸汽",
            })
    return result


@app.get("/corrections")
async def get_corrections():
    """
    昨日推演結果 → 今日修正提示
    給前端顯示在今日盤口頁面頂部
    邏輯：
    - WIN  → 該方向有效，繼續觀察同類信號
    - LOSS → 提示反向修正
    - 只看有蒸汽信號的場次
    """
    yesterday = et_date_str(utc_now() - timedelta(days=1))
    docs      = await get_db()["history"].find({
        "date":   yesterday,
        "result": {"$in": ["WIN", "LOSS", "PUSH"]},
    }).to_list(30)

    corrections = []
    win_patterns  = []
    loss_patterns = []

    for d in docs:
        delta = abs(d.get("total_delta", 0))
        pick  = d.get("pick")
        if delta < 1.0 or not pick:
            continue   # 只看蒸汽場次

        result       = d.get("result")
        actual       = d.get("actual_total")
        open_total   = d.get("open_total")
        close_total  = d.get("close_total")
        away         = (d.get("away") or "").split(" ")[-1]
        home         = (d.get("home") or "").split(" ")[-1]
        td           = d.get("total_delta", 0)
        direction    = "大分蒸汽" if td > 0 else "小分蒸汽"

        if result == "WIN":
            win_patterns.append(direction)
            corrections.append({
                "type":    "WIN",
                "game":    f"{away} @ {home}",
                "pick":    pick,
                "actual":  actual,
                "delta":   td,
                "message": f"✅ {direction} 推 {pick} → 實際 {actual} 分，方向正確",
                "hint":    f"今日遇到{direction}信號可持續參考",
            })
        elif result == "LOSS":
            loss_patterns.append(direction)
            reverse = "小分" if "大分" in direction else "大分"
            corrections.append({
                "type":    "LOSS",
                "game":    f"{away} @ {home}",
                "pick":    pick,
                "actual":  actual,
                "delta":   td,
                "message": f"❌ {direction} 推 {pick} → 實際 {actual} 分，推算錯誤",
                "hint":    f"今日遇到{direction}需謹慎，考慮觀望或反向",
            })
        elif result == "PUSH":
            corrections.append({
                "type":    "PUSH",
                "game":    f"{away} @ {home}",
                "pick":    pick,
                "actual":  actual,
                "delta":   td,
                "message": f"〜 {direction} 推 {pick} → 實際 {actual} 分，壓線平局",
                "hint":    "壓線場次需注意盤口精確度",
            })

    # 整體趨勢建議
    today_hint = "尚無昨日蒸汽記錄"
    if win_patterns or loss_patterns:
        win_dirs  = set(win_patterns)
        loss_dirs = set(loss_patterns)
        if loss_dirs and not win_dirs:
            today_hint = f"昨日蒸汽信號全部失準，今日建議提高門檻或觀望"
        elif win_dirs and not loss_dirs:
            today_hint = f"昨日蒸汽信號準確，今日同類信號可信度較高"
        else:
            today_hint = f"昨日蒸汽有贏有輸，今日嚴格執行「移動停滯才進場」"

    return {
        "date":        yesterday,
        "corrections": corrections,
        "today_hint":  today_hint,
        "steam_wins":  len(win_patterns),
        "steam_losses":len(loss_patterns),
    }


@app.get("/stats")
async def get_stats():
    # 只統計蒸汽場次（移動 ≥ 1.0）的勝率，才有意義
    docs   = await get_db()["history"].find({"result": {"$in": ["WIN","LOSS","PUSH"]}}).to_list(200)
    # 全部
    all_wins   = sum(1 for d in docs if d.get("result") == "WIN")
    all_losses = sum(1 for d in docs if d.get("result") == "LOSS")
    all_pushes = sum(1 for d in docs if d.get("result") == "PUSH")
    all_total  = all_wins + all_losses
    # 只看蒸汽
    steam_docs  = [d for d in docs if abs(d.get("total_delta", 0)) >= 1.0]
    s_wins      = sum(1 for d in steam_docs if d.get("result") == "WIN")
    s_losses    = sum(1 for d in steam_docs if d.get("result") == "LOSS")
    s_total     = s_wins + s_losses
    return {
        "all_wins":   all_wins, "all_losses": all_losses, "all_pushes": all_pushes,
        "all_total":  all_total,
        "win_rate":   round(all_wins / all_total * 100, 1) if all_total > 0 else 0,
        "steam_wins": s_wins, "steam_losses": s_losses,
        "steam_total":s_total,
        "steam_win_rate": round(s_wins / s_total * 100, 1) if s_total > 0 else 0,
        "roi":        round((all_wins * 0.91 - all_losses) / all_total * 100, 1) if all_total > 0 else 0,
    }
