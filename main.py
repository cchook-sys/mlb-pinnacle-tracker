"""
MLB Pinnacle Tracker v5.0
新增：
- 銳錢 vs 公眾錢判斷（total + ml 同步移動才算銳錢）
- 歷史保留整個 MLB 賽季（180天）
- 7/30 日滾動勝率 + ROI
- 每天 ET 06:00 自動計算模型修正統計
- 新門檻：≥1.5 推薦 · 1.0–1.4 蒸汽 · 0.5–0.9 觀察
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
ODDS_API_KEY        = os.getenv("ODDS_API_KEY", "")
MONGO_URI           = os.getenv("MONGO_URI", "")
BOOKMAKER           = "pinnacle"
SPORT               = "baseball_mlb"
ODDS_BASE           = f"https://api.the-odds-api.com/v4/sports/{SPORT}"
ACTIVE_START        = 8    # ET 08:00 開始
ACTIVE_END          = 23   # ET 23:00 結束
FETCH_INTERVAL_MINS = 30   # 每 30 分鐘抓取
HISTORY_RETAIN_DAYS = 180  # 保留整個 MLB 賽季

# 新門檻
THRESHOLD_RECOMMEND = 1.5  # ≥1.5 推薦
THRESHOLD_STEAM     = 1.0  # 1.0–1.4 蒸汽
THRESHOLD_WATCH     = 0.5  # 0.5–0.9 觀察
ML_THRESHOLD        = 15   # ML 移動 ≥ 15 算獨贏信號

client_db      = None
db             = None
_last_fetch_ts = None

def get_db(): return db

# ── Helpers ───────────────────────────────────────────────────────────────────
def utc_now(): return datetime.now(timezone.utc)
def et_now():  return utc_now().astimezone(timezone(timedelta(hours=-4)))
def et_date_str(dt=None):
    if dt is None: dt = utc_now()
    return dt.astimezone(timezone(timedelta(hours=-4))).strftime("%Y-%m-%d")
def is_active_hours():
    return ACTIVE_START <= et_now().hour < ACTIVE_END

# ── 銳錢判斷 ─────────────────────────────────────────────────────────────────
def is_sharp_money(td: float, mld: float) -> bool:
    """
    銳錢 = total 和 ML 同步移動
    total 往上 + ML 主隊變便宜（mld < 0）= 大分銳錢
    total 往下 + ML 主隊變貴（mld > 0）  = 小分銳錢
    只有 total 動但 ML 不動 = 公眾錢（莊家平衡用）
    """
    if abs(td) < 0.5:
        return False
    # ML 同步移動（同方向或反方向都算介入）
    return abs(mld) >= ML_THRESHOLD

# ── Signal（新門檻）──────────────────────────────────────────────────────────
def signal_from_snaps(snaps: list) -> dict:
    valid = [s for s in snaps if s.get("total") is not None]
    if len(valid) < 2:
        return {"total": "FLAT", "ml": "FLAT", "delta": 0, "ml_delta": 0,
                "grade": "NONE", "sharp": False, "snap_count": len(valid)}

    td  = round(valid[-1]["total"] - valid[0]["total"], 1)
    mld = round((valid[-1].get("ml_home") or 0) - (valid[0].get("ml_home") or 0))
    a   = abs(td)
    sharp = is_sharp_money(td, mld)

    # 新門檻
    if a >= THRESHOLD_RECOMMEND:
        ts    = "RECOMMEND_OVER"  if td > 0 else "RECOMMEND_UNDER"
        grade = "RECOMMEND"
    elif a >= THRESHOLD_STEAM:
        ts    = "STEAM_OVER"  if td > 0 else "STEAM_UNDER"
        grade = "STEAM"
    elif a >= THRESHOLD_WATCH:
        ts    = "WATCH_OVER"  if td > 0 else "WATCH_UNDER"
        grade = "WATCH"
    else:
        ts    = "FLAT"
        grade = "FLAT"

    # 獨贏信號
    if mld <= -ML_THRESHOLD:
        ms = "STEAM_HOME"
    elif mld >= ML_THRESHOLD:
        ms = "STEAM_AWAY"
    else:
        ms = "FLAT"

    return {
        "total":      ts,
        "ml":         ms,
        "delta":      td,
        "ml_delta":   mld,
        "grade":      grade,
        "sharp":      sharp,       # True = 銳錢，False = 公眾錢
        "snap_count": len(valid),
    }

def pick_from_signal(sig, game):
    d     = sig.get("delta", 0)
    grade = sig.get("grade", "FLAT")
    total = (game.get("latest") or {}).get("total")
    if grade not in ("RECOMMEND", "STEAM"):
        return None
    if d >= THRESHOLD_STEAM:   return f"OVER {total}"
    if d <= -THRESHOLD_STEAM:  return f"UNDER {total}"
    return None

# ── Fetch Odds ────────────────────────────────────────────────────────────────
async def fetch_and_store_odds(force: bool = False) -> dict:
    global _last_fetch_ts
    if not ODDS_API_KEY:
        return {"error": "ODDS_API_KEY not set"}
    if not force and not is_active_hours():
        return {"skipped": "sleep_hours", "et_hour": et_now().hour}
    try:
        url = (f"{ODDS_BASE}/odds/?apiKey={ODDS_API_KEY}&regions=us"
               f"&markets=h2h,totals,spreads&bookmakers={BOOKMAKER}&oddsFormat=american")
        async with httpx.AsyncClient(timeout=25) as c:
            r = await c.get(url)
        remaining = r.headers.get("x-requests-remaining", "?")
        log.info(f"Odds API {r.status_code} | remaining={remaining}")
        if r.status_code != 200:
            return {"error": f"API {r.status_code}"}

        games = r.json()
        ts    = utc_now()
        today = et_date_str(ts)
        coll  = get_db()["snapshots"]
        stored = skipped = 0

        for g in games:
            pin     = next((b for b in g.get("bookmakers", []) if b["key"] == BOOKMAKER), None)
            if not pin: continue
            totals  = next((m for m in pin["markets"] if m["key"] == "totals"),  None)
            h2h     = next((m for m in pin["markets"] if m["key"] == "h2h"),     None)
            spreads = next((m for m in pin["markets"] if m["key"] == "spreads"), None)
            over    = next((o for o in (totals  or {}).get("outcomes",[]) if o["name"]=="Over"),         None)
            under   = next((o for o in (totals  or {}).get("outcomes",[]) if o["name"]=="Under"),        None)
            ml_home = next((o for o in (h2h     or {}).get("outcomes",[]) if o["name"]==g["home_team"]),None)
            ml_away = next((o for o in (h2h     or {}).get("outcomes",[]) if o["name"]==g["away_team"]),None)
            sp_home = next((o for o in (spreads or {}).get("outcomes",[]) if o["name"]==g["home_team"]),None)

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
                prev    = existing.get("snapshots", [])
                last    = prev[-1] if prev else {}
                changed = (last.get("total")   != snap["total"] or
                           last.get("ml_home") != snap["ml_home"] or
                           last.get("over_juice") != snap["over_juice"])
                age_min = ((ts - last["ts"].replace(tzinfo=timezone.utc)).total_seconds()/60
                           if isinstance(last.get("ts"), datetime) else 999)
                if not changed and age_min < 30:
                    skipped += 1; continue
                new_snaps = prev[-49:] + [snap]
                sig = signal_from_snaps(new_snaps)
                await coll.update_one({"_id": existing["_id"]}, {"$set": {
                    "snapshots": new_snaps, "latest": snap,
                    "signal": sig, "updated_at": ts,
                }})
                stored += 1
            else:
                sig = signal_from_snaps([snap])
                await coll.insert_one({
                    "game_id": g["id"], "date": today,
                    "home": g["home_team"], "away": g["away_team"],
                    "commence_time": g["commence_time"],
                    "snapshots": [snap], "open": snap, "latest": snap,
                    "signal": sig, "created_at": ts, "updated_at": ts,
                })
                stored += 1

        _last_fetch_ts = ts
        log.info(f"✅ stored={stored} skipped={skipped} remaining={remaining}")
        return {"stored": stored, "skipped": skipped, "remaining": remaining}
    except Exception as e:
        log.error(f"fetch_odds error: {e}")
        return {"error": str(e)}

# ── Settle ────────────────────────────────────────────────────────────────────
async def fetch_and_settle():
    if not ODDS_API_KEY: return
    try:
        url = f"{ODDS_BASE}/scores/?apiKey={ODDS_API_KEY}&daysFrom=1&dateFormat=iso"
        async with httpx.AsyncClient(timeout=25) as c:
            r = await c.get(url)
        if r.status_code != 200: return
        scores = r.json()
        ts     = utc_now()
        yesterday  = et_date_str(ts - timedelta(days=1))
        snaps_col  = get_db()["snapshots"]
        hist_col   = get_db()["history"]
        settled = 0

        for s in scores:
            if not s.get("completed"): continue
            score_data = s.get("scores") or []
            if len(score_data) < 2: continue
            try: total_runs = sum(int(sc["score"]) for sc in score_data)
            except: continue

            # 修正時區對齊：用比賽開賽時間的 ET 日期
            try:
                game_et_date = et_date_str(
                    datetime.fromisoformat(s["commence_time"].replace("Z","+00:00"))
                )
            except:
                game_et_date = yesterday

            game_doc = await snaps_col.find_one({"game_id": s["id"], "date": game_et_date})
            if not game_doc:
                game_doc = await snaps_col.find_one({"game_id": s["id"], "date": yesterday})
            if not game_doc:
                game_doc = await snaps_col.find_one({"game_id": s["id"]})
            if not game_doc: continue

            snaps  = game_doc.get("snapshots", [])
            sig    = signal_from_snaps(snaps)
            pick   = pick_from_signal(sig, game_doc)
            total  = (game_doc.get("open") or {}).get("total")
            result = None
            if pick and total:
                if "OVER"  in pick: result = "WIN" if total_runs > total else "LOSS" if total_runs < total else "PUSH"
                if "UNDER" in pick: result = "WIN" if total_runs < total else "LOSS" if total_runs > total else "PUSH"

            settle_date = game_doc.get("date", yesterday)
            entry = {
                "game_id":       s["id"],
                "date":          settle_date,
                "home":          game_doc["home"],
                "away":          game_doc["away"],
                "commence_time": game_doc["commence_time"],
                "open_total":    total,
                "close_total":   (game_doc.get("latest") or {}).get("total"),
                "total_delta":   sig.get("delta", 0),
                "ml_delta":      sig.get("ml_delta", 0),
                "signal":        sig,
                "grade":         sig.get("grade", "FLAT"),
                "sharp":         sig.get("sharp", False),
                "pick":          pick,
                "actual_total":  total_runs,
                "result":        result,
                "settled_at":    ts,
            }
            await hist_col.update_one(
                {"game_id": s["id"], "date": settle_date},
                {"$set": entry}, upsert=True
            )
            await snaps_col.update_one(
                {"_id": game_doc["_id"]},
                {"$set": {"result": result, "actual_total": total_runs}}
            )
            settled += 1

        # 清理超過保留天數的舊資料
        cutoff = et_date_str(ts - timedelta(days=HISTORY_RETAIN_DAYS))
        del1 = await hist_col.delete_many({"date": {"$lt": cutoff}})
        del2 = await snaps_col.delete_many({"date": {"$lt": cutoff}})
        log.info(f"✅ Settled {settled} | cleaned hist={del1.deleted_count} snaps={del2.deleted_count}")
    except Exception as e:
        log.error(f"settle error: {e}")

# ── 每日模型修正統計（ET 06:00 執行）────────────────────────────────────────
async def daily_model_correction():
    """
    分析最近 7/30 天的預測準確度
    找出失敗模式（ex: 大分蒸汽勝率低於 50%）
    存入 model_stats collection
    """
    try:
        ts       = utc_now()
        hist_col = get_db()["history"]
        stats_col= get_db()["model_stats"]

        for days in [7, 14, 30]:
            cutoff = et_date_str(ts - timedelta(days=days))
            docs   = await hist_col.find(
                {"date": {"$gte": cutoff}, "result": {"$in": ["WIN","LOSS","PUSH"]}}
            ).to_list(500)

            if not docs: continue

            # 整體統計
            wins   = sum(1 for d in docs if d.get("result")=="WIN")
            losses = sum(1 for d in docs if d.get("result")=="LOSS")
            pushes = sum(1 for d in docs if d.get("result")=="PUSH")
            total  = wins + losses

            # 銳錢 vs 公眾錢
            sharp_docs  = [d for d in docs if d.get("sharp")]
            public_docs = [d for d in docs if not d.get("sharp")]
            sw = sum(1 for d in sharp_docs  if d.get("result")=="WIN")
            sl = sum(1 for d in sharp_docs  if d.get("result")=="LOSS")
            pw = sum(1 for d in public_docs if d.get("result")=="WIN")
            pl = sum(1 for d in public_docs if d.get("result")=="LOSS")

            # 推薦 vs 蒸汽
            rec_docs   = [d for d in docs if abs(d.get("total_delta",0)) >= THRESHOLD_RECOMMEND]
            steam_docs = [d for d in docs if THRESHOLD_STEAM <= abs(d.get("total_delta",0)) < THRESHOLD_RECOMMEND]
            rw = sum(1 for d in rec_docs   if d.get("result")=="WIN")
            rl = sum(1 for d in rec_docs   if d.get("result")=="LOSS")
            stw= sum(1 for d in steam_docs if d.get("result")=="WIN")
            stl= sum(1 for d in steam_docs if d.get("result")=="LOSS")

            # ROI（假設賠率 -110，每注 1.0）
            roi = round((wins * 0.909 - losses) / total * 100, 1) if total > 0 else 0

            stat_entry = {
                "date":         et_date_str(ts),
                "period_days":  days,
                "total":        total + pushes,
                "wins":         wins,
                "losses":       losses,
                "pushes":       pushes,
                "win_rate":     round(wins/total*100, 1) if total > 0 else 0,
                "roi":          roi,
                "sharp_wins":   sw, "sharp_losses":  sl,
                "sharp_wr":     round(sw/(sw+sl)*100, 1) if (sw+sl)>0 else 0,
                "public_wins":  pw, "public_losses": pl,
                "public_wr":    round(pw/(pw+pl)*100, 1) if (pw+pl)>0 else 0,
                "rec_wins":     rw, "rec_losses":    rl,
                "rec_wr":       round(rw/(rw+rl)*100, 1) if (rw+rl)>0 else 0,
                "steam_wins":   stw,"steam_losses":  stl,
                "steam_wr":     round(stw/(stw+stl)*100, 1) if (stw+stl)>0 else 0,
                "updated_at":   ts,
            }
            await stats_col.update_one(
                {"period_days": days},
                {"$set": stat_entry},
                upsert=True
            )

        log.info("✅ Model stats updated")
    except Exception as e:
        log.error(f"daily_model_correction error: {e}")

# ── Scheduler ────────────────────────────────────────────────────────────────
async def scheduler():
    last_correction_date = None
    while True:
        if is_active_hours():
            await fetch_and_store_odds()
            await fetch_and_settle()
            # 每天 ET 06:00 之後第一次抓取時執行模型修正
            today = et_date_str()
            if last_correction_date != today and et_now().hour >= 6:
                await daily_model_correction()
                last_correction_date = today
            await asyncio.sleep(FETCH_INTERVAL_MINS * 60)
        else:
            et  = et_now()
            m2a = ((ACTIVE_START - et.hour) * 60 - et.minute) if et.hour < ACTIVE_START \
                  else ((24 - et.hour + ACTIVE_START) * 60 - et.minute)
            log.info(f"😴 ET {et.strftime('%H:%M')} 睡眠中，距活躍 {m2a} 分鐘")
            await asyncio.sleep(min(5*60, m2a*60))

# ── Startup ───────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    global client_db, db
    if MONGO_URI:
        client_db = AsyncIOMotorClient(MONGO_URI)
        db        = client_db["mlb_tracker"]
        await db["snapshots"].create_index([("game_id",1),("date",1)], unique=True)
        await db["history"].create_index([("game_id",1),("date",1)],   unique=True)
        await db["model_stats"].create_index([("period_days",1)],       unique=True)
        log.info("✅ MongoDB connected")
    await fetch_and_store_odds(force=True)
    await fetch_and_settle()
    await daily_model_correction()
    asyncio.create_task(scheduler())
    log.info(f"⏰ v5.0 Scheduler: ET {ACTIVE_START:02d}:00–{ACTIVE_END:02d}:00 every {FETCH_INTERVAL_MINS}min")
    yield
    if client_db: client_db.close()

app = FastAPI(title="MLB Pinnacle Tracker v5.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["GET","POST","OPTIONS"], allow_headers=["*"])

# ── Routes ────────────────────────────────────────────────────────────────────
@app.get("/")
async def root():
    et = et_now()
    return {
        "status": "ok", "version": "5.0",
        "et_time": et.strftime("%Y-%m-%d %H:%M ET"),
        "active":  is_active_hours(),
        "schedule": f"ET {ACTIVE_START:02d}:00–{ACTIVE_END:02d}:00 every {FETCH_INTERVAL_MINS}min",
        "last_fetch": _last_fetch_ts.isoformat() if _last_fetch_ts else None,
        "thresholds": {"recommend": THRESHOLD_RECOMMEND, "steam": THRESHOLD_STEAM, "watch": THRESHOLD_WATCH},
    }

@app.get("/refresh")
async def manual_refresh():
    result = await fetch_and_store_odds(force=True)
    await fetch_and_settle()
    return {"triggered": True, **result}

@app.get("/games")
async def get_games():
    today = et_date_str()
    docs  = await get_db()["snapshots"].find({"date": today}).sort("commence_time",1).to_list(50)
    result= []
    for d in docs:
        snaps = d.get("snapshots", [])
        first = snaps[0]  if snaps else {}
        last  = snaps[-1] if snaps else {}
        sig   = signal_from_snaps(snaps)
        hist  = [{
            "ts":          s["ts"].isoformat() if isinstance(s.get("ts"),datetime) else str(s.get("ts")),
            "total":       s.get("total"),
            "over_juice":  s.get("over_juice"),
            "under_juice": s.get("under_juice"),
            "ml_home":     s.get("ml_home"),
            "ml_away":     s.get("ml_away"),
        } for s in snaps]

        ml_signal = sig.get("ml","FLAT")
        ml_delta  = sig.get("ml_delta",0)
        ml_hint   = None
        if ml_signal == "STEAM_HOME": ml_hint = f"鯊魚押客隊（主隊ML縮水 {ml_delta:+d}）"
        elif ml_signal == "STEAM_AWAY": ml_hint = f"鯊魚押主隊（主隊ML上升 {ml_delta:+d}）"

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
            "delta":   {"total": sig["delta"], "ml": ml_delta},
            "signal":  {"total": sig["total"], "ml": ml_signal, "grade": sig.get("grade","FLAT"),
                        "sharp": sig.get("sharp", False)},
            "ml_hint": ml_hint,
            "history": hist,
            "result":        d.get("result"),
            "actual_total":  d.get("actual_total"),
        })
    return result

@app.get("/history")
async def get_history():
    yesterday = et_date_str(utc_now() - timedelta(days=1))
    docs      = await get_db()["history"].find({"date": yesterday}).sort("commence_time",1).to_list(30)
    result    = []
    for d in docs:
        delta = d.get("total_delta", 0)
        pick  = d.get("pick")
        # 只顯示蒸汽以上（≥1.0）且有推算方向的場次
        if abs(delta) >= THRESHOLD_STEAM and pick:
            result.append({
                "game_id":       d["game_id"],
                "home":          d["home"],
                "away":          d["away"],
                "commence_time": d["commence_time"],
                "date":          d["date"],
                "open_total":    d.get("open_total"),
                "close_total":   d.get("close_total"),
                "total_delta":   delta,
                "ml_delta":      d.get("ml_delta", 0),
                "pick":          pick,
                "actual_total":  d.get("actual_total"),
                "result":        d.get("result"),
                "signal":        d.get("signal", {}),
                "sharp":         d.get("sharp", False),
                "recommended":   abs(delta) >= THRESHOLD_RECOMMEND,
                "grade":         "⚡ 推薦" if abs(delta) >= THRESHOLD_RECOMMEND else "🔥 蒸汽",
            })
    return result

@app.get("/corrections")
async def get_corrections():
    yesterday = et_date_str(utc_now() - timedelta(days=1))
    docs      = await get_db()["history"].find({
        "date": yesterday, "result": {"$in": ["WIN","LOSS","PUSH"]}
    }).to_list(30)

    corrections   = []
    win_patterns  = []
    loss_patterns = []

    for d in docs:
        delta = abs(d.get("total_delta", 0))
        pick  = d.get("pick")
        if delta < THRESHOLD_STEAM or not pick: continue

        result    = d.get("result")
        actual    = d.get("actual_total")
        td        = d.get("total_delta", 0)
        sharp     = d.get("sharp", False)
        direction = "大分蒸汽" if td > 0 else "小分蒸汽"
        away      = (d.get("away") or "").split()[-1]
        home      = (d.get("home") or "").split()[-1]
        money_type= "銳錢💎" if sharp else "公眾錢👥"

        if result == "WIN":
            win_patterns.append(direction)
            corrections.append({
                "type": "WIN", "game": f"{away} @ {home}",
                "pick": pick, "actual": actual, "delta": td, "sharp": sharp,
                "message": f"✅ {direction}（{money_type}）推 {pick} → 實際 {actual} 分，方向正確",
                "hint": f"{'銳錢信號有效，' if sharp else '公眾錢意外準確，'}今日同類信號可參考",
            })
        elif result == "LOSS":
            loss_patterns.append(direction)
            corrections.append({
                "type": "LOSS", "game": f"{away} @ {home}",
                "pick": pick, "actual": actual, "delta": td, "sharp": sharp,
                "message": f"❌ {direction}（{money_type}）推 {pick} → 實際 {actual} 分，失準",
                "hint": f"{'銳錢失準，今日提高門檻' if sharp else '公眾錢誤導，今日優先看ML同步信號'}",
            })
        elif result == "PUSH":
            corrections.append({
                "type": "PUSH", "game": f"{away} @ {home}",
                "pick": pick, "actual": actual, "delta": td, "sharp": sharp,
                "message": f"〜 {direction} 推 {pick} → 實際 {actual} 分，壓線平局",
                "hint": "壓線場次需注意盤口精確度",
            })

    today_hint = "尚無昨日蒸汽記錄"
    if win_patterns or loss_patterns:
        wset, lset = set(win_patterns), set(loss_patterns)
        if lset and not wset:
            today_hint = "昨日蒸汽信號全部失準，今日建議提高門檻或優先看銳錢（ML同步）信號"
        elif wset and not lset:
            today_hint = "昨日蒸汽信號全部準確，今日同類信號可信度高"
        else:
            today_hint = "昨日蒸汽有贏有輸，今日優先選銳錢信號（Total + ML 同步移動）"

    return {
        "date":         yesterday,
        "corrections":  corrections,
        "today_hint":   today_hint,
        "steam_wins":   len(win_patterns),
        "steam_losses": len(loss_patterns),
    }

@app.get("/stats")
async def get_stats():
    """即時統計 + 7/30日滾動勝率"""
    ts       = utc_now()
    hist_col = get_db()["history"]

    async def period_stats(days: int):
        cutoff = et_date_str(ts - timedelta(days=days))
        docs   = await hist_col.find(
            {"date": {"$gte": cutoff}, "result": {"$in": ["WIN","LOSS","PUSH"]}}
        ).to_list(500)
        wins   = sum(1 for d in docs if d.get("result")=="WIN")
        losses = sum(1 for d in docs if d.get("result")=="LOSS")
        pushes = sum(1 for d in docs if d.get("result")=="PUSH")
        total  = wins + losses
        sharp  = [d for d in docs if d.get("sharp")]
        sw     = sum(1 for d in sharp if d.get("result")=="WIN")
        sl     = sum(1 for d in sharp if d.get("result")=="LOSS")
        rec    = [d for d in docs if abs(d.get("total_delta",0)) >= THRESHOLD_RECOMMEND]
        rw     = sum(1 for d in rec if d.get("result")=="WIN")
        rl     = sum(1 for d in rec if d.get("result")=="LOSS")
        return {
            "wins": wins, "losses": losses, "pushes": pushes, "total": total+pushes,
            "win_rate":    round(wins/total*100,1) if total>0 else 0,
            "roi":         round((wins*0.909-losses)/total*100,1) if total>0 else 0,
            "sharp_wins":  sw, "sharp_losses": sl,
            "sharp_wr":    round(sw/(sw+sl)*100,1) if (sw+sl)>0 else 0,
            "rec_wins":    rw, "rec_losses":   rl,
            "rec_wr":      round(rw/(rw+rl)*100,1) if (rw+rl)>0 else 0,
        }

    s7  = await period_stats(7)
    s30 = await period_stats(30)
    sAll= await period_stats(HISTORY_RETAIN_DAYS)

    # 也從 model_stats 取預計算結果（更快）
    stats_docs = await get_db()["model_stats"].find({}).to_list(10)
    cached = {d["period_days"]: d for d in stats_docs}

    return {
        "7d":  s7,
        "30d": s30,
        "all": sAll,
        # 向前端相容舊格式
        "steam_total":    s30.get("total",0),
        "steam_win_rate": s30.get("win_rate",0),
        "win_rate":       sAll.get("win_rate",0),
        "sharp_wr_30d":   s30.get("sharp_wr",0),
        "rec_wr_30d":     s30.get("rec_wr",0),
        "roi_30d":        s30.get("roi",0),
    }

@app.get("/model")
async def get_model():
    """模型準確度看板（預計算版本，速度快）"""
    docs = await get_db()["model_stats"].find({}).sort("period_days",1).to_list(10)
    return [{"period_days": d["period_days"], "win_rate": d.get("win_rate",0),
             "sharp_wr": d.get("sharp_wr",0), "public_wr": d.get("public_wr",0),
             "rec_wr": d.get("rec_wr",0), "steam_wr": d.get("steam_wr",0),
             "roi": d.get("roi",0), "total": d.get("total",0),
             "updated_at": d["updated_at"].isoformat() if isinstance(d.get("updated_at"),datetime) else None,
            } for d in docs]
