"""
MLB Pinnacle Tracker v6.0

v6.0（2026-07-12）── 結算基準修正，這是一次「破壞性誠實化」：
- [重大 BUG 修正] 舊版結算拿「開盤價 open_total」對帳，但 pick 用的是「最新盤口」。
  盤口 8.0→11.0 時，系統顯示 OVER 11.0 卻拿 8.0 結算 —— 那個價你根本買不到。
  灌水幅度與 delta 成正比，所以「3.0+ 勝率 85%」「OVER 90%」幾乎可完全由此 bug 解釋。
- [新] picks collection：在真實決策窗口（ET 10:35 / 台灣 22:35）鎖定 pick 與當下盤口
  line_at_pick。結算以 line_at_pick 為準 = 你真正買得到的價。鎖定後永不覆蓋。
- [新] 同時保留 result_vs_open / result_vs_close 三種對照，直接量化灌水幅度。
- [新] settle_version=6 標記。/calibration 只分析 v6 之後的誠實資料，
  舊的灌水資料一律排除（會標明排除幾筆）。
- [修正] 建議引擎：最低樣本 5→20，改用 Wilson 95% 信賴區間對比 -110 損益平衡點
  52.4%，不再看原始勝率就喊「可提高權重」。
- [修正] 拆掉源自灌水數據的加分：OVER +0.5 移除（且 OVER 場次幾乎全是銳錢，
  與銳錢加分重複計算）；3.0+ base 9→7；ML 同步 1.5→1.0。
- [新] key_cross / one_way_ratio 存入 history 並在 /calibration 分桶 ——
  這兩個加分至今從未被驗證過（資料根本沒存）。

⚠ 預期：修正後勝率會大幅下修，很可能落在 50% 附近。追平收盤價本來就極難。
   真實情況也可能是這套系統目前沒有 edge。這是為了看見真相，不是為了看見好數字。

v5.1：影子結算（被 sharp 過濾的信號仍算假設結果供校正）
v5.0：銳錢/公眾錢判斷、180天保留、7/30日滾動勝率、每日模型統計
"""

import os, asyncio, logging, math
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

# 門檻
THRESHOLD_RECOMMEND = 1.5  # ≥1.5 推薦
THRESHOLD_STEAM     = 1.0  # 1.0–1.4 蒸汽
THRESHOLD_WATCH     = 0.5  # 0.5–0.9 觀察
ML_THRESHOLD        = 15   # ML 移動 ≥ 15 算獨贏信號

# ── 決策窗口（v6.0）──────────────────────────────────────────────────────────
# 你的實際下注時間：台灣 22:35 = ET 10:35。pick 與盤口在此刻鎖定。
DECISION_ET_HOUR     = 10
DECISION_ET_MIN      = 35
MIN_MINUTES_TO_GAME  = 20   # 距開賽 <20 分鐘視為來不及下注，不鎖定

# ── 統計嚴謹度（v6.0）────────────────────────────────────────────────────────
SETTLE_VERSION           = 6      # 只有 >= 此版本的結算資料才進 /calibration
MIN_SAMPLE_FOR_CLAIM     = 20     # 低於此樣本數不下任何結論（舊版是 5，太鬆）
BREAKEVEN_WR             = 52.4   # -110 賠率的損益平衡勝率
WILSON_Z                 = 1.96   # 95% 信賴區間

client_db      = None
db             = None
_last_fetch_ts = None

def get_db(): return db

# ── Wilson 信賴區間（v6.0）────────────────────────────────────────────────────
def wilson_interval(wins: int, n: int, z: float = WILSON_Z):
    """
    回傳 (下界, 上界)，單位 %。
    小樣本時原始勝率極不可靠：4 場 2 勝的「50%」，真實區間其實是 15%~85%。
    只有當「下界 > 損益平衡點」才算真的證明有 edge。
    """
    if n <= 0:
        return (None, None)
    p      = wins / n
    denom  = 1 + z*z/n
    center = p + z*z/(2*n)
    margin = z * math.sqrt(p*(1-p)/n + z*z/(4*n*n))
    lo = (center - margin) / denom
    hi = (center + margin) / denom
    return (round(lo*100, 1), round(hi*100, 1))

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

# ── 大小分信號增強（2026-07 新增）─────────────────────────────────────────────
KEY_NUMBERS = [7.0, 7.5, 8.0, 8.5, 9.0, 9.5]

def key_cross_count(open_t, close_t) -> int:
    """關鍵數字穿越數：盤口從開盤到現盤穿越幾個 MLB 高頻結果數字（7–9.5）"""
    if open_t is None or close_t is None or open_t == close_t:
        return 0
    lo, hi = min(open_t, close_t), max(open_t, close_t)
    return sum(1 for k in KEY_NUMBERS if lo < k < hi)

def one_way_ratio(snaps) -> float:
    """
    單向移動比率 = |淨移動| / 總路徑長
    1.0 = 完全單向（資金持續同方向，強）
    <0.6 = 來回震盪（多空拉鋸，不可靠）
    快照不足回傳 1.0（不影響評分）
    """
    totals = [s.get("total") for s in snaps if s.get("total") is not None]
    if len(totals) < 3:
        return 1.0
    net  = abs(totals[-1] - totals[0])
    path = sum(abs(totals[i] - totals[i-1]) for i in range(1, len(totals)))
    if path == 0:
        return 1.0
    return round(net / path, 2)

# ── 開賽前快照過濾 ────────────────────────────────────────────────────────────
def pregame_snaps(doc) -> list:
    """
    只回傳開賽前的快照（修復 Live 盤污染：開賽後總分隨得分暴漲會製造假信號）
    """
    snaps = doc.get("snapshots", [])
    ct_str = doc.get("commence_time")
    if not ct_str:
        return snaps
    try:
        ct = datetime.fromisoformat(ct_str.replace("Z", "+00:00"))
    except Exception:
        return snaps
    out = []
    for s in snaps:
        ts = s.get("ts")
        if isinstance(ts, datetime):
            ts_aware = ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
            if ts_aware < ct:
                out.append(s)
        else:
            out.append(s)  # 無法判斷時保留
    return out

# ── Signal（新門檻）──────────────────────────────────────────────────────────
def signal_from_snaps(snaps: list) -> dict:
    valid = [s for s in snaps if s.get("total") is not None]
    if len(valid) < 2:
        return {"total": "FLAT", "ml": "FLAT", "delta": 0, "ml_delta": 0,
                "juice_delta": 0, "momentum": 0,
                "grade": "NONE", "sharp": False, "snap_count": len(valid)}

    td  = round(valid[-1]["total"] - valid[0]["total"], 1)
    mld = round((valid[-1].get("ml_home") or 0) - (valid[0].get("ml_home") or 0))

    # 賠率變動（over juice delta）
    oj_first = valid[0].get("over_juice") or 0
    oj_last  = valid[-1].get("over_juice") or 0
    juice_d  = round(oj_last - oj_first)

    # 綜合動能分數：total 移動為主，賠率移動為輔
    momentum = round(abs(td) + abs(juice_d) * 0.05, 2)

    a     = abs(td)
    sharp = is_sharp_money(td, mld)

    # 新門檻（優先看 total 移動）
    if a >= THRESHOLD_RECOMMEND:
        ts    = "RECOMMEND_OVER"  if td > 0 else "RECOMMEND_UNDER"
        grade = "RECOMMEND"
    elif a >= THRESHOLD_STEAM:
        ts    = "STEAM_OVER"  if td > 0 else "STEAM_UNDER"
        grade = "STEAM"
    elif a >= THRESHOLD_WATCH:
        ts    = "WATCH_OVER"  if td > 0 else "WATCH_UNDER"
        grade = "WATCH"
    elif abs(juice_d) >= 5:
        # 總分沒動但賠率有明顯變動 = 莊家微調賠率
        ts    = "JUICE_SHIFT"
        grade = "JUICE"
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
        "total":       ts,
        "ml":          ms,
        "delta":       td,
        "ml_delta":    mld,
        "juice_delta": juice_d,
        "momentum":    momentum,
        "grade":       grade,
        "sharp":       sharp,
        "snap_count":  len(valid),
    }

def pick_direction(sig, ignore_sharp_filter: bool = False):
    """
    只回傳方向（OVER / UNDER / None），不綁盤口。
    盤口由呼叫端決定（鎖定時用 line_at_pick，才是真正買得到的價）。
    """
    grade = sig.get("grade", "FLAT")
    sharp = sig.get("sharp", False)
    d     = sig.get("delta", 0)
    if grade not in ("RECOMMEND", "STEAM"):
        return None
    # 蒸汽級(1.0-1.4)公眾錢須 ML 同步才給建議
    # ignore_sharp_filter=True 時跳過（供影子結算計算假設性結果）
    if grade == "STEAM" and not sharp and not ignore_sharp_filter:
        return None
    if d >= THRESHOLD_STEAM:   return "OVER"
    if d <= -THRESHOLD_STEAM:  return "UNDER"
    return None

def ou_result(pick_str, line, runs):
    """
    大小分結算。line 是「對帳用的盤口」——v6.0 起一律傳 line_at_pick（可下注價），
    不再傳 open_total（買不到的陳舊價，是舊版勝率灌水的來源）。
    """
    if not pick_str or line is None or runs is None:
        return None
    if "OVER" in pick_str:
        return "WIN" if runs > line else "LOSS" if runs < line else "PUSH"
    if "UNDER" in pick_str:
        return "WIN" if runs < line else "LOSS" if runs > line else "PUSH"
    return None

def pick_from_signal(sig, game, ignore_sharp_filter=False):
    """即時顯示用（/summary、/history 備援）。盤口取最新快照。"""
    direction = pick_direction(sig, ignore_sharp_filter)
    if not direction:
        return None
    total = None
    if game.get("latest"):
        total = game["latest"].get("total")
    elif game.get("snapshots"):
        snaps = game["snapshots"]
        if snaps:
            total = snaps[-1].get("total")
    return f"{direction} {total}"

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
            # ── 修復（2026-07-06）：跳過已開賽的比賽 ─────────────────
            # 開賽後 API 回傳的是 Live 盤口（總分隨得分暴漲，如 7→17.5）
            # 會製造假信號污染 delta 計算，必須過濾
            try:
                gct = datetime.fromisoformat(g["commence_time"].replace("Z","+00:00"))
                if gct <= ts:
                    continue  # 已開賽，不再記錄快照
            except Exception:
                pass

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

# ── 鎖定 Pick（v6.0 核心）─────────────────────────────────────────────────────
def in_decision_window() -> bool:
    et = et_now()
    return (et.hour > DECISION_ET_HOUR) or (et.hour == DECISION_ET_HOUR and et.minute >= DECISION_ET_MIN)

async def lock_picks(force: bool = False) -> dict:
    """
    在真實決策窗口（ET 10:35 / 台灣 22:35）鎖定 pick + 當下盤口。

    為什麼要鎖：舊版結算拿 open_total（開盤價）對帳，但你看到信號時盤口早就跑掉了。
    盤口 8.0→11.0，系統推 OVER 11.0 卻用 8.0 結算，等於用一個買不到的價在算勝率。
    line_at_pick = 鎖定當下 Pinnacle 的盤口 = 你真正買得到的價。

    鎖定後永不覆蓋（絕不能用後見之明修改當初的判斷）。
    """
    ts = utc_now()
    if not force and not in_decision_window():
        return {"skipped": "before_decision_window", "et_hour": et_now().hour}

    today     = et_date_str(ts)
    snaps_col = get_db()["snapshots"]
    picks_col = get_db()["picks"]
    docs      = await snaps_col.find({"date": today}).to_list(50)

    locked = skipped = 0
    for d in docs:
        # 已鎖定 → 跳過（永不覆蓋）
        if await picks_col.find_one({"game_id": d["game_id"], "date": today}):
            skipped += 1
            continue

        snaps = pregame_snaps(d)
        if len(snaps) < 2:
            continue

        try:
            ct = datetime.fromisoformat(d["commence_time"].replace("Z", "+00:00"))
        except Exception:
            continue
        mins_to_game = int((ct - ts).total_seconds() / 60)
        if mins_to_game < MIN_MINUTES_TO_GAME:
            continue  # 來不及下注，不列入紀錄（否則等於偷跑）

        sig  = signal_from_snaps(snaps)
        last = snaps[-1]
        line = last.get("total")
        if line is None:
            continue

        direction        = pick_direction(sig)                            # 正式 pick（含 sharp 過濾）
        shadow_direction = pick_direction(sig, ignore_sharp_filter=True)  # 影子 pick（無過濾）
        if not direction and not shadow_direction:
            continue  # 無任何方向信號，不需要鎖

        open_total = snaps[0].get("total")
        await picks_col.insert_one({
            "game_id":        d["game_id"],
            "date":           today,
            "home":           d["home"],
            "away":           d["away"],
            "commence_time":  d["commence_time"],
            "locked_at":      ts,
            "minutes_to_game": mins_to_game,

            # ★ 對帳基準：鎖定當下的真實可下注盤口
            "line_at_pick":   line,
            "over_juice_at_pick":  last.get("over_juice"),
            "under_juice_at_pick": last.get("under_juice"),
            "ml_home_at_pick":     last.get("ml_home"),

            "pick":           f"{direction} {line}"        if direction        else None,
            "shadow_pick":    f"{shadow_direction} {line}" if shadow_direction else None,

            # 鎖定當下的信號狀態（校正分桶用）
            "open_total":     open_total,
            "total_delta":    sig.get("delta", 0),
            "ml_delta":       sig.get("ml_delta", 0),
            "grade":          sig.get("grade", "FLAT"),
            "sharp":          sig.get("sharp", False),
            "ml_signal":      sig.get("ml", "FLAT"),
            "snapshot_count": len(snaps),
            # 這兩個加分至今從未被驗證過（舊版根本沒存進 history）→ 存下來讓校正能檢驗
            "key_cross":      key_cross_count(open_total, line),
            "one_way_ratio":  one_way_ratio(snaps),
            # 盤口跑掉多少（= 舊版灌水幅度）
            "line_drift":     round(line - open_total, 1) if open_total is not None else None,
        })
        locked += 1

    if locked:
        log.info(f"🔒 Locked {locked} picks @ {et_now().strftime('%H:%M ET')} (skipped {skipped} already-locked)")
    return {"locked": locked, "already_locked": skipped}

# ── Settle ────────────────────────────────────────────────────────────────────
async def fetch_and_settle():
    if not ODDS_API_KEY: return
    try:
        # daysFrom=2：涵蓋過去 48 小時開賽的比賽
        # 修復：daysFrom=1 會漏掉開賽超過 24h 的下午場（隔天中午就超過24h，永遠結算不到）
        url = f"{ODDS_BASE}/scores/?apiKey={ODDS_API_KEY}&daysFrom=2&dateFormat=iso"
        async with httpx.AsyncClient(timeout=25) as c:
            r = await c.get(url)
        if r.status_code != 200: return
        scores = r.json()
        ts     = utc_now()
        yesterday  = et_date_str(ts - timedelta(days=1))
        snaps_col  = get_db()["snapshots"]
        hist_col   = get_db()["history"]
        picks_col  = get_db()["picks"]
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

            snaps       = pregame_snaps(game_doc)
            sig         = signal_from_snaps(snaps)
            open_total  = (game_doc.get("open") or {}).get("total")
            close_total = (snaps[-1].get("total") if snaps else None)

            # ── 取回當初鎖定的 pick（v6.0 核心）────────────────────────────
            # 有鎖定紀錄 → 用當初鎖定的 pick 與 line_at_pick 結算（誠實對帳）
            # 無鎖定紀錄 → 這場在決策窗口沒有信號（或部署前的舊場次），不產生正式 result
            lock = await picks_col.find_one({"game_id": s["id"]})

            if lock:
                pick          = lock.get("pick")
                shadow_pick   = lock.get("shadow_pick")
                line_at_pick  = lock.get("line_at_pick")
                lock_delta    = lock.get("total_delta", 0)
                lock_sharp    = lock.get("sharp", False)
                lock_grade    = lock.get("grade", "FLAT")
                lock_ml_delta = lock.get("ml_delta", 0)
                lock_kx       = lock.get("key_cross")
                lock_owr      = lock.get("one_way_ratio")
                lock_drift    = lock.get("line_drift")
            else:
                pick = shadow_pick = line_at_pick = None
                lock_delta    = sig.get("delta", 0)
                lock_sharp    = sig.get("sharp", False)
                lock_grade    = sig.get("grade", "FLAT")
                lock_ml_delta = sig.get("ml_delta", 0)
                lock_kx = lock_owr = lock_drift = None

            # ★ 正式結算：一律對帳 line_at_pick（你真正買得到的價）
            result        = ou_result(pick,        line_at_pick, total_runs)
            shadow_result = ou_result(shadow_pick, line_at_pick, total_runs)

            # 對照組（診斷用，不進 /calibration 的主分桶）：
            # result_vs_open  = 舊版 v5 的算法（拿買不到的開盤價對帳）→ 灌水後的假勝率
            # result_vs_close = 對帳收盤價 → 檢查是否只是在追平收盤價
            result_vs_open  = ou_result(pick, open_total,  total_runs)
            result_vs_close = ou_result(pick, close_total, total_runs)

            # ── ML 獨贏結算（2026-07-08 新增：之前獨贏推薦從未被結算）──────
            # 取各隊得分
            home_runs = away_runs = None
            for sc in score_data:
                try:
                    if sc.get("name") == game_doc["home"]: home_runs = int(sc["score"])
                    elif sc.get("name") == game_doc["away"]: away_runs = int(sc["score"])
                except Exception:
                    pass
            ml_sig  = sig.get("ml", "FLAT")
            ml_pick = None      # 依前端慣例：STEAM_HOME=押客隊，STEAM_AWAY=押主隊
            if ml_sig == "STEAM_HOME":   ml_pick = "AWAY"
            elif ml_sig == "STEAM_AWAY": ml_pick = "HOME"
            ml_pick_team = None
            ml_result    = None
            if ml_pick and home_runs is not None and away_runs is not None and home_runs != away_runs:
                winner = "HOME" if home_runs > away_runs else "AWAY"
                ml_result    = "WIN" if winner == ml_pick else "LOSS"
                ml_pick_team = game_doc["away"] if ml_pick == "AWAY" else game_doc["home"]

            settle_date = game_doc.get("date", yesterday)
            entry = {
                "game_id":       s["id"],
                "date":          settle_date,
                "home":          game_doc["home"],
                "away":          game_doc["away"],
                "commence_time": game_doc["commence_time"],

                # ★ v6.0：標記結算版本。/calibration 只信任 >= SETTLE_VERSION 的資料，
                #   舊的（拿開盤價對帳的灌水資料）一律排除，不再污染參數調整。
                "settle_version": SETTLE_VERSION,
                "has_lock":       bool(lock),

                "open_total":    open_total,
                "close_total":   close_total,
                "line_at_pick":  line_at_pick,   # ★ 對帳基準（可下注價）
                "line_drift":    lock_drift,     # 盤口從開盤跑掉多少 = 舊版灌水幅度

                "total_delta":   lock_delta,
                "ml_delta":      lock_ml_delta,
                "signal":        sig,
                "grade":         lock_grade,
                "sharp":         lock_sharp,
                # 這兩個加分至今從未被驗證 → 存下來讓 /calibration 檢驗
                "key_cross":     lock_kx,
                "one_way_ratio": lock_owr,

                "pick":          pick,
                "actual_total":  total_runs,
                "result":        result,          # ★ 對帳 line_at_pick 的誠實結果
                "shadow_pick":   shadow_pick,     # 影子：忽略 sharp 過濾的假設 pick
                "shadow_result": shadow_result,   # 影子：假設性結果（僅供 /calibration）

                # 診斷對照組（量化舊 bug 的灌水幅度）
                "result_vs_open":  result_vs_open,   # 舊版 v5 算法的結果（灌水）
                "result_vs_close": result_vs_close,  # 對帳收盤價的結果

                "ml_pick":       ml_pick_team,   # 獨贏推薦隊伍
                "ml_result":     ml_result,      # 獨贏結果 WIN/LOSS
                "home_runs":     home_runs,
                "away_runs":     away_runs,
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
            # ★ v6.0：只統計對帳 line_at_pick 的誠實結算
            docs   = await hist_col.find(
                {"date": {"$gte": cutoff},
                 "settle_version": {"$gte": SETTLE_VERSION},
                 "result": {"$in": ["WIN","LOSS","PUSH"]}}
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
            # ★ 決策窗口一到就鎖定 pick + 當下盤口（idempotent，已鎖定的不會被覆蓋）
            await lock_picks()
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
        # v6.0：鎖定的 pick（每場只鎖一次，unique 防重複覆蓋）
        await db["picks"].create_index([("game_id",1),("date",1)],      unique=True)
        log.info("✅ MongoDB connected")
    await fetch_and_store_odds(force=True)
    await lock_picks()
    await fetch_and_settle()
    await daily_model_correction()
    asyncio.create_task(scheduler())
    log.info(f"⏰ v6.0 Scheduler: ET {ACTIVE_START:02d}:00–{ACTIVE_END:02d}:00 every {FETCH_INTERVAL_MINS}min")
    log.info(f"🔒 決策窗口 ET {DECISION_ET_HOUR:02d}:{DECISION_ET_MIN:02d}（台灣 22:35）鎖定 pick + line_at_pick")
    yield
    if client_db: client_db.close()

app = FastAPI(title="MLB Pinnacle Tracker v6.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["GET","POST","OPTIONS"], allow_headers=["*"])

# ── Routes ────────────────────────────────────────────────────────────────────
@app.get("/")
async def root():
    et = et_now()
    return {
        "status": "ok", "version": "6.0",
        "et_time": et.strftime("%Y-%m-%d %H:%M ET"),
        "active":  is_active_hours(),
        "schedule": f"ET {ACTIVE_START:02d}:00–{ACTIVE_END:02d}:00 every {FETCH_INTERVAL_MINS}min",
        "last_fetch": _last_fetch_ts.isoformat() if _last_fetch_ts else None,
        "thresholds": {"recommend": THRESHOLD_RECOMMEND, "steam": THRESHOLD_STEAM, "watch": THRESHOLD_WATCH},
        "decision_window": f"ET {DECISION_ET_HOUR:02d}:{DECISION_ET_MIN:02d}（台灣 22:35）鎖定 pick + line_at_pick",
        "in_decision_window": in_decision_window(),
        "settle_basis": "line_at_pick（鎖定當下的真實可下注盤口）",
        "stats_policy": f"Wilson 95% 下界 > {BREAKEVEN_WR}% 且樣本 ≥ {MIN_SAMPLE_FOR_CLAIM} 才算有 edge",
    }

@app.get("/refresh")
async def manual_refresh():
    result = await fetch_and_store_odds(force=True)
    lock   = await lock_picks()
    await fetch_and_settle()
    return {"triggered": True, "lock": lock, **result}

@app.get("/games")
async def get_games():
    today = et_date_str()
    docs  = await get_db()["snapshots"].find({"date": today}).sort("commence_time",1).to_list(50)
    result= []
    for d in docs:
        snaps = pregame_snaps(d)
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
            "delta":   {"total": sig["delta"], "ml": ml_delta,
                        "juice": sig.get("juice_delta", 0),
                        "momentum": sig.get("momentum", 0)},
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
    ts        = utc_now()
    yesterday = et_date_str(ts - timedelta(days=1))
    two_days  = et_date_str(ts - timedelta(days=2))

    # 先從 history collection 撈（已結算）
    hist_docs = await get_db()["history"].find(
        {"date": yesterday}
    ).sort("commence_time", 1).to_list(50)

    result = []
    seen_ids = set()

    for d in hist_docs:
        delta   = d.get("total_delta", 0)
        pick    = d.get("pick")
        ml_pick = d.get("ml_pick")
        a       = abs(delta)
        # ≥1.0有pick 或 有ML獨贏推薦 = 正式結算；0.5–0.9無ML = 微動參考
        if a >= 0.5 or ml_pick:
            has_ou  = a >= THRESHOLD_STEAM and pick
            is_watch = not has_ou and not ml_pick
            seen_ids.add(d["game_id"])
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
                "result":        d.get("result") if has_ou else None,
                "ml_pick":       ml_pick,
                "ml_result":     d.get("ml_result"),
                "home_runs":     d.get("home_runs"),
                "away_runs":     d.get("away_runs"),
                "signal":        d.get("signal", {}),
                "sharp":         d.get("sharp", False),
                "recommended":   a >= THRESHOLD_RECOMMEND,
                "watch_only":    is_watch,
                "grade":         "⚡ 推薦" if a >= THRESHOLD_RECOMMEND else ("🔥 蒸汽" if has_ou else ("🦈 獨贏" if ml_pick else "👁 微動")),
            })

    # 備用：從 snapshots 撈昨天有信號但可能沒進 history 的場次
    snap_docs = await get_db()["snapshots"].find(
        {"date": yesterday}
    ).sort("commence_time", 1).to_list(50)

    for d in snap_docs:
        if d["game_id"] in seen_ids:
            continue  # 已在 history 裡，跳過
        snaps = pregame_snaps(d)
        if len(snaps) < 2:
            continue
        sig   = signal_from_snaps(snaps)
        delta = sig.get("delta", 0)
        pick  = pick_from_signal(sig, d)
        if abs(delta) >= THRESHOLD_STEAM and pick:
            result.append({
                "game_id":       d["game_id"],
                "home":          d["home"],
                "away":          d["away"],
                "commence_time": d["commence_time"],
                "date":          d.get("date", yesterday),
                "open_total":    (snaps[0].get("total") if snaps else None),
                "close_total":   (snaps[-1].get("total") if snaps else None),
                "total_delta":   delta,
                "ml_delta":      sig.get("ml_delta", 0),
                "pick":          pick,
                "actual_total":  d.get("actual_total"),
                "result":        d.get("result"),
                "signal":        sig,
                "sharp":         sig.get("sharp", False),
                "recommended":   abs(delta) >= THRESHOLD_RECOMMEND,
                "grade":         "⚡ 推薦" if abs(delta) >= THRESHOLD_RECOMMEND else "🔥 蒸汽",
                "from_snapshots": True,  # 標記來源
            })

    # 按開賽時間排序
    result.sort(key=lambda x: x["commence_time"])
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
        # ★ v6.0：只統計誠實資料（對帳 line_at_pick）。
        #   舊資料拿開盤價對帳，勝率被灌水，混進來會讓看板繼續顯示假的 75%。
        docs   = await hist_col.find(
            {"date": {"$gte": cutoff},
             "settle_version": {"$gte": SETTLE_VERSION},
             "result": {"$in": ["WIN","LOSS","PUSH"]}}
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
        lo, hi = wilson_interval(wins, total)
        return {
            "wins": wins, "losses": losses, "pushes": pushes, "total": total+pushes,
            "win_rate":    round(wins/total*100,1) if total>0 else 0,
            # 真實勝率的 95% 區間。樣本小的時候原始勝率毫無意義，看這個。
            "ci95":        [lo, hi],
            "proven_edge": bool(lo is not None and total >= MIN_SAMPLE_FOR_CLAIM and lo > BREAKEVEN_WR),
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


@app.get("/summary")
async def get_summary():
    """
    今日推薦總結（v6.0）
    台灣時間 22:35 = ET 10:35 = 決策窗口。此刻 pick + 當下盤口會被鎖定（line_at_pick），
    結算一律對帳 line_at_pick，確保「看到的價 = 記錄的價 = 結算的價」。

    ⚠ 舊版所有勝率宣稱（3.0+ 81%、OVER 89%、銳錢 75%）皆由結算 bug 灌水而來，
      已全數下修。目前權重視為未驗證的先驗，等 ≥20 場誠實樣本再談調整。
    """
    today    = et_date_str()
    ts_now   = utc_now()
    et_time  = et_now()
    taiwan_ready = in_decision_window()

    # 進入決策窗口 → 順手鎖定（idempotent，已鎖的不覆蓋）
    # 保證使用者「看到的那個盤口」就是之後結算對帳的盤口。
    if taiwan_ready:
        await lock_picks()

    docs     = await get_db()["snapshots"].find({"date": today}).sort("commence_time",1).to_list(50)

    recommendations = []
    watch_list      = []
    all_items       = []

    for d in docs:
        snaps = pregame_snaps(d)
        # 快照門檻降低：≥2 筆就進入評估（睡前總結時間快照應已夠）
        if len(snaps) < 2:
            continue

        sig   = signal_from_snaps(snaps)
        td    = sig.get("delta", 0)
        grade = sig.get("grade", "FLAT")
        sharp = sig.get("sharp", False)
        ml_sig= sig.get("ml", "FLAT")

        try:
            ct = datetime.fromisoformat(d["commence_time"].replace("Z","+00:00"))
        except:
            continue
        mins_to_game = int((ct - ts_now).total_seconds() / 60)

        if mins_to_game < 20:
            continue

        # ── 蒸汽強度計算（v6.0 誠實化版）──────────────────────────
        # ⚠ 舊版的 base/bonus 是拿「灌水勝率」調出來的（3.0+ 宣稱 81%、OVER 宣稱 89%），
        #   那些數字幾乎全來自「拿買不到的開盤價對帳」這個 bug。已全部下修。
        #   目前所有權重視為「未經驗證的先驗」，等 /calibration 累積 ≥20 場誠實樣本再談調整。
        a = abs(td)
        base = 0
        if a >= 3.0:      base = 7   # 舊版 9 →（宣稱的 85% 勝率是灌水產物，不再重押）
        elif a >= 2.0:    base = 6   # 舊版 8
        elif a >= 1.5:    base = 5   # 舊版 7
        elif a >= 1.0:    base = 4
        elif a >= 0.5:    base = 3
        else:
            jd = abs(sig.get("juice_delta", 0))
            if jd >= 10: base = 2
            else:        base = 1

        bonus = 0
        # ML 同步（銳錢）：舊版 +1.5（依據灌水的 75% vs 25%）→ 下修為 +1.0
        if ml_sig != "FLAT":              bonus += 1.0
        # ⚠ 已移除：OVER 方向 +0.5
        #   理由 1：「OVER 88.9% 勝率」是灌水產物（OVER 信號多為大幅上移，灌水最嚴重）
        #   理由 2：歷史 10 場 OVER 全部同時是銳錢 → 與 ML 同步加分完全重複計算
        if len(snaps) >= 5:               bonus += 0.5
        if 120 <= mins_to_game <= 480:    bonus += 0.5
        # 舊版 RECOMMEND + ML 再加 1（與上面 ML 加分疊加）→ 下修為 0.5，避免重複計分
        if grade == "RECOMMEND" and ml_sig != "FLAT": bonus += 0.5

        # ── 信號品質（⚠ 從未被驗證：舊版根本沒把這兩個值存進 history）────
        # v6.0 起會存入 history 並在 /calibration 分桶。在拿到 ≥20 場誠實樣本前，
        # 權重先減半，避免用一個沒人檢查過的指標去左右下注決策。
        open_t  = snaps[0].get("total") if snaps else None
        close_t = snaps[-1].get("total") if snaps else None
        kx      = key_cross_count(open_t, close_t)   # 關鍵數字穿越
        owr     = one_way_ratio(snaps)               # 單向移動比率
        if kx >= 2:   bonus += 0.5   # 舊版 +1.0
        elif kx == 1: bonus += 0.25  # 舊版 +0.5
        if abs(td) >= 1.0:
            if owr >= 0.9:   bonus += 0.25  # 舊版 +0.5
            elif owr < 0.6:  bonus -= 0.5   # 舊版 -1.0

        # 停滯判斷
        settled = False
        settled_count = 0
        for i in range(len(snaps)-1, 0, -1):
            if abs((snaps[i].get("total") or 0) - (snaps[i-1].get("total") or 0)) < 0.1:
                settled_count += 1
            else:
                break
        if settled_count >= 2:
            bonus += 0.5
            settled = True

        steam_score = min(round(base + bonus, 1), 10)

        # ── 公眾錢過濾 ─────────────────────────────────────────────
        # ⚠ 舊版註解宣稱「公眾錢勝率僅25%」——那是 4 場樣本 + 灌水結算的產物，
        #   統計上毫無意義（4 場 2 勝的 95% 區間是 15%~85%）。
        #   過濾規則保留（保守方向），但不再對使用者謊報一個假的勝率數字。
        edge_steam = (grade == "STEAM") and ml_sig == "FLAT"
        weak_combo = (td < 0) and ml_sig == "FLAT"
        if edge_steam:
            steam_score = max(steam_score - 2.0, 2.0)

        # 推薦方向
        pick = pick_from_signal(sig, d)

        # 加入警告標記
        warnings = []
        if edge_steam:
            warnings.append("蒸汽級(1.0–1.4)但ML未同步，已降為觀察（樣本不足，尚無法證實此類信號有效）")
        elif weak_combo and pick:
            warnings.append("小分+ML未同步，過往樣本偏弱但數量不足以下定論，建議減注")
        if not settled and steam_score >= 5:
            warnings.append("盤口仍在移動，建議等停滯後再確認")
        if abs(td) >= 1.0 and owr < 0.6:
            warnings.append(f"盤口來回震盪（單向率{owr}），多空拉鋸信號不可靠")
        if mins_to_game > 720:
            warnings.append("距開賽超過12小時，信號可能繼續變化")
        if len(snaps) < 5:
            warnings.append(f"快照僅 {len(snaps)} 筆，建議等累積 5 筆以上")

        item = {
            "game_id":        d["game_id"],
            "home":           d["home"],
            "away":           d["away"],
            "commence_time":  d["commence_time"],
            "minutes_to_game":mins_to_game,
            "snapshot_count": len(snaps),
            "delta":          td,
            "grade":          grade,
            "sharp":          sharp,
            "ml_signal":      ml_sig,
            "steam_score":    steam_score,
            "pick":           pick,
            "settled":        settled,
            "settled_count":  settled_count,
            "key_cross":      kx,
            "one_way_ratio":  owr,
            "edge_steam":     edge_steam,
            "warnings":       warnings,
            "open_total":    (snaps[0].get("total") if snaps else None),
            "close_total":   (snaps[-1].get("total") if snaps else None),
            "over_juice":    (snaps[-1].get("over_juice") if snaps else None),
            "under_juice":   (snaps[-1].get("under_juice") if snaps else None),
            "ml_home":       (snaps[-1].get("ml_home") if snaps else None),
            "ml_away":       (snaps[-1].get("ml_away") if snaps else None),
        }

        if steam_score >= 5 and pick and not edge_steam:
            recommendations.append(item)
        elif steam_score >= 3 or ml_sig != "FLAT" or edge_steam:
            watch_list.append(item)
        all_items.append(item)

    recommendations.sort(key=lambda x: x["steam_score"], reverse=True)
    watch_list.sort(key=lambda x: x["steam_score"], reverse=True)

    # 兩個清單都空時，顯示今日最接近門檻的前3場，讓使用者確認系統運作中
    near_misses = []
    if not recommendations and not watch_list and all_items:
        all_items.sort(key=lambda x: x["steam_score"], reverse=True)
        near_misses = all_items[:3]

    stats_docs = await get_db()["model_stats"].find({"period_days": 30}).to_list(1)
    stats_30d  = stats_docs[0] if stats_docs else {}

    return {
        "generated_at":    ts_now.isoformat(),
        "et_time":         et_now().strftime("%Y-%m-%d %H:%M ET"),
        "taiwan_time":     et_now().astimezone(
                               __import__('datetime').timezone(
                                   __import__('datetime').timedelta(hours=8)
                               )
                           ).strftime("%Y-%m-%d %H:%M 台灣"),
        "taiwan_ready":    taiwan_ready,
        "total_games":     len(docs),
        "recommendations": recommendations[:5],
        "watch_list":      watch_list[:5],
        "near_misses":     near_misses,
        "historical_ref": {
            "period":    "近30天",
            "win_rate":  stats_30d.get("win_rate", 0),
            "rec_wr":    stats_30d.get("rec_wr", 0),
            "sharp_wr":  stats_30d.get("sharp_wr", 0),
            "roi":       stats_30d.get("roi", 0),
            "total":     stats_30d.get("total", 0),
        },
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


@app.get("/calibration")
async def get_calibration():
    """
    邏輯校正分析：把歷史結算按條件分桶，找出哪些條件勝率最高
    用於每週調整篩選邏輯
    """
    ts       = utc_now()
    cutoff   = et_date_str(ts - timedelta(days=30))

    # ── 只信任誠實資料（v6.0）──────────────────────────────────────────────
    # settle_version >= 6 且有 line_at_pick = 對帳「真正買得到的盤口」的結算。
    # v5 以前的資料拿開盤價對帳，勝率被系統性灌水（且灌水量與 delta 成正比），
    # 拿它調參數等於把 bug 固化進模型 → 一律排除。
    q_honest = {
        "date": {"$gte": cutoff},
        "settle_version": {"$gte": SETTLE_VERSION},
        "line_at_pick": {"$ne": None},
        "$or": [{"result":        {"$in": ["WIN", "LOSS"]}},
                {"shadow_result": {"$in": ["WIN", "LOSS"]}}],
    }
    docs = await get_db()["history"].find(q_honest).to_list(500)

    # 被排除的舊灌水資料有多少（讓你知道為什麼樣本突然變小）
    legacy_excluded = await get_db()["history"].count_documents({
        "date": {"$gte": cutoff},
        "result": {"$in": ["WIN", "LOSS"]},
        "$or": [{"settle_version": {"$exists": False}},
                {"settle_version": {"$lt": SETTLE_VERSION}}],
    })

    # 分析時把影子 pick 一併納入（讓被 sharp 過濾掉的信號也能被檢驗）
    def cal_result(d): return d.get("result") or d.get("shadow_result")
    def cal_pick(d):   return d.get("pick")   or d.get("shadow_pick") or ""

    def bucket_stats(items):
        w = sum(1 for d in items if cal_result(d) == "WIN")
        l = sum(1 for d in items if cal_result(d) == "LOSS")
        t = w + l
        lo, hi = wilson_interval(w, t)
        # 只有「95% 信賴下界 > 損益平衡點」才算真的證明有 edge
        proven = (lo is not None and t >= MIN_SAMPLE_FOR_CLAIM and lo > BREAKEVEN_WR)
        refuted = (hi is not None and t >= MIN_SAMPLE_FOR_CLAIM and hi < BREAKEVEN_WR)
        return {
            "wins": w, "losses": l, "total": t,
            "win_rate": round(w / t * 100, 1) if t > 0 else None,
            "roi":      round((w * 0.909 - l) / t * 100, 1) if t > 0 else None,
            "ci95":     [lo, hi],                      # 真實勝率的 95% 區間
            "verdict":  ("有效" if proven else
                         "無效" if refuted else
                         "樣本不足" if t < MIN_SAMPLE_FOR_CLAIM else "無定論"),
        }

    # ── 分桶1：移動幅度 ──
    by_delta = {
        "1.0-1.4 蒸汽":   bucket_stats([d for d in docs if 1.0 <= abs(d.get("total_delta", 0)) < 1.5]),
        "1.5-1.9 推薦":   bucket_stats([d for d in docs if 1.5 <= abs(d.get("total_delta", 0)) < 2.0]),
        "2.0-2.9 強推薦": bucket_stats([d for d in docs if 2.0 <= abs(d.get("total_delta", 0)) < 3.0]),
        "3.0+ 極強":      bucket_stats([d for d in docs if abs(d.get("total_delta", 0)) >= 3.0]),
    }

    # ── 分桶2：銳錢 vs 公眾錢 ──
    by_money = {
        "銳錢(ML同步)":     bucket_stats([d for d in docs if d.get("sharp")]),
        "公眾錢(ML未同步)": bucket_stats([d for d in docs if not d.get("sharp")]),
    }

    # ── 分桶3：大分 vs 小分方向 ──
    by_direction = {
        "OVER 大分":  bucket_stats([d for d in docs if cal_pick(d).startswith("OVER")]),
        "UNDER 小分": bucket_stats([d for d in docs if cal_pick(d).startswith("UNDER")]),
    }

    # ── 分桶4：方向 × 銳錢交叉 ──
    by_cross = {
        "大分+銳錢": bucket_stats([d for d in docs if cal_pick(d).startswith("OVER")  and d.get("sharp")]),
        "大分+公眾": bucket_stats([d for d in docs if cal_pick(d).startswith("OVER")  and not d.get("sharp")]),
        "小分+銳錢": bucket_stats([d for d in docs if cal_pick(d).startswith("UNDER") and d.get("sharp")]),
        "小分+公眾": bucket_stats([d for d in docs if cal_pick(d).startswith("UNDER") and not d.get("sharp")]),
    }

    # ── 分桶5＋6：從未被驗證過的兩個加分（v6.0 首次可檢驗）──────────────
    kx_docs = [d for d in docs if d.get("key_cross") is not None]
    by_key_cross = {
        "穿越0個關鍵數字":  bucket_stats([d for d in kx_docs if d.get("key_cross") == 0]),
        "穿越1個關鍵數字":  bucket_stats([d for d in kx_docs if d.get("key_cross") == 1]),
        "穿越2+個關鍵數字": bucket_stats([d for d in kx_docs if d.get("key_cross", 0) >= 2]),
    }
    owr_docs = [d for d in docs if d.get("one_way_ratio") is not None]
    by_one_way = {
        "單向率 ≥0.9（單向推進）": bucket_stats([d for d in owr_docs if d.get("one_way_ratio", 0) >= 0.9]),
        "單向率 0.6–0.89":         bucket_stats([d for d in owr_docs if 0.6 <= d.get("one_way_ratio", 0) < 0.9]),
        "單向率 <0.6（來回震盪）": bucket_stats([d for d in owr_docs if d.get("one_way_ratio", 1) < 0.6]),
    }

    # ── 灌水診斷：同一批 pick，三種對帳基準的勝率差距 ─────────────────────
    # 這直接量化舊 bug 造成的假優勢：vs_open 是舊版算法，at_pick 是真實可下注價。
    def wr_of(field):
        w = sum(1 for d in docs if d.get(field) == "WIN")
        l = sum(1 for d in docs if d.get(field) == "LOSS")
        t = w + l
        return {"wins": w, "losses": l, "total": t,
                "win_rate": round(w / t * 100, 1) if t > 0 else None}

    drifts = [abs(d["line_drift"]) for d in docs if d.get("line_drift") is not None]
    inflation_check = {
        "說明": "同一批 pick 用三種盤口對帳。vs_open = 舊版 v5 的算法（買不到的價），"
                "at_pick = 你真正買得到的價。兩者差距 = 舊版勝率灌水幅度。",
        "對帳 line_at_pick（真實·現行）": wr_of("result"),
        "對帳 open_total（舊版·灌水）":   wr_of("result_vs_open"),
        "對帳 close_total（收盤價）":     wr_of("result_vs_close"),
        "平均盤口漂移":  round(sum(drifts) / len(drifts), 2) if drifts else None,
    }

    # ── 建議引擎（v6.0：Wilson 下界 vs 損益平衡點，最低 20 場）────────────
    suggestions = []
    all_buckets = {**by_delta, **by_money, **by_direction, **by_cross,
                   **by_key_cross, **by_one_way}
    for name, st in all_buckets.items():
        t, lo, hi = st["total"], st["ci95"][0], st["ci95"][1]
        if t == 0:
            continue
        if t < MIN_SAMPLE_FOR_CLAIM:
            suggestions.append(
                f"⏳「{name}」{t} 場（勝率 {st['win_rate']}%，真實區間 {lo}%–{hi}%）"
                f"— 樣本 <{MIN_SAMPLE_FOR_CLAIM}，區間太寬，不做任何調整"
            )
        elif st["verdict"] == "有效":
            suggestions.append(
                f"✅「{name}」{t} 場，勝率下界 {lo}% > 損益平衡 {BREAKEVEN_WR}% "
                f"— 統計上證實有 edge，可考慮提高權重"
            )
        elif st["verdict"] == "無效":
            suggestions.append(
                f"❌「{name}」{t} 場，勝率上界 {hi}% < 損益平衡 {BREAKEVEN_WR}% "
                f"— 統計上證實無利可圖，應過濾"
            )
        else:
            suggestions.append(
                f"➖「{name}」{t} 場，區間 {lo}%–{hi}% 橫跨損益平衡 {BREAKEVEN_WR}% "
                f"— 無定論，維持現狀"
            )

    warnings = [
        f"⚠ 已排除 {legacy_excluded} 筆 v5 以前的結算資料（拿開盤價對帳，勝率被系統性灌水）。"
        f"樣本會從 v6.0 部署日重新累積。",
        f"⚠ 判定標準：Wilson 95% 信賴下界 > {BREAKEVEN_WR}%（-110 損益平衡點）才算有 edge，"
        f"且至少 {MIN_SAMPLE_FOR_CLAIM} 場。原始勝率高不代表有 edge。",
        "⚠ 30天滾動視窗每週重疊 29 天 —— 每週在幾乎同一批資料上重新調參 = 過擬合複利。"
        "建議每月調一次，或改用不重疊的區間比較。",
        "⚠ 同時檢驗多個分桶會放大偽陽性（跑 18 個桶，即使全無 edge 也預期約 1 個會「顯著」）。"
        "單一分桶亮綠燈不足以行動，要看它是否跨期穩定重現。",
    ]
    if inflation_check["對帳 line_at_pick（真實·現行）"]["total"] == 0:
        warnings.insert(0, "🔎 目前尚無 v6.0 誠實樣本（需等新的比賽在決策窗口鎖定並結算）。"
                           "本報告的分桶全為空是預期行為，不是故障。")

    return {
        "period":            "近30天",
        "sample_size":       len(docs),
        "legacy_excluded":   legacy_excluded,
        "settle_version":    SETTLE_VERSION,
        "breakeven_wr":      BREAKEVEN_WR,
        "min_sample":        MIN_SAMPLE_FOR_CLAIM,
        "inflation_check":   inflation_check,
        "by_delta":          by_delta,
        "by_money":          by_money,
        "by_direction":      by_direction,
        "by_cross":          by_cross,
        "by_key_cross":      by_key_cross,   # ⚠ 此加分過去從未被驗證
        "by_one_way":        by_one_way,     # ⚠ 此加分過去從未被驗證
        "suggestions":       suggestions,
        "warnings":          warnings,
        "generated_at":      ts.isoformat(),
    }


@app.get("/picks")
async def get_picks():
    """
    查看已鎖定的 pick（v6.0）。
    line_at_pick = 鎖定當下的真實可下注盤口 = 結算對帳基準。
    鎖定後永不覆蓋 —— 不能用後見之明修改當初的判斷。
    """
    ts     = utc_now()
    cutoff = et_date_str(ts - timedelta(days=7))
    docs   = await get_db()["picks"].find(
        {"date": {"$gte": cutoff}}
    ).sort("locked_at", -1).to_list(100)
    return [{
        "game_id":       d["game_id"],
        "date":          d["date"],
        "matchup":       f"{d.get('away')} @ {d.get('home')}",
        "locked_at":     d["locked_at"].isoformat() if isinstance(d.get("locked_at"), datetime) else None,
        "minutes_to_game": d.get("minutes_to_game"),
        "pick":          d.get("pick"),
        "shadow_pick":   d.get("shadow_pick"),
        "line_at_pick":  d.get("line_at_pick"),
        "open_total":    d.get("open_total"),
        "line_drift":    d.get("line_drift"),
        "total_delta":   d.get("total_delta"),
        "grade":         d.get("grade"),
        "sharp":         d.get("sharp"),
        "key_cross":     d.get("key_cross"),
        "one_way_ratio": d.get("one_way_ratio"),
    } for d in docs]


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port)
