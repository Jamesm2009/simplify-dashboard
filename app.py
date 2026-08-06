"""
Simplify ETF Dashboard - Tiingo + Upstash Redis cache
Forked from the KCM ETF Dashboard. Tracks the Simplify Asset Management ETF lineup.
Redis cache survives spindowns and is shared across all browsers/devices.
Daily refresh via /refresh. Serves from cache instantly on repeat visits.
RS Score: (1D x 0.10) + (1W x 0.20) + (1M x 0.30) + (3M x 0.40)
"""

from flask import Flask, render_template, jsonify
import requests
import pandas as pd
import threading
import time
import json, os
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo


app = Flask(__name__)
CT  = ZoneInfo("America/Chicago")

TIINGO_TOKEN  = os.environ.get("TIINGO_TOKEN", "")
TIINGO_BASE   = "https://api.tiingo.com/tiingo/daily"
REDIS_URL     = os.environ.get("UPSTASH_REDIS_REST_URL", "")
REDIS_TOKEN   = os.environ.get("UPSTASH_REDIS_REST_TOKEN", "")
REDIS_KEY_MF  = "simplify_dashboard_cache"
REDIS_KEY_PRG = "simplify_dashboard_progress"

cache = {
    "data": {}, "ranked": [], "last_updated": "Loading...",
    "phase": 0, "progress": "Starting...", "error": None,
}
_lock    = threading.Lock()
_started = False


def load_funds():
    with open("funds.json", "r") as f:
        return json.load(f)


# ── Upstash Redis helpers ─────────────────────────────────────────────────────

def redis_set(key, value, ex_seconds=90000):
    """Store JSON value in Redis."""
    if not REDIS_URL or not REDIS_TOKEN:
        return False
    try:
        payload = json.dumps(value)
        r = requests.post(
            REDIS_URL,
            headers={
                "Authorization": f"Bearer {REDIS_TOKEN}",
                "Content-Type": "application/json",
            },
            json=["SET", key, payload, "EX", ex_seconds],
            timeout=10
        )
        return r.status_code == 200
    except Exception as e:
        print(f"   SET error: {e}")
        return False


def redis_get(key):
    """Retrieve and parse JSON value from Redis."""
    if not REDIS_URL or not REDIS_TOKEN:
        return None
    try:
        r = requests.post(
            REDIS_URL,
            headers={
                "Authorization": f"Bearer {REDIS_TOKEN}",
                "Content-Type": "application/json",
            },
            json=["GET", key],
            timeout=10
        )
        if r.status_code != 200:
            return None
        result = r.json().get("result")
        if result is None:
            return None
        return json.loads(result)
    except Exception as e:
        print(f"  Redis GET error: {e}")
        return None


def redis_del(key):
    if not REDIS_URL or not REDIS_TOKEN:
        return
    try:
        requests.post(
            REDIS_URL,
            headers={
                "Authorization": f"Bearer {REDIS_TOKEN}",
                "Content-Type": "application/json",
            },
            json=["DEL", key],
            timeout=10
        )
    except Exception:
        pass


def save_to_redis():
    """Save full ETF cache to Redis."""
    payload = {
        "data":         cache["data"],
        "last_updated": cache["last_updated"],
        "phase":        cache["phase"],
    }
    ok = redis_set(REDIS_KEY_MF, payload)
    print(f"  Redis save: {'OK' if ok else 'FAILED'} ({len(cache['data'])} funds)")


def load_from_redis():
    """Restore ETF cache from Redis. Returns True if full cache found."""
    print("  Checking Redis for cached data...")
    payload = redis_get(REDIS_KEY_MF)
    if not payload:
        print("  No Redis cache found.")
        return False
    cache["data"]         = payload.get("data", {})
    cache["last_updated"] = payload.get("last_updated", "-")
    cache["phase"]        = payload.get("phase", 0)
    rebuild_ranked()
    n = len(cache["data"])
    print(f"  Redis restored {n} funds (phase={cache['phase']}).")
    return n > 0


def save_progress(completed_symbols):
    """Save list of completed symbols so we can resume after spindown."""
    redis_set(REDIS_KEY_PRG, list(completed_symbols), ex_seconds=90000)


def load_progress():
    """Return set of already-completed symbols."""
    result = redis_get(REDIS_KEY_PRG)
    if isinstance(result, list):
        return set(result)
    return set()


# ── Tiingo ────────────────────────────────────────────────────────────────────

def tiingo_history(symbol, years=3):
    if not TIINGO_TOKEN:
        raise ValueError("TIINGO_TOKEN not set")
    start  = (date.today() - timedelta(days=int(365*years+10))).strftime("%Y-%m-%d")
    url    = f"{TIINGO_BASE}/{symbol}/prices"
    params = {"startDate": start, "token": TIINGO_TOKEN, "resampleFreq": "daily"}
    while True:
        try:
            r = requests.get(url, params=params, timeout=30)
            if r.status_code == 429:
                now       = datetime.now(CT)
                next_hour = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
                wait_secs = int((next_hour - now).total_seconds()) + 120
                resume_at = (now + timedelta(seconds=wait_secs)).strftime("%H:%M")
                msg = f"Rate limit - resuming at {resume_at} CT ({wait_secs//60} min)"
                print(f"    429 {symbol} - {msg}")
                with _lock:
                    cache["progress"] = msg
                    save_to_redis()
                time.sleep(wait_secs)
                continue
            if r.status_code == 404:
                return None
            r.raise_for_status()
            data = r.json()
            if not data:
                return None
            df = pd.DataFrame(data)
            df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None)
            return df.set_index("date").sort_index()
        except requests.exceptions.Timeout:
            print(f"    timeout {symbol} - retrying in 10s")
            time.sleep(10)


# ── Calc helpers ──────────────────────────────────────────────────────────────

def period_return(closes, days):
    if len(closes) < 2:
        return None
    latest = closes.index[-1]
    past   = closes[closes.index <= latest - pd.Timedelta(days=days)]
    if past.empty:
        return None
    return (closes.iloc[-1] - past.iloc[-1]) / past.iloc[-1] * 100


def ytd_return(closes):
    yr = closes[closes.index.year == date.today().year]
    if yr.empty:
        return None
    return (yr.iloc[-1] - yr.iloc[0]) / yr.iloc[0] * 100


def zscore_1yr(closes):
    cutoff = closes.index[-1] - pd.Timedelta(days=365)
    c = closes[closes.index >= cutoff].dropna()
    if len(c) < 20:
        return None
    std = c.std()
    if std == 0:
        return None
    return round((c.iloc[-1] - c.mean()) / std, 2)


def sma_flag(closes, window):
    c = closes.dropna()
    if len(c) < window:
        return "grey"
    sma  = c.tail(window).mean()
    last = c.iloc[-1]
    return "green" if last > sma else ("red" if last < sma else "grey")


def make_sparkline(closes, days=170, w=90, h=28):
    c    = closes.dropna()
    tail = c.tail(days).values
    if len(tail) < 2:
        return ""
    mn, mx = tail.min(), tail.max()
    if mn == mx:
        return ""
    n   = len(tail) - 1
    pts = [
        f"{round(i/n*w,1)},{round((1-(v-mn)/(mx-mn))*(h-2)+1,1)}"
        for i, v in enumerate(tail)
    ]
    sma63 = c.tail(63).mean() if len(c) >= 63 else c.mean()
    col   = "#16a34a" if c.iloc[-1] > sma63 else "#dc2626"
    sma_pts = []
    for i, v in enumerate(tail):
        window = tail[max(0, i-62):i+1]
        sv = window.mean()
        sma_pts.append(
            f"{round(i/n*w,1)},{round((1-(sv-mn)/(mx-mn))*(h-2)+1,1)}"
        )
    return (
        f'<svg width="{w}" height="{h}" viewBox="0 0 {w} {h}" '
        f'xmlns="http://www.w3.org/2000/svg">'
        f'<polyline points="{" ".join(sma_pts)}" fill="none" stroke="#9ca3af" '
        f'stroke-width="1" stroke-dasharray="2,2" '
        f'stroke-linejoin="round" stroke-linecap="round"/>'
        f'<polyline points="{" ".join(pts)}" fill="none" stroke="{col}" '
        f'stroke-width="1.5" stroke-linejoin="round" stroke-linecap="round"/>'
        f'</svg>'
    )


def price_bar_data(closes):
    c = closes.dropna()
    if len(c) < 2:
        return None, None, None, None
    lo, hi = round(c.min(), 2), round(c.max(), 2)
    last   = round(c.iloc[-1], 2)
    pct    = round((last - lo) / (hi - lo) * 100, 1) if hi > lo else 50.0
    return lo, hi, last, pct

def volume_flow(df):
    """
    Compare 5-day avg volume vs 21-day avg volume to determine flow direction.
    Compare current ratio vs ratio from 5 days ago to determine if flow is
    strengthening or weakening.

    Returns (arrow, change):
      arrow:  'up'   = inflow  (5d avg > 21d avg)
              'down' = outflow (5d avg < 21d avg)
      change: 'pos'  = flow strengthening vs last week
              'neg'  = flow weakening vs last week
      Both return None if insufficient data.
    """
    if "volume" not in df.columns:
        return None, None
    vol = df["volume"].dropna()
    if len(vol) < 26:
        return None, None

    # Current: 5-day avg vs 21-day avg
    avg5_now  = vol.iloc[-5:].mean()
    avg21_now = vol.iloc[-21:].mean()
    ratio_now = avg5_now / avg21_now if avg21_now > 0 else 1.0

    # One week ago: 5-day avg (days 6-10) vs 21-day avg (days 6-26)
    avg5_ago  = vol.iloc[-10:-5].mean()
    avg21_ago = vol.iloc[-26:-5].mean()
    ratio_ago = avg5_ago / avg21_ago if avg21_ago > 0 else 1.0

    arrow  = "up"  if ratio_now >= 1.0 else "down"
    change = "pos" if ratio_now >= ratio_ago else "neg"

    return arrow, change

def rebuild_ranked():
    rows     = list(cache["data"].values())
    scored   = sorted(
        [r for r in rows if r.get("rs_score") is not None],
        key=lambda x: x["rs_score"],
        reverse=True
    )
    unscored = [r for r in rows if r.get("rs_score") is None]
    for i, r in enumerate(scored):
        r["rank"] = i + 1
    for r in unscored:
        r["rank"] = None
    cache["ranked"] = scored + unscored


# ── Main update ───────────────────────────────────────────────────────────────

def run_update():
    with _lock:
        cache["phase"] = 1
        cache["error"] = None

    time.sleep(10)

    try:
        if not TIINGO_TOKEN:
            with _lock:
                cache["error"] = "TIINGO_TOKEN not set."
                cache["phase"] = 4
            return

        funds     = load_funds()
        total     = len(funds)
        completed = load_progress()
        remaining = [f for f in funds if f["symbol"] not in completed]

        if completed:
            print(f"  Resuming: {len(completed)} done, {len(remaining)} remaining")
            with _lock:
                cache["progress"] = (
                    f"Resuming - {len(completed)} done, {len(remaining)} to go..."
                )

        for fund in remaining:
            ticker   = fund["symbol"]
            name     = fund.get("name", ticker)
            category = fund.get("category", "equity")
            ftype    = fund.get("type", "")
            ms_url   = f"https://finance.yahoo.com/quote/{ticker.lower()}/profile/"
            ttm      = fund.get("ttm_yield", None)

            done_count = len(completed)
            with _lock:
                cache["progress"] = f"Loading {done_count+1}/{total}: {ticker}"
            print(f"  [{done_count+1}/{total}] {ticker}")

            try:
                df = tiingo_history(ticker, years=3)
                if df is None or df.empty:
                    print(f"    skip")
                    time.sleep(3)
                    continue

                closes = df["adjClose"].dropna()
                if len(closes) < 30:
                    time.sleep(3)
                    continue

                def fmt(v):
                    return round(v, 2) if v is not None else None

                d1  = period_return(closes, 1)
                w1  = period_return(closes, 7)
                m1  = period_return(closes, 30)
                m3  = period_return(closes, 91)
                m6  = period_return(closes, 182)
                ytd = ytd_return(closes)
                y1  = period_return(closes, 365)
                rs  = None
                if all(v is not None for v in [d1, w1, m1, m3]):
                    rs = (d1*0.10) + (w1*0.20) + (m1*0.30) + (m3*0.40)

                zsc   = zscore_1yr(closes)
                ob_os = (
                    "Overbought" if zsc and zsc > 2.10 else
                    "Oversold"   if zsc and zsc < -2.05 else
                    ""
                )
                lo, hi, last_px, bar_pct = price_bar_data(closes)
                vol_arrow, vol_change = volume_flow(df)

                row = {
                    "symbol":        ticker,
                    "name":          name,
                    "type":          ftype,
                    "category":      category,
                    "morningstar_url": ms_url,
                    "exp_ratio":     fund.get("exp_ratio", None),
                    "sparkline":     make_sparkline(closes),
                    "1D":   fmt(d1), "1W": fmt(w1), "1M": fmt(m1),
                    "3M":   fmt(m3), "6M": fmt(m6), "YTD": fmt(ytd), "1Y": fmt(y1),
                    "rs_score":  round(rs, 3) if rs is not None else None,
                    "zscore":    zsc,
                    "ob_os":     ob_os,
                    "trade_flag": sma_flag(closes, 21),
                    "trend_flag": sma_flag(closes, 63),
                    "low3":      lo,
                    "high3":     hi,
                    "last_price": last_px,
                    "bar_pct":   bar_pct,
                    "vol_arrow":  vol_arrow,
                    "vol_change": vol_change,
                    "rank":      None,
                }

                with _lock:
                    cache["data"][ticker] = row
                    rebuild_ranked()
                    cache["last_updated"] = datetime.now(CT).strftime("%-m/%-d/%y %H:%M CT")

                completed.add(ticker)
                save_progress(completed)
                save_to_redis()
                print(f"    OK")

            except Exception as e:
                print(f"    ERR {ticker}: {e}")

            time.sleep(3)

        with _lock:
            cache["phase"]        = 4
            cache["progress"]     = "Complete"
            cache["last_updated"] = datetime.now(CT).strftime("%-m/%-d/%y %H:%M CT")
            save_to_redis()

        redis_del(REDIS_KEY_PRG)
        print(f"Done - {len(cache['data'])} funds loaded.")

    except Exception as e:
        import traceback; traceback.print_exc()
        with _lock:
            cache["error"] = str(e)
            cache["phase"] = 4


def trigger_update():
    threading.Thread(target=run_update, daemon=True).start()


def _ensure_started():
    global _started
    if not _started:
        _started = True
        restored = load_from_redis()
        if restored and cache["phase"] == 4:
            print("  Full cache from Redis - no download needed.")
            with _lock:
                cache["progress"] = "Loaded from cache"
        elif restored and cache["phase"] < 4:
            print("  Partial cache found - resuming download.")
            trigger_update()
        else:
            trigger_update()


# ── Routes ────────────────────────────────────────────────────────────────────

def compute_breadth(funds):
    """
    Count how many ETFs are above / below their 21-day (1M) and 63-day (3M) SMAs.
    Uses trade_flag (21d) and trend_flag (63d) already stored on each fund row.
    Grey = insufficient data, excluded from the totals.
    Returns a dict with sma21 and sma63 sub-dicts.
    """
    def tally(flag_key):
        above = sum(1 for f in funds if f.get(flag_key) == "green")
        below = sum(1 for f in funds if f.get(flag_key) == "red")
        total = above + below          # excludes grey (no data)
        return {
            "above":     above,
            "below":     below,
            "total":     total,
            "above_pct": round(above / total * 100, 1) if total else 0,
            "below_pct": round(below / total * 100, 1) if total else 0,
        }
    return {
        "sma21": tally("trade_flag"),   # 21-day = ~1 month
        "sma63": tally("trend_flag"),   # 63-day = ~3 months
    }


@app.route("/")
def index():
    _ensure_started()
    with _lock:
        snap  = dict(cache)
        funds = list(snap["ranked"])
    is_loading = snap["phase"] < 4 or len(funds) == 0

    # Breadth meters - computed live from cached flags (no extra API calls)
    breadth = compute_breadth(funds)

    return render_template(
        "index.html",
        funds=funds,
        last_updated=snap["last_updated"],
        is_loading=is_loading,
        phase=snap["phase"],
        progress=snap["progress"],
        error=snap["error"],
        breadth=breadth,
    )


@app.route("/refresh")
def refresh():
    """Force a full fresh download - use once daily after market close."""
    redis_del(REDIS_KEY_MF)
    redis_del(REDIS_KEY_PRG)
    with _lock:
        cache["data"]   = {}
        cache["ranked"] = []
        cache["phase"]  = 0
    trigger_update()
    return jsonify({"status": "full refresh started - check /status for progress"})


@app.route("/status")
def status():
    _ensure_started()
    with _lock:
        return jsonify({
            "phase":        cache["phase"],
            "funds":        len(cache["data"]),
            "progress":     cache["progress"],
            "last_updated": cache["last_updated"],
            "error":        cache["error"],
        })


@app.route("/api/data")
def api_data():
    with _lock:
        return jsonify(cache["ranked"])


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
