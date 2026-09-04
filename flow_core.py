"""flow_core.py — scanner internals, importable. Extracted from FLOW_SCANNER
so the scheduled worker and the interactive run share one implementation."""

import csv
import getpass
import os
import math
import statistics
import sys
import time
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone

try:
    import requests
except ImportError:
    sys.exit("Run first:  !pip install requests")

BASE = "https://api.massive.com"

# ---------------------------------------------------------------------------
# SETTINGS
# ---------------------------------------------------------------------------

# Leave empty to scan the most liquid names automatically from bar_cache.csv.
WATCHLIST = []

# Tier 1 is one API call per ticker and runs about 6.5 tickers/second, so 500
# costs ~75s. That matches the universe the pattern backtest was measured on,
# rather than scanning a narrower slice than the strategy itself covers.
MAX_TICKERS      = 500     # Tier 1 scan breadth (1 API call each)
# Tier 2 costs ~13 calls per contract, so each increment is real time. A wider
# Tier 1 mostly improves WHICH contracts reach this stage rather than needing
# more of them.
TOP_N_DEEP       = 30      # contracts sent to Tier 2 tape analysis
MIN_PREMIUM      = 250_000 # ignore clusters below this
MIN_VOL          = 100     # ignore contracts below this day volume
DTE_MIN, DTE_MAX = 3, 180
MONEYNESS        = 0.15    # +/-15% of spot (30% admitted stock-proxy strikes)
MIN_DIRECTIONAL_PRINTS = 3     # prints needed before claiming a side...
SINGLE_PRINT_PREMIUM = 500_000 # ...unless one print is this big
SINGLE_PRINT_CONFIDENCE = 0.60 # ...and classified on a tight, fresh book
TAPE_TOP_PRINTS  = 12      # largest prints classified per contract
MIN_DIRECTIONAL_SHARE = 0.55  # premium at a touch required to claim a side
MAX_QUOTE_AGE_MS = 5_000   # beyond this the quote is not a usable book
MAX_PER_TICKER   = 2       # stop one name flooding the report

# Condition codes (OPRA)
COND_MULTILEG    = {227, 228, 229, 230, 231}
COND_SWEEP       = {233}

IV_RANK_FLIP     = 90      # above this, prior flips toward premium SELLING
STALE_QUOTE_MS   = 500     # observed staleness reaches 2800ms; be strict
HISTORY_FILE     = "flow_history.csv"   # builds ticker premium baselines


try:
    from zoneinfo import ZoneInfo
    _ET = ZoneInfo("America/New_York")
except Exception:                       # fallback if tzdata is unavailable
    _ET = timezone(timedelta(hours=-4))


def et(ns):
    """OPRA sip_timestamp (nanoseconds) -> Eastern wall clock."""
    return datetime.fromtimestamp(ns / 1e9, tz=timezone.utc).astimezone(_ET)



def get_api_key(prompt="Massive API key (hidden): "):
    """Colab Secrets -> environment variable -> prompt.

    In Colab: key icon in the left sidebar, add MASSIVE_API_KEY, and toggle
    "Notebook access" on for this notebook or userdata.get() will refuse.
    """
    try:
        from google.colab import userdata          # type: ignore
        k = (userdata.get("MASSIVE_API_KEY") or "").strip()
        if k:
            print("  key loaded from Colab Secrets", flush=True)
            return k
    except Exception:
        pass                                        # not Colab, or not granted
    k = os.environ.get("MASSIVE_API_KEY", "").strip()
    if k:
        print("  key loaded from environment", flush=True)
        return k
    return getpass.getpass(prompt).strip()


def log(m):
    print(m, flush=True)


_key = None


def get(path, params=None, retries=3):
    p = dict(params or {})
    p["apiKey"] = _key
    url = path if path.startswith("http") else BASE + path
    for a in range(retries):
        try:
            r = requests.get(url, params=p, timeout=30)
        except Exception:
            time.sleep(1.5 ** a)
            continue
        if r.status_code == 200:
            return r.json()
        if r.status_code == 429:
            time.sleep(2 ** a)
            continue
        return None
    return None


# ---------------------------------------------------------------------------
# TIER 1 — whole-chain snapshot
# ---------------------------------------------------------------------------

_SESSION = None


def scan_ticker(ticker):
    """One call returns the full chain. Returns per-contract clusters + GEX."""
    j = get(f"/v3/snapshot/options/{ticker}", {"limit": 250})
    if not j:
        return None
    results = j.get("results") or []
    if not results:
        return None

    spot = None
    for r in results:
        ua = r.get("underlying_asset") or {}
        if ua.get("price"):
            spot = float(ua["price"])
            break
    if not spot:
        return None

    today = datetime.now(timezone.utc).date()
    clusters, ivs, gex_pool, all_oi, siblings = [], [], [], [], []
    gex_call = gex_put = 0.0

    for r in results:
        det = r.get("details") or {}
        day = r.get("day") or {}
        gk = r.get("greeks") or {}
        try:
            strike = float(det["strike_price"])
            expiry = date.fromisoformat(det["expiration_date"])
            ctype = det["contract_type"]
        except (KeyError, TypeError, ValueError):
            continue

        dte = (expiry - today).days
        if not (DTE_MIN <= dte <= DTE_MAX):
            continue
        if abs(strike - spot) / spot > MONEYNESS:
            continue

        # CRITICAL: the snapshot's `day` block persists the LAST SESSION IN
        # WHICH THE CONTRACT TRADED, not today. A dormant strike can report
        # months-old volume and close. Observed live: an HCA put reporting
        # 5,204 contracts and $134.35 from a session 3 months earlier, which
        # the scanner turned into a $70M "unusual activity" alert.
        du = day.get("last_updated")
        if not du:
            continue
        # Must be THIS session. Allowing "within 1 day" let yesterday's volume
        # be reported as today's flow - e.g. a GS call showing $9.1M and 750
        # contracts whose day block was 21.9 hours old, with no tape today.
        if et(du).date() != _SESSION:
            continue

        vol = day.get("volume") or 0
        close = day.get("close") or day.get("vwap") or 0
        oi = r.get("open_interest") or 0
        iv = r.get("implied_volatility")
        gamma = gk.get("gamma") or 0
        delta = gk.get("delta") or 0

        # GAMMA EXPOSURE: dealer convention is short customer calls, long puts.
        # Sign is an ASSUMPTION about who is on which side, not an observation.
        if gamma and oi:
            g = gamma * oi * 100 * spot * spot * 0.01
            if ctype == "call":
                gex_call += g
            else:
                gex_put -= g

        # Kept for the gamma profile even if it has no volume today - GEX is
        # about POSITIONING (open interest), not today's activity.
        if oi and iv:
            gex_pool.append({"strike": strike, "type": ctype, "expiry": expiry,
                             "oi": oi, "iv": iv})
        all_oi.append({"contract": det.get("ticker", ""), "oi": oi})
        # Every traded contract, not just the ones clearing MIN_PREMIUM. A
        # spread's other leg is often cheaper and would never rank into the
        # tape-analysis set on its own.
        if vol:
            siblings.append({"contract": det.get("ticker", ""),
                             "strike": strike, "type": ctype,
                             "expiry": expiry, "volume": vol})
        if vol < MIN_VOL or not close:
            continue
        # Rank on EXTRINSIC value. Total premium favours deep-ITM contracts,
        # which are stock substitutes rather than directional bets - a 420C
        # with spot 560 is $140 of intrinsic and almost no opinion.
        intrinsic = max(0.0, (spot - strike) if ctype == "call" else (strike - spot))
        extrinsic = max(0.0, close - intrinsic)
        premium = vol * close * 100
        extrinsic_prem = vol * extrinsic * 100
        if premium < MIN_PREMIUM:
            continue
        if iv:
            ivs.append(iv)

        clusters.append({
            "ticker": ticker, "contract": det.get("ticker", ""),
            "type": ctype, "strike": strike, "expiry": expiry, "dte": dte,
            "volume": vol, "oi": oi, "premium": premium, "close": close,
            "extrinsic_prem": extrinsic_prem,
            "extrinsic_share": (extrinsic / close) if close else 0.0,
            "iv": iv, "delta": delta, "gamma": gamma, "spot": spot,
            # NEWNESS: capped, never a gate. 10x+ is definitively new; a huge
            # ratio off a tiny OI base carries no extra information.
            "vol_oi": (vol / oi) if oi else None,
            "newness": 1.0 if not oi or oi < 10 else min(vol / oi, 10.0) / 10.0,
            "delta_notional": abs(delta) * vol * 100 * spot,
        })

    if not clusters:
        return None
    total = sum(c["premium"] for c in clusters)
    for c in clusters:
        c["concentration"] = c["premium"] / total

    profile = gamma_profile(gex_pool, spot)
    return {"ticker": ticker, "spot": spot, "clusters": clusters,
            "profile": profile, "all_oi": all_oi, "siblings": siblings,
            "total_premium": total, "median_iv": statistics.median(ivs) if ivs else None,
            "gex": gex_call + gex_put,
            "call_premium": sum(c["premium"] for c in clusters if c["type"] == "call"),
            "put_premium": sum(c["premium"] for c in clusters if c["type"] == "put")}



# ---------------------------------------------------------------------------
# GAMMA PROFILE — per-strike GEX, walls, and the flip level
# ---------------------------------------------------------------------------

def _ncdf(x):
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def bs_gamma(S, K, T, sigma, r=0.04):
    """Gamma at an ARBITRARY spot. The snapshot reports gamma at the CURRENT
    spot only; finding the flip level requires repricing at each candidate
    spot, because gamma itself moves as spot moves."""
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        return 0.0
    d1 = (math.log(S / K) + (r + sigma * sigma / 2) * T) / (sigma * math.sqrt(T))
    return math.exp(-d1 * d1 / 2) / (S * sigma * math.sqrt(2 * math.pi * T)) \
       


def gamma_profile(contracts, spot, span=0.04, steps=41):
    """Net dealer GEX as a function of spot, plus walls and the flip level.

    SIGN CONVENTION: calls positive, puts negative. This ASSUMES dealers are
    short customer calls and long customer puts. Usually right, occasionally
    badly wrong - it is a prior, not an observation.
    """
    live = [c for c in contracts
            if c.get("oi") and c.get("iv") and c.get("expiry")]
    if not live:
        return None

    today = datetime.now(timezone.utc).date()
    lo, hi = spot * (1 - span), spot * (1 + span)
    curve = []
    for i in range(steps):
        S = lo + (hi - lo) * i / (steps - 1)
        tot = 0.0
        for c in live:
            T = max((c["expiry"] - today).days, 0.5) / 365.0
            g = bs_gamma(S, c["strike"], T, c["iv"])
            v = g * c["oi"] * 100 * S * S * 0.01
            tot += v if c["type"] == "call" else -v
        curve.append((S, tot))

    # flip = where the curve crosses zero (linear interp between bracketing pts)
    flip = None
    for (s0, g0), (s1, g1) in zip(curve, curve[1:]):
        if g0 == 0:
            flip = s0; break
        if (g0 < 0) != (g1 < 0):
            flip = s0 + (s1 - s0) * (-g0) / (g1 - g0)
            break

    # walls: strikes with the largest same-sign gamma concentration at spot
    per_strike = defaultdict(float)
    for c in live:
        T = max((c["expiry"] - today).days, 0.5) / 365.0
        v = bs_gamma(spot, c["strike"], T, c["iv"]) * c["oi"] * 100 * spot * spot * 0.01
        per_strike[c["strike"]] += v if c["type"] == "call" else -v

    calls = {k: v for k, v in per_strike.items() if v > 0}
    puts = {k: v for k, v in per_strike.items() if v < 0}
    net_now = next((g for s, g in curve if s >= spot), curve[-1][1])

    return {
        "net_gex": net_now,
        "flip": flip,
        "call_wall": max(calls, key=calls.get) if calls else None,
        "call_wall_gex": max(calls.values()) if calls else 0.0,
        "put_wall": min(puts, key=puts.get) if puts else None,
        "put_wall_gex": min(puts.values()) if puts else 0.0,
        "curve": curve,
    }


# ---------------------------------------------------------------------------
# TIER 2 — tape classification on flagged contracts only
# ---------------------------------------------------------------------------

def last_session(d=None):
    """Most recent weekday. Using UTC 'today' breaks after 20:00 ET, when the
    UTC date rolls forward and the tape query lands on a day with no trades."""
    d = d or datetime.now(timezone.utc).date()
    if datetime.now(timezone.utc).hour < 13:      # before US open
        d -= timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def classify_contract(occ, day):
    """Classify the LARGEST prints, not every print.

    Pulling a whole session of quotes for a liquid contract means hundreds of
    thousands of rows - the request times out and you get nothing. Instead:
    fetch trades (far fewer), rank by premium, then ask for exactly the ONE
    quote prevailing before each big print. Accurate where it matters, and
    ~13 calls per contract instead of one impossible one.
    """
    d0 = day.isoformat()
    d1 = (day + timedelta(days=1)).isoformat()
    tj = get(f"/v3/trades/{occ}", {"timestamp.gte": d0, "timestamp.lt": d1,
                                   "limit": 50000, "sort": "timestamp"})
    trades = (tj or {}).get("results") or []
    if not trades:
        return None

    legs = sweeps = 0
    scored = []
    for t in trades:
        ts, px, sz = t.get("sip_timestamp"), t.get("price"), t.get("size", 0)
        if not ts or not px or not sz:
            continue
        conds = set(t.get("conditions") or [])
        if conds & COND_MULTILEG:
            legs += 1                      # spread leg: not a directional bet
            continue
        if conds & COND_SWEEP:
            sweeps += 1
        scored.append((px * sz * 100, ts, px, sz))

    if not scored:
        return None
    scored.sort(reverse=True)
    top = scored[:TAPE_TOP_PRINTS]

    ask_prem = bid_prem = mid_prem = 0.0
    conf_num = conf_den = 0.0
    stale = unusable = 0
    spreads = []
    prints = []          # timestamped record of every classified print

    for prem, ts, px, sz in top:
        # timestamp.lte must be a STRING - an int returns INVALID_REQUEST.
        qj = get(f"/v3/quotes/{occ}", {"timestamp.lte": str(ts), "order": "desc",
                                       "sort": "timestamp", "limit": 1})
        qr = (qj or {}).get("results") or []
        if not qr:
            continue
        q = qr[0]
        bid, ask = q.get("bid_price"), q.get("ask_price")
        qts = q.get("sip_timestamp", ts)
        if not bid or not ask or ask <= bid:
            continue

        age_ms = (ts - qts) / 1e6
        if age_ms > MAX_QUOTE_AGE_MS:
            # Observed up to 130 SECONDS on illiquid strikes. Classifying a
            # print against a book that old is guessing, not measuring.
            unusable += 1
            continue
        mid = (bid + ask) / 2
        spread_pct = (ask - bid) / mid if mid else 1.0
        spreads.append(spread_pct)

        place = (px - bid) / (ask - bid)
        conf = 1.0 if spread_pct <= 0.02 else max(0.10, 1.0 - 9 * (spread_pct - 0.02))
        if age_ms > STALE_QUOTE_MS:
            conf *= 0.45
            stale += 1

        if place >= 0.85:
            ask_prem += prem
        elif place <= 0.15:
            bid_prem += prem
        else:
            mid_prem += prem
        conf_num += conf * prem
        conf_den += prem
        prints.append({"ts": ts, "price": px, "size": sz, "prem": prem,
                       "bid": bid, "ask": ask, "place": place,
                       "side": ("BUY" if place >= 0.85 else
                                "SELL" if place <= 0.15 else "MID"),
                       "age_ms": age_ms})

    tot = ask_prem + bid_prem + mid_prem
    if tot <= 0:
        return None
    directional = ask_prem + bid_prem
    return {
        "ask_prem": ask_prem, "bid_prem": bid_prem, "mid_prem": mid_prem,
        # ask_share is computed over DIRECTIONAL premium only. Mid prints are
        # negotiated and carry no side information - folding them into the
        # denominator made every block look like aggressive selling.
        "ask_share": (ask_prem / directional) if directional > 0 else None,
        "mid_share": mid_prem / tot,
        "directional_share": directional / tot,
        "confidence": conf_num / conf_den if conf_den else 0.0,
        "sweeps": sweeps, "legs": legs, "stale": stale,
        "unusable": unusable,
        "n_directional": sum(1 for p in prints if p["side"] in ("BUY", "SELL")),
        "median_spread_pct": statistics.median(spreads) if spreads else None,
        "n_trades": len(trades), "n_classified": len(spreads),
        "classified_prem": tot,
        "prints": sorted(prints, key=lambda x: -x["prem"]),
        "first_ts": min((p["ts"] for p in prints), default=None),
        "last_ts": max((p["ts"] for p in prints), default=None),
        "vwap": (sum(p["price"] * p["size"] for p in prints)
                 / sum(p["size"] for p in prints)) if prints else None,
    }


# ---------------------------------------------------------------------------
# History (self-calibrating per-ticker baselines)
# ---------------------------------------------------------------------------

def load_history():
    h = defaultdict(list)
    try:
        for r in csv.DictReader(open(HISTORY_FILE)):
            h[r["ticker"]].append(float(r["total_premium"]))
    except FileNotFoundError:
        pass
    return h


def append_history(rows):
    exists = False
    try:
        open(HISTORY_FILE).close()
        exists = True
    except FileNotFoundError:
        pass
    with open(HISTORY_FILE, "a", newline="") as fh:
        w = csv.writer(fh)
        if not exists:
            w.writerow(["date", "ticker", "total_premium", "gex", "median_iv"])
        for r in rows:
            w.writerow([date.today().isoformat(), r["ticker"],
                        round(r["total_premium"]), round(r["gex"]),
                        r["median_iv"]])


def pct_rank(hist, v):
    if len(hist) < 10:
        return None
    return 100.0 * sum(1 for x in hist if x < v) / len(hist)



# ---------------------------------------------------------------------------
# OI STICK CHECK — did yesterday's flow actually become a position?
# ---------------------------------------------------------------------------
# Snapshot open interest reflects the PRIOR close, so:
#     OI(today's snapshot) - OI(yesterday's snapshot) = net position change
#     during yesterday's session.
# Compare that to yesterday's VOLUME:
#     ~100%  -> nearly all of it opened and is still on. Real positioning.
#     ~0%    -> opened and closed the same day, or offsetting. Day trading.
#     negative -> net closing; someone was getting OUT, not in.
# OI change is NET, so 1,000 opened against 800 closed shows as 20%.
# This costs no extra API calls: the chains are already fetched.

CONTRACT_LOG = "flow_contracts.csv"
CONTRACT_FIELDS = ["date", "ticker", "contract", "strike", "type", "expiry",
                   "volume", "open_interest", "premium", "verdict"]


def log_contracts(rows):
    """Replace this session's rows rather than appending.

    Running the scanner several times in one day would otherwise stack
    duplicate entries for the same date+contract, and tomorrow's stick check
    would count the same position repeatedly.
    """
    keep = []
    try:
        today = rows[0]["date"] if rows else None
        keep = [r for r in csv.DictReader(open(CONTRACT_LOG))
                if r.get("date") != today]
    except FileNotFoundError:
        pass
    with open(CONTRACT_LOG, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=CONTRACT_FIELDS)
        w.writeheader()
        w.writerows(keep)
        w.writerows(rows)


def load_prior_contracts(before_session):
    """Most recent logged session strictly before the current one."""
    try:
        rows = list(csv.DictReader(open(CONTRACT_LOG)))
    except FileNotFoundError:
        return None, []
    dates = sorted({r["date"] for r in rows if r["date"] < before_session.isoformat()})
    if not dates:
        return None, []
    prev = dates[-1]
    return prev, [r for r in rows if r["date"] == prev]


def stick_check(prior_rows, snaps):
    """Match yesterday's logged contracts against today's chain OI."""
    oi_now = {}
    for s in snaps:
        for c in s.get("all_oi", []):
            oi_now[c["contract"]] = c["oi"]

    out = []
    for r in prior_rows:
        now = oi_now.get(r["contract"])
        if now is None:
            continue
        try:
            was, vol = int(r["open_interest"]), int(float(r["volume"]))
        except (ValueError, TypeError):
            continue
        if vol <= 0:
            continue
        delta = now - was
        out.append({**r, "oi_then": was, "oi_now": now, "oi_delta": delta,
                    "stick": delta / vol, "vol": vol})
    out.sort(key=lambda x: -abs(x["stick"]) * float(x.get("premium") or 0))
    return out


def render_stick(x):
    st = x["stick"]
    if st >= 0.70:
        verdict = ("STUCK — nearly all of it opened and is still on. "
                   "Real positioning.")
    elif st >= 0.30:
        verdict = ("PARTLY STUCK — some opened and held, some closed out "
                   "the same day.")
    elif st > -0.10:
        verdict = ("DID NOT STICK — opened and closed the same session, or "
                   "offsetting. Day trading, not positioning.")
    else:
        verdict = ("NET CLOSING — open interest FELL. Someone was getting "
                   "OUT, not in.")
    prem = float(x.get("premium") or 0)
    return "\n".join([
        f"  {x['ticker']} {float(x['strike']):g}{x['type'][0].upper()} "
        f"exp {x['expiry']}  ·  flagged {x['verdict']}  ·  ${prem/1e6:.1f}M",
        f"    that session: {x['vol']:,} traded against {x['oi_then']:,} open",
        f"    now: open interest {x['oi_now']:,}  ({x['oi_delta']:+,})",
        f"    {st:.0%} of the volume is still open  ->  {verdict}",
    ])


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

def _dte_words(dte, expiry):
    if dte <= 1:
        return "expiring tomorrow"
    if dte <= 7:
        return f"expiring {expiry:%a} ({dte}d)"
    if dte <= 45:
        return f"expiring {expiry:%b %d} ({dte}d)"
    return f"expiring {expiry:%b %d} ({dte}d out)"


def _newness_words(c):
    if c["vol_oi"] is None:
        return "This strike had no open interest — it's a brand new position."
    r = c["vol_oi"]
    if c["oi"] < 100:
        return (f"Only {c['oi']} contracts were open here, so the {r:.0f}x "
                f"volume ratio is arithmetic more than signal.")
    if r >= 5:
        return (f"Volume is {r:.1f}x the {c['oi']:,} contracts already open — "
                f"clearly new positioning.")
    if r >= 1.5:
        return (f"Volume is {r:.1f}x the {c['oi']:,} open — likely new, though "
                f"some could be closing.")
    return (f"Volume is only {r:.1f}x the {c['oi']:,} already open — this may "
            f"be traders closing out, not opening.")


def classify_tier(c, tape):
    """Bucket by how much the tape actually tells us."""
    if not tape:
        return "unknown", "?"
    a = tape.get("ask_share")
    # A side is only claimed when most of the premium actually traded at a
    # touch. With 60%+ crossing at mid, the honest answer is "no direction".
    if a is None or tape.get("directional_share", 1.0) < MIN_DIRECTIONAL_SHARE:
        return "negotiated", "="
    # A single print can absolutely be the signal - a large sweep lifting the
    # offer on a tight book is stronger evidence than twenty small prints.
    # What disqualifies is WEAK EVIDENCE, not few prints: GEV claimed "100%
    # buyers" from one 15-lot ($18k) on a 20%-wide book at 0.10 confidence.
    # So: enough prints, OR one print big enough and clean enough to trust.
    n_dir = tape.get("n_directional", 99)
    if n_dir < MIN_DIRECTIONAL_PRINTS:
        big = (tape.get("ask_prem", 0) + tape.get("bid_prem", 0)) \
            >= SINGLE_PRINT_PREMIUM
        clean = tape.get("confidence", 0) >= SINGLE_PRINT_CONFIDENCE
        if not (big and clean):
            return "mixed", "~"
    if a >= 0.65:
        return ("bullish" if c["type"] == "call" else "bearish",
                "^" if c["type"] == "call" else "v")
    if a <= 0.35:
        return ("bearish" if c["type"] == "call" else "bullish",
                "v" if c["type"] == "call" else "^")
    return "mixed", "~"


def render(c, tk, tape, iv_rank, prem_rank):
    tier, mark = classify_tier(c, tape)
    kind = "calls" if c["type"] == "call" else "puts"
    otm = (c["strike"] - c["spot"]) / c["spot"]
    if c["type"] == "put":
        otm = -otm
    where = ("at the money" if abs(otm) < 0.01
             else f"{abs(otm):.0%} {'out of' if otm > 0 else 'in'} the money")

    # --- headline: one plain sentence -------------------------------------
    if tier in ("bullish", "bearish") and tape:
        verb = "bought" if tape["ask_share"] >= 0.65 else "sold"
        head = (f"{mark} {c['ticker']}  ${c['premium']/1e6:.1f}M of "
                f"{c['strike']:g} {kind} {verb}")
    elif tier == "negotiated":
        head = (f"{mark} {c['ticker']}  ${c['premium']/1e6:.1f}M in "
                f"{c['strike']:g} {kind} — negotiated")
    else:
        head = (f"{mark} {c['ticker']}  ${c['premium']/1e6:.1f}M in "
                f"{c['strike']:g} {kind}")

    # --- ticket-style header ----------------------------------------------
    if tier == "negotiated":
        action = "NEGOTIATED"
    elif tier == "unknown":
        action = "NO TAPE"
    elif tier == "mixed":
        a0 = tape.get("ask_share") if tape else None
        action = "NOT CALLED" if (a0 is not None and not (0.35 < a0 < 0.65)) \
            else "TWO-WAY"
    else:
        action = "BUY" if tape["ask_share"] >= 0.65 else "SELL"
    entry = tape.get("vwap") if tape and tape.get("vwap") else c["close"]
    bar = "\u2501" * 68
    L = [bar,
         f"Stock: ${c['ticker']} | Strike: {c['strike']:g}{c['type'][0].upper()} "
         f"| Expiry: {c['expiry']:%m/%d/%Y} | Entry: ${entry:,.2f} "
         f"| Action: {action}",
         bar,
         head,
         f"    {_dte_words(c['dte'], c['expiry'])} · stock "
         f"{c['spot']:,.2f} · strike {where}"]

    # --- when it happened --------------------------------------------------
    if tape and tape.get("first_ts"):
        t0, t1 = et(tape["first_ts"]), et(tape["last_ts"])
        span = (tape["last_ts"] - tape["first_ts"]) / 6e10   # minutes
        if span < 1:
            L.append(f"    WHEN: {t0:%H:%M:%S} ET — all inside "
                     f"{span*60:.0f} seconds")
        else:
            L.append(f"    WHEN: {t0:%H:%M:%S} - {t1:%H:%M:%S} ET "
                     f"({span:.0f} min window)")

    # --- what the tape says ------------------------------------------------
    if tape:
        a = tape.get("ask_share")
        mid = tape.get("mid_share", 0)
        if tier == "negotiated":
            L.append(f"    Every large print crossed near the middle of the "
                     f"spread ({mid:.0%} at mid).")
            L.append("    Real size moved, but the tape does not reveal which "
                     "side was aggressive.")
        elif a is not None:
            # `a` is the BUYERS' share. When the verdict is sellers we must
            # report 1-a, or the card contradicts its own headline.
            tail = f" ({mid:.0%} of premium crossed at mid)" if mid > 0.1 else ""
            if tier == "mixed" and not (0.35 < a < 0.65):
                pass          # handled below as a thin-evidence lean
            elif a >= 0.65:
                L.append(f"    {a:.0%} of the directional premium was buyers "
                         f"lifting the offer.{tail}")
            elif a <= 0.35:
                L.append(f"    {1 - a:.0%} of the directional premium was "
                         f"sellers hitting the bid.{tail}")
            else:
                L.append(f"    Two-way: {a:.0%} bought vs {1 - a:.0%} sold — "
                         f"no clear aggressor.{tail}")

        # A lean outside the two-way band that still lands in `mixed` was
        # downgraded for THIN EVIDENCE, not because flow was balanced. Say so,
        # or the body contradicts the header.
        if a is not None and tier == "mixed" and not (0.35 < a < 0.65):
            lean = "buyers" if a >= 0.65 else "sellers"
            dp = tape.get("ask_prem", 0) + tape.get("bid_prem", 0)
            L.append(f"    Leans {lean} ({max(a, 1 - a):.0%} of directional "
                     f"premium), but NOT CALLED:")
            L.append(f"    only {tape.get('n_directional', 0)} classifiable "
                     f"print(s) worth ${dp/1e6:.2f}M at "
                     f"{tape['confidence']:.2f} confidence.")
        if tape.get("n_directional", 9) < MIN_DIRECTIONAL_PRINTS and \
                tier in ("bullish", "bearish"):
            L.append(f"    Note: this call rests on {tape['n_directional']} "
                     f"large print(s), but on a tight book")
            L.append(f"    with a fresh quote (confidence "
                     f"{tape['confidence']:.2f}).")

        det = []
        if tape["sweeps"]:
            det.append(f"{tape['sweeps']} sweeps (order split across exchanges "
                       f"to fill fast)")
        if tape["legs"]:
            det.append(f"{tape['legs']} spread legs excluded")
        if det:
            L.append("    " + " · ".join(det))
        # When spread legs dominate, any directional read comes from a small
        # minority of activity. AAPL 325C: 3,958 legs excluded, verdict SELL
        # computed on what was left. That deserves a caveat, not silence.
        legshare = tape["legs"] / max(tape["n_trades"], 1)
        if legshare > 0.60:
            L.append(f"    NOTE: {legshare:.0%} of prints were spread legs. Any "
                     f"direction here reflects")
            L.append("    the small non-spread remainder, not the bulk of the "
                     "activity.")
        if tape.get("unusable"):
            L.append(f"    {tape['unusable']} prints discarded — no usable quote "
                     f"within {MAX_QUOTE_AGE_MS/1000:.0f}s.")
        if tape["confidence"] < 0.35:
            L.append(f"    LOW CONFIDENCE ({tape['confidence']:.2f}) — wide "
                     f"spreads or stale quotes make the side unreliable.")
    else:
        L.append("    No tape available for this contract today.")

    if tape and tape.get("prints"):
        L.append("    Largest prints:")
        for pr in tape["prints"][:5]:
            flag = ""
            if pr["age_ms"] > STALE_QUOTE_MS:
                flag = f"  [quote {pr['age_ms']/1000:.1f}s stale]"
            L.append(f"      {et(pr['ts']):%H:%M:%S}  {pr['size']:>5,} @ "
                     f"${pr['price']:>8,.2f}   book {pr['bid']:,.2f}/"
                     f"{pr['ask']:,.2f}  -> {pr['side']}{flag}")

    if tape and tape.get("prints"):
        from collections import Counter as _C
        sec = _C(pr["ts"] // 1_000_000_000 for pr in tape["prints"])
        top_sec, cnt = sec.most_common(1)[0]
        if cnt >= 4:
            L.append(f"    {cnt} of these printed inside the same second — "
                     f"one order worked in slices,")
            L.append("    not several participants.")

    if c.get("_paired"):
        L.append(f"    PAIRED with {', '.join(c['_paired'])} — same sizes at the "
                 f"same timestamps.")
        L.append("    This is one multi-leg order, not independent size. The "
                 "condition codes")
        L.append("    did not flag it; the timing did.")

    L.append("    " + _newness_words(c))
    L.append(f"    This one strike is {c['concentration']:.0%} of the ticker's "
             f"options premium today.")

    if iv_rank is not None and iv_rank >= IV_RANK_FLIP:
        L.append("    CAUTION: implied vol is near the top of this chain. "
                 "Large size at rich")
        L.append("    vol is more often premium being SOLD than bought.")

    # --- dealer positioning ------------------------------------------------
    pf = tk.get("profile")
    if pf:
        g = pf["net_gex"]
        if g > 0:
            L.append(f"    Dealers are LONG gamma (${g/1e6:+.0f}M): they sell "
                     f"strength and buy weakness,")
            L.append("    which tends to pin price and dampen moves.")
        else:
            L.append(f"    Dealers are SHORT gamma (${g/1e6:+.0f}M): they chase "
                     f"the move, which")
            L.append("    tends to accelerate breakouts in either direction.")
        lv = []
        if pf["flip"]:
            side = "above" if c["spot"] > pf["flip"] else "below"
            lv.append(f"flip {pf['flip']:,.2f} (price {side})")
        if pf["call_wall"]:
            lv.append(f"call wall {pf['call_wall']:g}")
        if pf["put_wall"]:
            lv.append(f"put wall {pf['put_wall']:g}")
        if lv:
            L.append("    Levels: " + " · ".join(lv))
    return "\n".join(L)



