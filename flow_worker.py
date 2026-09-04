"""
flow_worker.py — the flow scanner, headless, for scheduled runs.

Does one pass and writes files instead of printing a report:
    flow_latest.json     what the app reads
    flow_contracts.csv   appended per session; drives the next-day stick check

Optionally posts an alert. It only alerts on flow it can actually read —
directional, decent confidence, real size — because the whole point of the
tiers is that most large prints tell you nothing.

    ALERT_WEBHOOK   Discord or Slack incoming webhook (optional)
    MASSIVE_API_KEY required

    python flow_worker.py
"""

import csv
import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import flow_core as fc

# Alert only on flow that cleared the reading bar. A NEGOTIATED block or a
# spread leg is information for the log, not a reason to buzz a phone.
ALERT_MIN_PREMIUM = 2_000_000
ALERT_MIN_CONFIDENCE = 0.55
ALERT_TIERS = {"bullish", "bearish"}

STATE = "flow_alerted.json"      # so a signal is not re-sent every 30 minutes


def watchlist():
    """Names you follow, read from the PRIVATE repo.

    The workflow already clones the private repo for flow_contracts.csv, so
    watchlist.txt rides along. Keeping it there rather than in this public
    repo means the list of what you watch is never exposed.
    """
    for path in ("_results/watchlist.txt", "watchlist.txt"):
        try:
            return [l.strip().upper() for l in open(path)
                    if l.strip() and not l.startswith("#")]
        except FileNotFoundError:
            continue
    return []


def universe(n):
    """Most liquid names from the last session, fetched at runtime.

    Deliberately not read from a cached universe file. This worker lives in a
    public repo so the scans are unmetered, and a committed universe would
    expose what is being watched. "Most traded stocks yesterday" is public.
    """
    import requests
    d = fc.last_session()
    for _ in range(5):                       # walk back over holidays
        try:
            r = requests.get(
                f"{fc.BASE}/v2/aggs/grouped/locale/us/market/stocks/{d}",
                params={"adjusted": "true", "apiKey": fc._key}, timeout=30)
        except Exception:
            r = None
        if r is not None and r.status_code == 200:
            res = r.json().get("results") or []
            if res:
                liq = [(a["T"], a["c"] * a["v"]) for a in res
                       if a.get("T") and a.get("c") and a.get("v")
                       and a["c"] >= 5]
                liq.sort(key=lambda x: -x[1])
                ranked = [t for t, _ in liq]
                wl = watchlist()
                if wl:
                    tradeable = set(ranked)
                    forced = [t for t in wl if t in tradeable]
                    rest = [t for t in ranked if t not in set(forced)]
                    print(f"  watchlist: {len(forced)}/{len(wl)} included",
                          flush=True)
                    return forced + rest[:max(n - len(forced), 0)]
                return ranked[:n]
        d -= timedelta(days=1)
        while d.weekday() >= 5:
            d -= timedelta(days=1)
    return []


def already_sent():
    try:
        return set(json.load(open(STATE)).get(str(fc.last_session()), []))
    except Exception:
        return set()


def remember(keys):
    try:
        d = json.load(open(STATE))
    except Exception:
        d = {}
    s = str(fc.last_session())
    d = {s: sorted(set(d.get(s, [])) | keys)}      # keep only today
    json.dump(d, open(STATE, "w"))


def post(text):
    url = os.environ.get("ALERT_WEBHOOK", "").strip()
    if not url:
        return False
    try:
        import requests
        payload = {"content": text} if "discord" in url else {"text": text}
        r = requests.post(url, json=payload, timeout=15)
        return r.status_code < 300
    except Exception:
        return False


def main():
    fc._key = os.environ.get("MASSIVE_API_KEY", "").strip()
    if not fc._key:
        sys.exit("MASSIVE_API_KEY not set")
    fc._SESSION = fc.last_session()

    tickers = universe(fc.MAX_TICKERS)
    if not tickers:
        sys.exit("could not build a universe from the grouped endpoint")
    print(f"session {fc._SESSION} · scanning {len(tickers)} tickers", flush=True)

    import time as _t
    t0 = _t.time()

    def sweep(lo, hi, top_n, label, universe=None):
        """One pass over the universe for a given DTE band."""
        uni = universe if universe is not None else tickers
        snaps = []
        for i, t in enumerate(uni, 1):
            sn = fc.scan_ticker(t, lo, hi)
            if sn:
                snaps.append(sn)
            if i % 100 == 0:
                print(f"    [{label}] {i}/{len(uni)} · {len(snaps)} active "
                      f"· {_t.time()-t0:.0f}s", flush=True)
        if not snaps:
            return [], []
        allc = []
        for sn in snaps:
            for c in sn["clusters"]:
                c["_snap"] = sn
                c["_tier"] = label
                c["_score"] = (0.40 * min(c["premium"] / 15e6, 1.0)
                               + 0.25 * min(c["concentration"] / 0.5, 1.0)
                               + 0.20 * c["newness"]
                               + 0.15 * min(c["delta_notional"] / 50e6, 1.0))
                allc.append(c)
        allc.sort(key=lambda c: -c["_score"])
        seen, picked = defaultdict(int), []
        for c in allc:
            if seen[c["ticker"]] >= fc.MAX_PER_TICKER:
                continue
            seen[c["ticker"]] += 1
            picked.append(c)
            if len(picked) >= top_n:
                break
        print(f"  [{label}] {len(snaps)} tickers active, "
              f"{len(picked)} contracts selected ({_t.time()-t0:.0f}s)",
              flush=True)
        return picked, snaps

    top, snaps = sweep(fc.DTE_MIN, fc.DTE_MAX, fc.TOP_N_DEEP, "3-365d")
    zero, zsnaps = sweep(fc.ZERO_DTE_MIN, fc.ZERO_DTE_MAX,
                         fc.TOP_N_ZERO_DTE, "0-2d",
                         universe=tickers[:fc.ZERO_DTE_TICKERS])
    if not top and not zero:
        print("no chain activity"); return
    snaps = snaps + zsnaps

    for c in top + zero:
        c["_tape"] = fc.classify_contract(c["contract"], fc._SESSION)
    print(f"  classified {len(top) + len(zero)} contracts "
          f"({_t.time()-t0:.0f}s)", flush=True)

    json.dump({"session": str(fc._SESSION),
               "updated": datetime.now(timezone.utc).isoformat(timespec="seconds"),
               "scanned": len(tickers), "active": len(snaps),
               "clusters": out}, open("flow_latest.json", "w"), indent=1)

    # Log enough that the stick check can show what the original alert said,
    # not just a bare OI delta.
    fc.CONTRACT_FIELDS = ["date", "ticker", "contract", "strike", "type",
                          "expiry", "volume", "open_interest", "premium",
                          "verdict", "band", "spot", "concentration",
                          "ask_share", "confidence", "sweeps", "first_ts",
                          "dte"]
    fc.log_contracts([{
        "date": str(fc._SESSION), "ticker": r["ticker"],
        "contract": r["contract"], "strike": r["strike"], "type": r["type"],
        "expiry": r["expiry"], "volume": r["volume"],
        "open_interest": r["open_interest"], "premium": r["premium"],
        "verdict": r["verdict"], "band": r.get("band"), "spot": r.get("spot"),
        "concentration": r.get("concentration"),
        "ask_share": r.get("ask_share"), "confidence": r.get("confidence"),
        "sweeps": r.get("sweeps"), "first_ts": r.get("first_ts"),
        "dte": r.get("dte")} for r in out])

    if alerts:
        head = f"Options flow · {fc._SESSION} · {len(alerts)} readable"
        if post(head + "\n\n" + "\n\n".join(alerts[:5])):
            remember(new_keys)
            print(f"  alerted on {len(alerts)}", flush=True)
        else:
            print(f"  {len(alerts)} alertable, no webhook set", flush=True)
    else:
        print("  nothing cleared the alert bar", flush=True)

    el = _t.time() - t0
    print(f"done · {len(out)} clusters logged · {el:.0f}s total", flush=True)
    if el > 240:
        print("  NOTE: approaching the 5-minute cron interval. Lower "
              "TOP_N_DEEP or MAX_TICKERS in flow_core.py if runs start "
              "overlapping.", flush=True)


if __name__ == "__main__":
    main()

