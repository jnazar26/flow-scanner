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
                return [t for t, _ in liq[:n]]
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

    snaps = []
    for t in tickers:
        s = fc.scan_ticker(t)
        if s:
            snaps.append(s)
    if not snaps:
        print("no chain activity"); return
    print(f"  {len(snaps)} with activity", flush=True)

    all_c = []
    for s in snaps:
        for c in s["clusters"]:
            c["_snap"] = s
            c["_score"] = (0.40 * min(c["premium"] / 15e6, 1.0)
                           + 0.25 * min(c["concentration"] / 0.5, 1.0)
                           + 0.20 * c["newness"]
                           + 0.15 * min(c["delta_notional"] / 50e6, 1.0))
            all_c.append(c)
    all_c.sort(key=lambda c: -c["_score"])
    seen, top = defaultdict(int), []
    for c in all_c:
        if seen[c["ticker"]] >= fc.MAX_PER_TICKER:
            continue
        seen[c["ticker"]] += 1
        top.append(c)
        if len(top) >= fc.TOP_N_DEEP:
            break

    for c in top:
        c["_tape"] = fc.classify_contract(c["contract"], fc._SESSION)
    print(f"  classified {len(top)} contracts", flush=True)

    # cross-contract spread pairing (condition codes miss many of these)
    sig = defaultdict(list)
    for c in top:
        for pr in (c.get("_tape") or {}).get("prints", []):
            sig[(c["ticker"], pr["ts"], pr["size"])].append(c)
    for group in sig.values():
        if len({id(x) for x in group}) > 1:
            for c in group:
                c["_paired"] = True

    out, alerts = [], []
    sent = already_sent()
    new_keys = set()
    for c in top:
        tier = ("spread" if c.get("_paired")
                else fc.classify_tier(c, c.get("_tape"))[0])
        tape = c.get("_tape") or {}
        rec = {
            "ticker": c["ticker"], "contract": c["contract"],
            "strike": c["strike"], "type": c["type"],
            "expiry": c["expiry"].isoformat(), "dte": c["dte"],
            "spot": round(c["spot"], 2), "premium": round(c["premium"]),
            "volume": c["volume"], "open_interest": c["oi"],
            "concentration": round(c["concentration"], 3),
            "verdict": tier.upper(),
            "ask_share": tape.get("ask_share"),
            "mid_share": tape.get("mid_share"),
            "confidence": tape.get("confidence"),
            "sweeps": tape.get("sweeps"), "legs": tape.get("legs"),
            "gex": (c["_snap"].get("profile") or {}).get("net_gex"),
            "flip": (c["_snap"].get("profile") or {}).get("flip"),
        }
        out.append(rec)

        key = f"{c['contract']}|{tier}"
        if (tier in ALERT_TIERS
                and c["premium"] >= ALERT_MIN_PREMIUM
                and (tape.get("confidence") or 0) >= ALERT_MIN_CONFIDENCE
                and key not in sent):
            a = tape.get("ask_share") or 0
            side = "bought" if tier == "bullish" else "sold"
            alerts.append(
                f"**{c['ticker']} {c['strike']:g}"
                f"{c['type'][0].upper()} {c['expiry']:%m/%d}** · "
                f"${c['premium']/1e6:.1f}M {side}\n"
                f"{max(a,1-a):.0%} of directional premium · "
                f"conf {tape['confidence']:.2f} · "
                f"{tape.get('sweeps',0)} sweeps · "
                f"{c['concentration']:.0%} of day premium\n"
                f"spot {c['spot']:,.2f} · vol/OI "
                f"{(c['vol_oi'] or 0):.1f}x")
            new_keys.add(key)

    json.dump({"session": str(fc._SESSION),
               "updated": datetime.now(timezone.utc).isoformat(timespec="seconds"),
               "scanned": len(tickers), "active": len(snaps),
               "clusters": out}, open("flow_latest.json", "w"), indent=1)

    fc.log_contracts([{
        "date": str(fc._SESSION), "ticker": r["ticker"],
        "contract": r["contract"], "strike": r["strike"], "type": r["type"],
        "expiry": r["expiry"], "volume": r["volume"],
        "open_interest": r["open_interest"], "premium": r["premium"],
        "verdict": r["verdict"]} for r in out])

    if alerts:
        head = f"Options flow · {fc._SESSION} · {len(alerts)} readable"
        if post(head + "\n\n" + "\n\n".join(alerts[:5])):
            remember(new_keys)
            print(f"  alerted on {len(alerts)}", flush=True)
        else:
            print(f"  {len(alerts)} alertable, no webhook set", flush=True)
    else:
        print("  nothing cleared the alert bar", flush=True)

    print(f"done · {len(out)} clusters logged", flush=True)


if __name__ == "__main__":
    main()
