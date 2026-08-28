"""
surveyed_map_helpers.py - state for the surveyed plant map.

Replaces the schematic layout used by the original plant map. The topology here
is as-built: 370 trackers at their surveyed positions, 808 strings, 432 MPPTs,
36 inverters, 3 substations, taken from the pile survey and the layout DXF and
cross-checked against CONFIGURAZIONE STRINGHE PER MPPT REV2.

No new alert rules are invented. Severity comes from what the watchdog already
publishes:

  active_anomalies[]        {id, type, severity, message}   red / yellow / grey
  inverter_health{}         overall_status plus pr/temp/dc_current/ac_power/iso
  inverter_health.mppt_data [{mppt, strings, v, exp}]       measured vs expected
  tracker_status            alarm, mode, target_angle, actual_angle
  user_settings.thresholds  tracker_deviation, pr, temp, ac, dc

This module only routes those to the element they belong to, so the map shows
the same state as the rest of the dashboard.
"""
import json
import logging
import re
from datetime import date as _date
from pathlib import Path

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
LAYOUT_PATH = Path(__file__).resolve().parent / "plant_layout_surveyed.json"
SERIALS_PATH = Path(__file__).resolve().parent / "panel_serials.json"
SETTINGS_PATH = ROOT / "user_settings.json"

RANK = {"green": 0, "grey": 1, "yellow": 2, "red": 3}

# the watchdog splits the day at noon and compares per-string current against
# these; mppt_dc_analyzer flags a 2-string MPPT sitting at 40-60% of expected
# as having lost one of its two strings
NOON = 12
SS_LOSS_LO, SS_LOSS_HI = 0.4, 0.6
OPEN_CIRCUIT = 0.1
# the thresholds are calibrated on a 2-string MPPT (analyzer divides the fleet
# 2-string median by 18.0 A), so currents are normalised to that basis
THRESHOLD_BASIS_STRINGS = 2
_LAYOUT_CACHE = None


def load_surveyed_layout() -> dict:
    """Static topology and geometry. Cached; it never changes at runtime."""
    global _LAYOUT_CACHE
    if _LAYOUT_CACHE is None:
        try:
            with open(LAYOUT_PATH, encoding="utf-8") as f:
                _LAYOUT_CACHE = json.load(f)
        except Exception as e:
            logger.error(f"[SURVEYED-MAP] cannot load layout: {e}")
            _LAYOUT_CACHE = {}
    return _LAYOUT_CACHE


def _thresholds() -> dict:
    try:
        with open(SETTINGS_PATH, encoding="utf-8") as f:
            return json.load(f).get("thresholds", {})
    except Exception:
        return {}


def _inv_id(raw: str):
    """'INV TX1-01', 'TX1-01', 'TX1-INV01' -> 'TX1-INV01'."""
    if not raw:
        return None
    m = re.search(r"(TX\d+)\s*[-_]?\s*(?:INV)?\s*(\d{1,2})", str(raw), re.I)
    return "%s-INV%02d" % (m.group(1).upper(), int(m.group(2))) if m else None


def _worse(a: str, b: str) -> str:
    return a if RANK.get(a, 0) >= RANK.get(b, 0) else b


def _hour(snapshot: dict) -> int:
    """Hour the snapshot describes, so the right DC threshold pair is used."""
    from datetime import datetime
    for key in ("timestamp", "data_time"):
        v = snapshot.get(key)
        if v:
            m = re.search(r"(\d{1,2}):(\d{2})", str(v))
            if m:
                return int(m.group(1))
    return datetime.now().hour


def _dc_status(basis_amps, hour, th):
    """The watchdog's own DC rule. Feed it a current normalised to the 2-string
    basis the thresholds were set for, not a single string's share."""
    if basis_amps is None:
        return "grey"
    dc = th.get("dc", {})
    if hour <= NOON:
        g, y = dc.get("morning_green", 10.0), dc.get("morning_yellow", 2.0)
    else:
        g, y = dc.get("afternoon_green", 5.0), dc.get("afternoon_yellow", 0.5)
    if basis_amps >= g:
        return "green"
    if basis_amps >= y:
        return "yellow"
    return "red"


def _anomaly_targets(anom: dict, layout: dict) -> dict:
    """Which map elements an anomaly refers to.

    The watchdog names the inverter in the anomaly id or message, and MPPT
    alarms carry the MPPT number in the message. Anything that names neither is
    plant-wide and is shown in the list rather than painted on one element.
    """
    blob = " ".join(str(anom.get(k, "")) for k in ("id", "type", "message"))
    inv = _inv_id(blob)
    if not inv:
        return {"scope": "plant", "inverters": [], "mppts": [], "trackers": []}
    m = re.search(r"MPPT\s*(\d{1,2})", blob, re.I)
    if m:
        mid = "%s-MPPT%02d" % (inv, int(m.group(1)))
        known = {x["id"] for x in layout.get("mppts", [])}
        if mid in known:
            return {"scope": "mppt", "inverters": [inv], "mppts": [mid], "trackers": []}
    return {"scope": "inverter", "inverters": [inv], "mppts": [], "trackers": []}


def get_surveyed_state(target_date: str = None) -> dict:
    """Per-element severity for the map, plus the problem list behind it."""
    layout = load_surveyed_layout()
    if not layout:
        return {"error": "layout unavailable"}
    day = target_date or _date.today().isoformat()
    th = _thresholds()

    snapshot = {}
    try:
        from db.db_manager import load_latest_snapshot
        snapshot = load_latest_snapshot(day) or {}
    except Exception as e:
        logger.warning(f"[SURVEYED-MAP] no snapshot for {day}: {e}")

    inv_health = snapshot.get("inverter_health", {}) or {}
    anomalies = snapshot.get("active_anomalies", []) or []

    inverters, mppts, trackers, problems = {}, {}, {}, []

    # ---- inverters: the status the dashboard already shows elsewhere
    for raw, h in inv_health.items():
        iid = _inv_id(raw)
        if not iid:
            continue
        status = "grey" if h.get("comms_lost_flag") else (h.get("overall_status") or "grey")
        inverters[iid] = {
            "status": status,
            "pr": h.get("pr"), "pr_v": h.get("pr_v"),
            "temp": h.get("temp"), "temp_v": h.get("temp_v"),
            "dc": h.get("dc_current"), "dc_v": h.get("dc_v"),
            "ac": h.get("ac_power"), "ac_v": h.get("ac_v"),
            "iso": h.get("iso"), "iso_v": h.get("iso_v"),
            "comms_lost": bool(h.get("comms_lost_flag")),
            "data_time": h.get("data_time"),
        }
        # measured vs expected per MPPT, straight from the watchdog
        for m in h.get("mppt_data") or []:
            try:
                mid = "%s-MPPT%02d" % (iid, int(m.get("mppt")))
            except (TypeError, ValueError):
                continue
            v, exp = m.get("v"), m.get("exp")
            n = m.get("strings") or 1
            per = round(v / n, 2) if isinstance(v, (int, float)) and n else None
            ratio = (v / exp) if isinstance(v, (int, float)) and exp else None
            # a 2-string MPPT at 40-60% of expected has lost one of its two
            # strings; the meter cannot say which, so both are marked suspect
            ss_loss = bool(n == 2 and ratio is not None
                           and SS_LOSS_LO <= ratio <= SS_LOSS_HI)
            open_c = bool(ratio is not None and ratio < OPEN_CIRCUIT and exp > 1.0)
            basis = (v * THRESHOLD_BASIS_STRINGS / n) if isinstance(v, (int, float)) and n \
                else None
            mppts[mid] = {"status": "green" if status == "green" else "grey",
                          "v": v, "exp": exp, "strings": n,
                          "per_string_a": per,
                          "basis_a": round(basis, 2) if basis is not None else None,
                          "ratio": round(ratio, 3) if ratio else None,
                          "single_string_loss": ss_loss, "open_circuit": open_c}

    # ---- trackers: their own alarm flag, and the deviation threshold in settings
    dev_limit = th.get("tracker_deviation", 6.0)
    try:
        from db.db_manager import get_all_tracker_status
        rows = get_all_tracker_status() or []
    except Exception as e:
        logger.warning(f"[SURVEYED-MAP] tracker status unavailable: {e}")
        rows = []
    by_key = {}
    for r in rows:
        d = dict(r) if not isinstance(r, dict) else r
        ncu = re.sub(r"\D", "", str(d.get("ncu_id", "")))
        tcu = re.sub(r"\D", "", str(d.get("tcu_id", "")))
        if ncu and tcu:
            by_key[(int(ncu), int(tcu))] = d
    for t in layout.get("trackers", []):
        d = by_key.get((t["ncu"], int(t["tcu"])))
        if not d:
            trackers[t["id"]] = {"status": "grey", "reason": "no tracker data"}
            continue
        tgt, act = d.get("target_angle"), d.get("actual_angle")
        dev = None
        if tgt is not None and act is not None:
            try:
                dev = round(float(act) - float(tgt), 2)
            except (TypeError, ValueError):
                dev = None
        alarm = (str(d.get("alarm")) or "").strip()
        has_alarm = bool(alarm) and alarm.lower() not in ("none", "0", "ok", "nan", "-")
        status = "green"
        reason = None
        if has_alarm:
            status, reason = "red", f"allarme tracker: {alarm}"
        elif dev is not None and abs(dev) > dev_limit:
            status, reason = "yellow", f"scarto angolo {dev:+.1f} deg (soglia {dev_limit})"
        trackers[t["id"]] = {"status": status, "reason": reason, "alarm": alarm or None,
                             "mode": d.get("mode"), "target_angle": tgt,
                             "actual_angle": act, "deviation": dev,
                             "last_update": d.get("last_update")}
        if reason:
            problems.append({"key": "TRACKER", "type": "TRACKER",
                             "severity": status, "scope": "tracker",
                             "element": t["id"], "message": reason,
                             "inverters": [], "mppts": [], "trackers": [t["id"]]})

    # ---- the watchdog's own anomalies, routed to the element they name
    for a in anomalies:
        tgt = _anomaly_targets(a, layout)
        sev = (a.get("severity") or "yellow").lower()
        for iid in tgt["inverters"]:
            if iid in inverters:
                inverters[iid]["status"] = _worse(inverters[iid]["status"], sev)
        for mid in tgt["mppts"]:
            cur = mppts.get(mid) or {"status": "grey"}
            cur["status"] = _worse(cur.get("status", "grey"), sev)
            mppts[mid] = cur
        problems.append({"key": a.get("type") or "ANOMALIA", "type": a.get("type"),
                         "severity": sev, "scope": tgt["scope"],
                         "element": (tgt["mppts"] or tgt["inverters"] or ["IMPIANTO"])[0],
                         "message": a.get("message"), "id": a.get("id"),
                         "inverters": tgt["inverters"], "mppts": tgt["mppts"],
                         "trackers": []})

    # ---- an MPPT with no reading of its own follows its inverter
    for m in layout.get("mppts", []):
        mppts.setdefault(m["id"], {"status": inverters.get(m["inverter"], {})
                                   .get("status", "grey")})

    # ---- strings inherit their MPPT; that is the finest the meters go
    hour = _hour(snapshot)
    strings = {}
    for s in layout.get("strings", []):
        m = mppts.get(s["mppt"], {})
        if m.get("basis_a") is not None:
            dc = _dc_status(m["basis_a"], hour, th)
        else:
            dc = m.get("status", "grey")
        note = None
        if m.get("open_circuit"):
            dc, note = "red", "MPPT a circuito aperto"
        elif m.get("single_string_loss"):
            dc = _worse(dc, "yellow")
            note = "una delle due stringhe di questo MPPT risulta persa"
        if m.get("status") == "red":
            dc = _worse(dc, "red")
        tr = trackers.get(s["tracker"], {}).get("status", "grey")
        strings[s["id"]] = {"status": _worse(dc, tr) if tr == "red" else dc,
                            "per_string_a": m.get("per_string_a"),
                            "basis_a": m.get("basis_a"),
                            "mppt_a": m.get("v"), "mppt_exp_a": m.get("exp"),
                            "mppt_strings": m.get("strings"), "note": note}

    legend = {}
    for p in problems:
        k = p["key"]
        e = legend.setdefault(k, {"key": k, "label": k, "severity": p["severity"], "count": 0})
        e["count"] += 1
        e["severity"] = _worse(e["severity"], p["severity"])

    counts = {}
    for name, coll in (("inverters", inverters), ("mppts", mppts),
                       ("trackers", trackers), ("strings", strings)):
        c = {"green": 0, "yellow": 0, "red": 0, "grey": 0}
        for v in coll.values():
            c[v.get("status", "grey")] = c.get(v.get("status", "grey"), 0) + 1
        counts[name] = c

    return {"date": day,
            "generated": snapshot.get("timestamp") or snapshot.get("data_time"),
            "has_snapshot": bool(snapshot),
            "thresholds": th,
            "inverters": inverters, "mppts": mppts,
            "trackers": trackers, "strings": strings,
            "hour": hour,
            "problems": sorted(problems, key=lambda p: -RANK.get(p["severity"], 0)),
            "legend": sorted(legend.values(), key=lambda e: -RANK.get(e["severity"], 0)),
            "counts": counts}


_SERIALS_CACHE = None


def _serials() -> dict:
    global _SERIALS_CACHE
    if _SERIALS_CACHE is None:
        try:
            with open(SERIALS_PATH, encoding="utf-8") as f:
                _SERIALS_CACHE = json.load(f)
        except Exception as e:
            logger.warning(f"[SURVEYED-MAP] panel serials unavailable: {e}")
            _SERIALS_CACHE = {"by_string": {}, "by_tracker": {}}
    return _SERIALS_CACHE


def get_panel_serials(string_id: str = None, tracker_id: str = None) -> dict:
    """Panel serial numbers for one string, or for a whole tracker.

    20 200 panels in total, so they are served on request rather than shipped
    with the layout. Module order runs north to south.
    """
    data = _serials()
    if string_id:
        serials = data.get("by_string", {}).get(string_id, [])
        return {"scope": "string", "id": string_id, "count": len(serials),
                "modules_per_string": data.get("modules_per_string", 25),
                "serials": [{"n": i + 1, "serial": s} for i, s in enumerate(serials)]}
    if tracker_id:
        meta = data.get("by_tracker", {}).get(tracker_id, {})
        out = []
        for sid in meta.get("strings", []):
            for i, s in enumerate(data.get("by_string", {}).get(sid, [])):
                out.append({"n": i + 1, "serial": s, "string": sid})
        return {"scope": "tracker", "id": tracker_id, "count": len(out),
                "tcu": meta.get("tcu"), "strings": meta.get("strings", []),
                "unassigned": meta.get("unassigned", []), "serials": out}
    d = data
    return {"scope": "plant", "panels": d.get("panels"), "strings": d.get("strings"),
            "trackers": d.get("trackers")}


def get_inverter_detail(inverter_id: str, target_date: str = None) -> dict:
    """Everything behind one inverter: production now, then every tracker, TCU,
    MPPT and string under it with the status each currently holds."""
    layout = load_surveyed_layout()
    state = get_surveyed_state(target_date)
    if not layout or state.get("error"):
        return {"error": "state unavailable"}
    inv = state["inverters"].get(inverter_id, {})
    mine = [s for s in layout.get("strings", []) if s["inverter"] == inverter_id]
    if not mine:
        return {"error": "unknown inverter %s" % inverter_id}

    by_tracker = {}
    for s in mine:
        t = by_tracker.setdefault(s["tracker"], {
            "tracker": s["tracker"], "tcu": s["tcu"], "ncu": s["ncu"], "area": s["area"],
            "status": state["trackers"].get(s["tracker"], {}).get("status", "grey"),
            "strings": []})
        st = state["strings"].get(s["id"], {})
        t["strings"].append({
            "string": s["id"], "mppt": s["mppt"], "status": st.get("status", "grey"),
            "per_string_a": st.get("per_string_a"), "basis_a": st.get("basis_a"),
            "mppt_a": st.get("mppt_a"), "mppt_exp_a": st.get("mppt_exp_a"),
            "mppt_strings": st.get("mppt_strings"), "note": st.get("note")})
    trackers = sorted(by_tracker.values(), key=lambda t: int(t["tracker"].split()[1]))

    counts = {"green": 0, "yellow": 0, "red": 0, "grey": 0}
    for t in trackers:
        for s in t["strings"]:
            counts[s["status"]] = counts.get(s["status"], 0) + 1

    mppt_rows = []
    for m in sorted({s["mppt"] for s in mine}):
        d = state["mppts"].get(m, {})
        mppt_rows.append({"mppt": m, "status": d.get("status"), "v": d.get("v"),
                          "exp": d.get("exp"), "strings": d.get("strings"),
                          "per_string_a": d.get("per_string_a"),
                          "basis_a": d.get("basis_a"),
                          "single_string_loss": d.get("single_string_loss"),
                          "open_circuit": d.get("open_circuit")})
    total = sum(r["v"] for r in mppt_rows if isinstance(r.get("v"), (int, float)))
    return {"inverter": inverter_id, "date": state["date"], "hour": state.get("hour"),
            "status": inv.get("status", "grey"),
            "production": {"ac_w": inv.get("ac_v"), "dc_a": inv.get("dc_v"),
                           "dc_a_sum_mppt": round(total, 2) if total else None,
                           "pr_pct": inv.get("pr_v"), "temp_c": inv.get("temp_v"),
                           "iso": inv.get("iso_v"), "comms_lost": inv.get("comms_lost"),
                           "data_time": inv.get("data_time")},
            "counts": counts, "trackers": trackers, "mppts": mppt_rows,
            "thresholds": state.get("thresholds", {}).get("dc", {})}
