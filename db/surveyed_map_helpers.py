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
OVERRIDES_PATH = Path(__file__).resolve().parent / "panel_serial_overrides.json"
SETTINGS_PATH = ROOT / "user_settings.json"

RANK = {"green": 0, "grey": 1, "yellow": 2, "red": 3}

# mppt_dc_analyzer's own DC rules, as ratios of measured to expected. It sets
# exp = fleet 2-string median scaled to this MPPT's nominal, read from the same
# row as v, so the ratio is irradiance-independent and holds at any hour --
# unlike the inverter DC LED, which compares an absolute average and therefore
# turns every inverter yellow at dusk without naming a single MPPT.
NOON = 12
OPEN_CIRCUIT = 0.1                    # OPEN CIRCUIT           CRITICAL
SS_LOSS_LO, SS_LOSS_HI = 0.4, 0.6     # SINGLE STRING LOSS     CRITICAL
LOW_VS_PEERS = 0.65                   # LOW CURRENT (vs PEERS) WARNING
UNDERPERF = 0.75                      # UNDERPERFORMANCE       INFO
# below this the plant is dark and no ratio means anything
DARK_EXPECTED_A = 1.0
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


def _dc_led_note(dc_v, hour, th):
    """Why the watchdog's inverter DC LED is off green: it compares the latest
    average current against absolute amps, which is the only 'now' DC number
    the snapshot carries."""
    dc = th.get("dc", {})
    g = dc.get("morning_green", 10.0) if hour <= NOON else dc.get("afternoon_green", 5.0)
    if isinstance(dc_v, (int, float)):
        return "corrente DC media dell'inverter %.1f A, sotto la soglia di %.1f A" % (dc_v, g)
    return "corrente DC dell'inverter sotto la soglia di %.1f A" % g


def _mppt_dc_status(v, exp, strings):
    """One MPPT judged by the DC rules mppt_dc_analyzer already defines.

    Returns (status, note, ratio). The analyzer requires the condition to hold
    for 30-60 minutes before it raises an alarm; the map is a live view, so it
    shows the condition as soon as the reading does.
    """
    if not isinstance(v, (int, float)) or not isinstance(exp, (int, float)):
        return "grey", "nessuna lettura DC", None
    if exp <= DARK_EXPECTED_A:
        return "grey", "irraggiamento assente", None
    r = v / exp
    pct = "%.0f%% dell'atteso" % (r * 100)
    if r < OPEN_CIRCUIT:
        return "red", "circuito aperto (%s)" % pct, r
    if strings == 2 and SS_LOSS_LO <= r <= SS_LOSS_HI:
        return "red", "persa una delle due stringhe (%s)" % pct, r
    if r < LOW_VS_PEERS:
        return "yellow", "corrente bassa rispetto ai pari (%s)" % pct, r
    if r < UNDERPERF:
        return "yellow", "sotto-rendimento (%s)" % pct, r
    return "green", None, r


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
    hour = _hour(snapshot)

    inverters, mppts, trackers, problems = {}, {}, {}, []
    dc_low = {}

    # ---- inverters: the status the dashboard already shows elsewhere
    for raw, h in inv_health.items():
        iid = _inv_id(raw)
        if not iid:
            continue
        status = "grey" if h.get("comms_lost_flag") else (h.get("overall_status") or "grey")
        dc_led = "grey" if h.get("comms_lost_flag") else (h.get("dc_current") or "grey")
        led_note = _dc_led_note(h.get("dc_v"), hour, th) if dc_led in ("yellow", "red") else None
        if led_note:
            dc_low[iid] = (dc_led, led_note)
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
            m_st, m_note, _ = _mppt_dc_status(v, exp, n)
            own = m_st
            # dc_v is read at the latest timestamp, v/exp at the fleet-median
            # reference row, so the LED can already be low while the ratio still
            # reads a healthy earlier moment. The newer number wins as a floor.
            if RANK.get(dc_led, 0) > RANK.get(m_st, 0):
                m_st = dc_led
                m_note = m_note or led_note
            mppts[mid] = {"status": m_st, "own": own, "note": m_note,
                          "v": v, "exp": exp, "strings": n,
                          "per_string_a": per,
                          "basis_a": round(basis, 2) if basis is not None else None,
                          "ratio": round(ratio, 3) if ratio else None,
                          "single_string_loss": ss_loss, "open_circuit": open_c}

    # ---- the inverter carries the worst of its own parts.
    # overall_status folds in a DC LED taken from an absolute average of all 12
    # MPPTs, which flags the whole fleet every evening and points at no MPPT.
    # Where per-MPPT readings exist they own the DC verdict, so the inverter
    # takes the worst of them plus its own non-DC LEDs. The raw LEDs stay in the
    # payload, so the detail panel still reports dc_current as the watchdog set it.
    for iid, inv in inverters.items():
        if inv.get("comms_lost"):
            continue
        mine = [m for mid, m in mppts.items() if mid.startswith(iid + "-")]
        if not mine:
            continue
        own = "green"
        for k in ("pr", "temp", "dc", "ac", "iso"):
            own = _worse(own, inv.get(k) or "green")
        for m in mine:
            own = _worse(own, m.get("status", "grey"))
        inv["status"] = own

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
        # the column carries the TCU's own status colour, not an alarm message,
        # so "green" here means healthy. Only an unrecognised value is treated
        # as text worth showing, in case a TCU ever reports one.
        raw_alarm = d.get("alarm")
        alarm = str(raw_alarm or "").strip().lower()
        status = "green"
        reason = None
        if alarm in ("red", "rosso", "alarm", "allarme"):
            status, reason = "red", "allarme tracker"
        elif alarm in ("yellow", "orange", "giallo", "warning"):
            status, reason = "yellow", "avviso tracker"
        elif alarm not in ("", "green", "verde", "none", "ok", "0", "nan",
                           "-", "grey", "gray", "grigio"):
            status, reason = "red", "allarme tracker: %s" % raw_alarm
        if reason is None and dev is not None and abs(dev) > dev_limit:
            status, reason = "yellow", f"scarto angolo {dev:+.1f} deg (soglia {dev_limit})"
        trackers[t["id"]] = {"status": status, "reason": reason, "alarm": raw_alarm,
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
    strings = {}
    for s in layout.get("strings", []):
        m = mppts.get(s["mppt"], {})
        dc = m.get("status", "grey")
        note = m.get("note")
        if m.get("single_string_loss"):
            note = "una delle due stringhe di questo MPPT risulta persa"
        strings[s["id"]] = {"status": dc,
                            "per_string_a": m.get("per_string_a"),
                            "basis_a": m.get("basis_a"),
                            "mppt": s["mppt"], "ratio": m.get("ratio"),
                            "mppt_a": m.get("v"), "mppt_exp_a": m.get("exp"),
                            "mppt_strings": m.get("strings"), "note": note}

    # an MPPT off its expected current is a problem in its own right, so it is
    # listed and clickable even when the watchdog has not yet held it long
    # enough to raise an alarm. Ones already named by an anomaly are not repeated.
    named = {mid for p in problems for mid in p.get("mppts") or []}
    for mid, m in sorted(mppts.items()):
        # "own" is the MPPT's own ratio verdict; a colour it merely inherited
        # from its inverter's LED belongs to the inverter, listed below
        if m.get("own") not in ("yellow", "red") or mid in named:
            continue
        problems.append({"key": "CORRENTE DC", "type": "CORRENTE DC",
                         "severity": m["status"], "scope": "mppt",
                         "element": mid, "message": "%s: %s" % (mid, m.get("note") or ""),
                         "inverters": [mid.rsplit("-", 1)[0]], "mppts": [mid],
                         "trackers": []})

    # the inverter DC LED compares an absolute average, so a low-irradiance
    # evening trips the whole fleet at once. One row per inverter would bury
    # everything else, so a fleet-wide dip is collapsed into a single entry the
    # way the watchdog collapses its own repeated alarms.
    dc_named = {i for p in problems for i in p.get("inverters") or []}
    rest = {i: v for i, v in dc_low.items() if i not in dc_named}
    if rest and len(rest) > len(inverters) / 2:
        sev = "green"
        for s, _ in rest.values():
            sev = _worse(sev, s)
        problems.append({"key": "CORRENTE DC", "type": "CORRENTE DC",
                         "severity": sev, "scope": "plant", "element": "IMPIANTO",
                         "message": "corrente DC sotto soglia su %d inverter su %d: "
                                    "condizione di impianto, nessun MPPT isolato"
                                    % (len(rest), len(inverters)),
                         "inverters": sorted(rest), "mppts": [], "trackers": []})
    else:
        for iid, (sev, note) in sorted(rest.items()):
            problems.append({"key": "CORRENTE DC", "type": "CORRENTE DC",
                             "severity": sev, "scope": "inverter", "element": iid,
                             "message": "%s: %s" % (iid, note),
                             "inverters": [iid], "mppts": [], "trackers": []})

    legend = {}
    for p in problems:
        k = p["key"]
        e = legend.setdefault(k, {"key": k, "label": k, "severity": p["severity"], "count": 0})
        e["count"] += 1
        e["severity"] = _worse(e["severity"], p["severity"])

    reporting = sum(1 for t in trackers.values() if t.get("status") != "grey")
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
            "tracker_feed": {"reporting": reporting,
                             "total": len(layout.get("trackers", []))},
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
    ov = _overrides_by_slot()
    if string_id:
        serials = data.get("by_string", {}).get(string_id, [])
        out = []
        for i, s in enumerate(serials):
            row = {"n": i + 1, "serial": s}
            _apply_override(row, ov.get((string_id, i + 1)))
            out.append(row)
        return {"scope": "string", "id": string_id, "count": len(out),
                "modules_per_string": data.get("modules_per_string", 25),
                "serials": out}
    if tracker_id:
        meta = data.get("by_tracker", {}).get(tracker_id, {})
        out = []
        for sid in meta.get("strings", []):
            for i, s in enumerate(data.get("by_string", {}).get(sid, [])):
                row = {"n": i + 1, "serial": s, "string": sid}
                _apply_override(row, ov.get((sid, i + 1)))
                out.append(row)
        return {"scope": "tracker", "id": tracker_id, "count": len(out),
                "tcu": meta.get("tcu"), "strings": meta.get("strings", []),
                "unassigned": meta.get("unassigned", []), "serials": out}
    d = data
    # per-tracker coverage, so the map can colour by whether every module on a
    # tracker actually has a serial recorded against it
    cov = {}
    for tid, meta in (d.get("by_tracker") or {}).items():
        cov[tid] = {"panels": meta.get("panels", 0),
                    "strings": len(meta.get("strings") or []),
                    "unassigned": len(meta.get("unassigned") or [])}
    return {"scope": "plant", "panels": d.get("panels"), "strings": d.get("strings"),
            "trackers": d.get("trackers"), "coverage": cov}


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
                          "ratio": d.get("ratio"), "note": d.get("note"),
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


# ---------------------------------------------------------------- serial edits

SERIAL_RE = re.compile(r"^[A-Z0-9]{16,28}$")


def _load_overrides() -> list:
    """Append-only log of panel replacements. The workbook stays authoritative
    for the original build; this records what changed in the field since."""
    try:
        with open(OVERRIDES_PATH, encoding="utf-8") as f:
            return json.load(f).get("changes", [])
    except Exception:
        return []


def _overrides_by_slot() -> dict:
    """Latest change per (string, module), keyed for lookup."""
    out = {}
    for c in _load_overrides():
        out[(c.get("string"), c.get("module"))] = c
    return out


def _apply_override(row: dict, change: dict) -> None:
    if not change:
        return
    row["original_serial"] = row["serial"]
    row["serial"] = change.get("new")
    row["changed_at"] = change.get("at")
    row["changed_by"] = change.get("by")
    row["change_note"] = change.get("note")


def _serial_index() -> dict:
    """Every serial the plant has ever carried -> where it was seen.

    A panel taken out is never refitted, so a retired serial stays spent and
    must not come back on another module.
    """
    data = _serials()
    ov = _overrides_by_slot()
    idx = {}
    for sid, lst in (data.get("by_string") or {}).items():
        for i, s in enumerate(lst):
            slot = (sid, i + 1)
            ch = ov.get(slot)
            cur = ch["new"] if ch else s
            idx.setdefault(cur, []).append({"string": sid, "module": i + 1,
                                            "state": "attuale"})
            if ch:
                for old in {ch.get("old"), ch.get("original"), s}:
                    if old and old != cur:
                        idx.setdefault(old, []).append(
                            {"string": sid, "module": i + 1, "state": "sostituito",
                             "at": ch.get("at")})
    return idx


def validate_serial(value: str, string_id: str, module: int) -> str:
    """Returns an error message, or None when the value is acceptable."""
    v = (value or "").strip().upper()
    if not v:
        return "Il seriale non puo essere vuoto."
    if not SERIAL_RE.match(v):
        return ("Formato non valido: attesi 16-28 caratteri alfanumerici "
                "maiuscoli, ricevuto %r." % value)
    for where in _serial_index().get(v, []):
        if (where["string"], where["module"]) == (string_id, module) \
                and where["state"] == "attuale":
            continue
        if where["state"] == "attuale":
            return "Seriale gia presente su %s modulo %d." % (where["string"],
                                                              where["module"])
        return ("Seriale gia usato su %s modulo %d e poi sostituito. "
                "Un pannello rimosso non torna in campo."
                % (where["string"], where["module"]))
    return None


def record_serial_change(string_id: str, module: int, new_serial: str,
                         by: str = None, note: str = None) -> dict:
    """Record one panel replacement. Nothing is overwritten: the previous value
    is kept on the entry so the history stays readable."""
    data = _serials()
    lst = (data.get("by_string") or {}).get(string_id)
    if not lst:
        return {"error": "stringa sconosciuta: %s" % string_id}
    try:
        module = int(module)
    except (TypeError, ValueError):
        return {"error": "modulo non valido"}
    if not 1 <= module <= len(lst):
        return {"error": "modulo %s fuori intervallo 1-%d" % (module, len(lst))}

    new_serial = (new_serial or "").strip().upper()
    err = validate_serial(new_serial, string_id, module)
    if err:
        return {"error": err}

    ov = _overrides_by_slot().get((string_id, module))
    previous = ov["new"] if ov else lst[module - 1]
    if previous == new_serial:
        return {"error": "Il seriale e gia questo."}

    from datetime import datetime
    entry = {"string": string_id, "module": module,
             "old": previous, "new": new_serial,
             "original": lst[module - 1],
             "at": datetime.now().isoformat(timespec="seconds"),
             "by": (by or "dashboard").strip()[:60],
             "note": (note or "").strip()[:200] or None}
    changes = _load_overrides()
    changes.append(entry)
    tmp = OVERRIDES_PATH.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump({"note": "append-only log of panel replacements; the workbook "
                           "remains the source for the original build",
                   "changes": changes}, f, indent=2, ensure_ascii=False)
    tmp.replace(OVERRIDES_PATH)
    logger.info("[SURVEYED-MAP] panel %s#%d %s -> %s by %s",
                string_id, module, previous, new_serial, entry["by"])
    return {"ok": True, "change": entry, "total_changes": len(changes)}


def get_serial_issues() -> dict:
    """Problems in the serial mapping worth fixing, with where to find them."""
    data = _serials()
    ov = _overrides_by_slot()
    by_string = data.get("by_string") or {}
    seen, dupes, malformed, notes = {}, [], [], []
    for sid, lst in by_string.items():
        for i, s in enumerate(lst):
            cur = (ov.get((sid, i + 1)) or {}).get("new", s)
            if not SERIAL_RE.match(cur):
                malformed.append({"string": sid, "module": i + 1, "serial": cur})
            if cur in seen:
                dupes.append({"serial": cur, "first": seen[cur],
                              "second": {"string": sid, "module": i + 1}})
            else:
                seen[cur] = {"string": sid, "module": i + 1}
    for tid, meta in (data.get("by_tracker") or {}).items():
        for extra in meta.get("unassigned") or []:
            notes.append({"tracker": tid, "text": extra})
    return {"duplicates": dupes, "malformed": malformed, "stray_notes": notes,
            "changes_recorded": len(_load_overrides()),
            "total": len(dupes) + len(malformed) + len(notes)}


def serials_export_rows() -> list:
    """One row per panel: the serial in place now, the one it replaced, and a
    note carrying that history in plain words. Ready to hand to anyone."""
    layout = load_surveyed_layout()
    data = _serials()
    ov = _overrides_by_slot()
    by_string = data.get("by_string") or {}

    meta = {}
    for s in layout.get("strings", []):
        meta[s["id"]] = s

    counts = {}
    for sid, lst in by_string.items():
        for i, s in enumerate(lst):
            cur = (ov.get((sid, i + 1)) or {}).get("new", s)
            counts[cur] = counts.get(cur, 0) + 1

    rows = []
    for sid in sorted(by_string):
        m = meta.get(sid, {})
        for i, original in enumerate(by_string[sid]):
            n = i + 1
            ch = ov.get((sid, n))
            cur = ch["new"] if ch else original
            note = ""
            if ch:
                note = "sostituito il %s%s, prima %s%s" % (
                    (ch.get("at") or "")[:10],
                    " da " + ch["by"] if ch.get("by") else "",
                    ch.get("old") or original,
                    "; " + ch["note"] if ch.get("note") else "")
            problem = ""
            if counts.get(cur, 0) > 1:
                problem = "DUPLICATO"
            elif not SERIAL_RE.match(cur):
                problem = "FORMATO ANOMALO"
            rows.append({
                "Serial": cur,
                "Previous_Serial": (ch.get("old") or original) if ch else "",
                "Note": note,
                "Status": "sostituito" if ch else "originale",
                "Problem": problem,
                "String": sid,
                "MPPT": m.get("mppt", ""),
                "Inverter": m.get("inverter", ""),
                "Tracker": m.get("tracker", ""),
                "TCU": m.get("tcu", ""),
                "Area": m.get("area", ""),
                "Module_in_string": n,
                "Changed_at": ch.get("at", "") if ch else "",
                "Changed_by": ch.get("by", "") if ch else "",
            })
    return rows


SERIAL_EXPORT_HEADER = ["Serial", "Previous_Serial", "Note", "Status", "Problem",
                        "String", "MPPT", "Inverter", "Tracker", "TCU", "Area",
                        "Module_in_string", "Changed_at", "Changed_by"]


def serials_export_csv() -> str:
    """The same rows as CSV text, UTF-8 BOM so Excel opens it directly."""
    import csv
    import io as _io
    buf = _io.StringIO()
    w = csv.DictWriter(buf, fieldnames=SERIAL_EXPORT_HEADER, extrasaction="ignore",
                       lineterminator="\r\n")
    w.writeheader()
    w.writerows(serials_export_rows())
    return "\ufeff" + buf.getvalue()


def find_serial(query: str, limit: int = 25) -> dict:
    """Locate a panel by serial. Matches a full serial or any fragment, and
    reports superseded ones too so a swapped panel can still be traced."""
    q = (query or "").strip().upper()
    if len(q) < 4:
        return {"query": query, "matches": [], "note": "almeno 4 caratteri"}
    layout = load_surveyed_layout()
    meta = {s["id"]: s for s in layout.get("strings", [])}
    out = []
    for serial, places in _serial_index().items():
        if q not in serial:
            continue
        for p in places:
            m = meta.get(p["string"], {})
            out.append({"serial": serial, "string": p["string"],
                        "module": p["module"], "state": p["state"],
                        "replaced_at": p.get("at"),
                        "tracker": m.get("tracker"), "tcu": m.get("tcu"),
                        "mppt": m.get("mppt"), "inverter": m.get("inverter"),
                        "area": m.get("area"),
                        "exact": serial == q})
        if len(out) >= limit * 3:
            break
    out.sort(key=lambda r: (not r["exact"], r["state"] != "attuale", r["serial"]))
    return {"query": query, "matches": out[:limit], "total": len(out)}


def serial_problem_locations() -> dict:
    """Where the defective serials are, so the map can point at them."""
    iss = get_serial_issues()
    layout = load_surveyed_layout()
    meta = {s["id"]: s for s in layout.get("strings", [])}
    out = []

    def add(kind, serial, string_id, module, detail):
        m = meta.get(string_id, {})
        out.append({"kind": kind, "serial": serial, "string": string_id,
                    "module": module, "detail": detail,
                    "tracker": m.get("tracker"), "tcu": m.get("tcu"),
                    "mppt": m.get("mppt"), "inverter": m.get("inverter")})

    for d in iss["duplicates"]:
        for side in ("first", "second"):
            w = d[side]
            add("duplicato", d["serial"], w["string"], w["module"],
                "stesso seriale su %s mod %d e %s mod %d"
                % (d["first"]["string"], d["first"]["module"],
                   d["second"]["string"], d["second"]["module"]))
    for d in iss["malformed"]:
        add("formato", d["serial"], d["string"], d["module"],
            "formato anomalo: %s" % d["serial"])
    for d in iss["stray_notes"]:
        out.append({"kind": "annotazione", "serial": None, "string": None,
                    "module": None, "tracker": d["tracker"],
                    "detail": d["text"]})
    return {"problems": out, "total": len(out)}
