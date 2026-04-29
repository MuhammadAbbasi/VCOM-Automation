"""
submit_ticket.py — Manual ticket submission tool for Mazara SCADA / Odoo.

Usage:
    python submit_ticket.py
"""

import sys
import io
from datetime import datetime
from pathlib import Path

# Windows console UTF-8 fix
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from db.odoo_client import OdooClient

# ── Odoo connection ──────────────────────────────────────────────────────────
ODOO_URL  = "http://localhost:8069"
ODOO_DB   = "odoo"
ODOO_USER = "pietro.artale@gmail.com"
ODOO_PASS = "odoo"

# ── Choices ──────────────────────────────────────────────────────────────────
FAULT_TYPES = {
    "1":  ("INVERTER TRIP",      "inverter_fault",    "guasto",    "urgente", "urgente"),
    "2":  ("LOW PR",             "produzione_bassa",  "ispezione", "alta",    "alta"),
    "3":  ("CRIT PR",            "produzione_bassa",  "ispezione", "urgente", "urgente"),
    "4":  ("ISO FAULT",          "inverter_fault",    "guasto",    "urgente", "urgente"),
    "5":  ("COMM LOST",          "comunicazione",     "ispezione", "alta",    "alta"),
    "6":  ("DC MPPT FAULT",      "inverter_fault",    "guasto",    "alta",    "alta"),
    "7":  ("HIGH TEMP",          "inverter_fault",    "ispezione", "alta",    "alta"),
    "8":  ("CRIT TEMP",          "inverter_fault",    "ispezione", "urgente", "urgente"),
    "9":  ("TRACKER OFFLINE",    "tracker",           "ispezione", "alta",    "alta"),
    "10": ("GRID LIMIT CHANGE",  "rete",              "ispezione", "normale", "media"),
    "11": ("CUSTOM",             "altro",             "altro",     "normale", "bassa"),
}

INVERTERS = [
    f"TX{tx}-{inv:02d}" for tx in range(1, 4) for inv in range(1, 13)
] + ["PLANT", "GRID", "TRACKER FIELD"]

PRIORITIES = {
    "1": "bassa",
    "2": "normale",
    "3": "alta",
    "4": "urgente",
}

INTERVENTION_TYPES = {
    "1": "manutenzione_ordinaria",
    "2": "manutenzione_straordinaria",
    "3": "guasto",
    "4": "ispezione",
    "5": "sfalcio",
    "6": "collaudo",
    "7": "altro",
}

# ── Helpers ──────────────────────────────────────────────────────────────────
def hr(char="─", n=60):
    print(char * n)

def pick(prompt: str, options: dict, default: str | None = None) -> str:
    while True:
        val = input(f"{prompt}: ").strip()
        if not val and default:
            return default
        if val in options:
            return val
        print(f"  ↳ Invalid choice. Options: {', '.join(options.keys())}")

def ask(prompt: str, default: str = "") -> str:
    val = input(f"{prompt} [{default}]: ").strip() if default else input(f"{prompt}: ").strip()
    return val if val else default

def pick_inverter() -> str:
    print("\nInverters: (type directly, e.g. TX1-03, TX2-11, PLANT, GRID)")
    print("  or press ENTER to list all")
    val = input("Device/Inverter: ").strip()
    if not val:
        for i, inv in enumerate(INVERTERS, 1):
            print(f"  {i:2d}. {inv}", end="\t" if i % 4 else "\n")
        print()
        idx = ask("Pick number (1-39)", "1")
        try:
            return INVERTERS[int(idx) - 1]
        except (ValueError, IndexError):
            return "PLANT"
    return val.upper()

# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    hr("═")
    print("  MAZARA SCADA — Manual Ticket Submission")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    hr("═")

    # Connect
    print("\nConnecting to Odoo...")
    client = OdooClient(ODOO_URL, ODOO_DB, ODOO_USER, ODOO_PASS)
    if not client.login():
        print("ERROR: Could not authenticate with Odoo. Check credentials.")
        sys.exit(1)
    print("  ✓ Connected\n")

    # ── Step 1: Fault type ──────────────────────────────────────────────────
    hr()
    print("STEP 1 — Fault Type")
    hr()
    for k, (label, *_) in FAULT_TYPES.items():
        print(f"  {k:>2s}. {label}")

    choice = pick("\nSelect fault type", FAULT_TYPES)
    fault_label, anom_tipo, intv_tipo, intv_prio, anom_prio = FAULT_TYPES[choice]

    if choice == "11":  # CUSTOM
        fault_label = ask("Custom fault name", "CUSTOM FAULT")

    # ── Step 2: Device ──────────────────────────────────────────────────────
    hr()
    print("STEP 2 — Affected Device")
    hr()
    device = pick_inverter()

    # ── Step 3: Priority override ───────────────────────────────────────────
    hr()
    print("STEP 3 — Priority")
    hr()
    print("  1. Bassa    2. Normale    3. Alta    4. Urgente")
    prio_key = pick("Priority", PRIORITIES, default=list(PRIORITIES.keys())[list(PRIORITIES.values()).index(intv_prio)])
    intv_prio = PRIORITIES[prio_key]
    anom_prio = intv_prio  # keep in sync

    # ── Step 4: Intervention type ───────────────────────────────────────────
    hr()
    print("STEP 4 — Intervention Type")
    hr()
    for k, v in INTERVENTION_TYPES.items():
        print(f"  {k}. {v}")
    intv_key = pick("Intervention type", INTERVENTION_TYPES,
                    default=list(INTERVENTION_TYPES.keys())[list(INTERVENTION_TYPES.values()).index(intv_tipo)] if intv_tipo in INTERVENTION_TYPES.values() else "4")
    intv_tipo = INTERVENTION_TYPES[intv_key]

    # ── Step 5: Description ─────────────────────────────────────────────────
    hr()
    print("STEP 5 — Fault Description")
    hr()
    print("Describe the fault. Type your text and press ENTER.")
    print("Leave the line EMPTY and press ENTER again to finish.\n")
    desc_lines = []
    try:
        while True:
            line = input()
            if line == "":
                break
            desc_lines.append(line)
    except EOFError:
        pass
    description = "\n".join(desc_lines).strip() or f"{fault_label} on {device} — manually submitted."

    # ── Step 6: Notes ───────────────────────────────────────────────────────
    try:
        notes = ask("\nAdditional notes (optional)", "")
    except EOFError:
        notes = ""

    # ── Step 7: Field intervention required? ────────────────────────────────
    needs_field = ask("\nRequires field intervention? (y/n)", "y").lower().startswith("y")

    # ── Preview ─────────────────────────────────────────────────────────────
    hr("═")
    print("PREVIEW — Ticket to be created")
    hr("═")

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    titolo  = f"[{fault_label}] {device} — Mazara 01"

    causa = "\n".join([
        f"SCADA FAULT REPORT — Manually Submitted",
        f"{'='*50}",
        f"Fault Type      : {fault_label}",
        f"Device          : {device}",
        f"Submitted At    : {now_str}",
        f"Priority        : {intv_prio.upper()}",
        f"",
        f"DESCRIPTION",
        f"{'-'*50}",
        description,
    ])
    if notes:
        causa += f"\n\nNOTES\n{'-'*50}\n{notes}"

    print(f"  Title        : {titolo}")
    print(f"  Fault Type   : {fault_label}")
    print(f"  Device       : {device}")
    print(f"  Priority     : {intv_prio.upper()}")
    print(f"  Intv Type    : {intv_tipo}")
    print(f"  Field Work   : {'Yes' if needs_field else 'No'}")
    print(f"  Description  : {description[:80]}{'...' if len(description)>80 else ''}")
    hr()

    confirm = ask("Submit this ticket? (y/n)", "y").lower()
    if not confirm.startswith("y"):
        print("\nAborted. No ticket created.")
        return

    # ── Submit ───────────────────────────────────────────────────────────────
    hr("═")
    print("Submitting to Odoo...")
    hr("═")

    # 1. SCADA Session
    print("  Creating SCADA session...", end=" ", flush=True)
    session_id = client.create_scada_session(
        fault_summary=f"Manual ticket: {fault_label} on {device}",
        stato_impianto="alarm" if intv_prio == "urgente" else "warning",
    )
    print(f"✓  Session ID: {session_id}")

    # 2. Anomalia
    print("  Creating anomalia...", end=" ", flush=True)
    anomalia_id = client.create_anomalia(
        session_id=session_id,
        titolo=titolo,
        tipo=anom_tipo,
        priorita=anom_prio,
        descrizione=causa,
        intervento_richiesto=needs_field,
    )
    print(f"✓  Anomalia ID: {anomalia_id}")

    # 3. Intervento
    print(f"DEBUG: calling create_intervento with tipo={intv_tipo}, prio={intv_prio}")
    print("  Creating intervento...", end=" ", flush=True)
    intervento_id = client.create_intervento(
        titolo=titolo,
        tipo_intervento=intv_tipo,
        priorita=intv_prio,
        causa_guasto=causa,
        session_id=session_id,
    )
    print(f"✓  Intervento ID: {intervento_id}")

    # 4. Link
    if anomalia_id and intervento_id:
        client.link_anomalia_to_intervento(anomalia_id, intervento_id)
        print("  Linked anomalia → intervento ✓")

    # 5. Verify and print final names
    intv_data = client.get_intervento(intervento_id)
    intv_name = intv_data["name"] if intv_data else f"ID {intervento_id}"

    # ── Result ───────────────────────────────────────────────────────────────
    hr("═")
    print(f"\n  TICKET CREATED SUCCESSFULLY\n")
    print(f"  Intervento  : {intv_name}  (state: nuovo — in attesa di assegnazione admin)")
    print(f"  Anomalia ID : {anomalia_id}")
    print(f"  Session     : {session_id}")
    print(f"\n  Open in Odoo → http://localhost:8069/odoo/fv-interventi")
    hr("═")

    # ── Another ticket? ──────────────────────────────────────────────────────
    again = ask("\nSubmit another ticket? (y/n)", "n").lower()
    if again.startswith("y"):
        print()
        main()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nCancelled.")
