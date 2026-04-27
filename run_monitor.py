"""
run_monitor.py — Orchestrator for the Mazara SCADA monitoring system.

Launches four concurrent services:
  1. [DASHBOARD]  dashboard/app.py      — opened in a NEW terminal window
  2. [WATCHDOG]   processor_watchdog.py — logs in THIS terminal
  3. [EXTRACTION] vcom_monitor.py       — logs in THIS terminal
  4. [TELEGRAM]   telegram_bot.py       — logs in THIS terminal

The dashboard gets its own console so its output stays separate.
WATCHDOG and EXTRACTION stream their logs here with prefixes.

Run with:
    python run_monitor.py
"""

import os
import socket
import subprocess
import sys
import threading
import time
import signal
from pathlib import Path

# Fix for Windows console encoding issues
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except:
        pass

ROOT = Path(__file__).resolve().parent

RESTART_COOLDOWN = 5
_stop_event = threading.Event()
_processes: dict[str, subprocess.Popen] = {}


# ---------------------------------------------------------------------------
# Service definitions
# ---------------------------------------------------------------------------

# DASHBOARD → new console window (CREATE_NEW_CONSOLE = 0x00000010)
# WATCHDOG + EXTRACTION → piped into this terminal with prefixes
CREATE_NEW_CONSOLE = 0x00000010

DASHBOARD_CMD = [sys.executable, "-u", str(ROOT / "dashboard" / "app.py")]

SERVICES = [
    {
        "name": "DASHBOARD",
        "cmd": DASHBOARD_CMD,
        "new_console": True,
    },
    {
        "name": "WATCHDOG",
        "cmd": [sys.executable, "-u", str(ROOT / "processor_watchdog_final.py")],
        "new_console": False,
    },
    {
        "name": "EXTRACTION",
        "cmd": [sys.executable, "-u", str(ROOT / "vcom_monitor.py")],
        "new_console": False,
    },
    {
        "name": "TELEGRAM",
        "cmd": [sys.executable, "-u", str(ROOT / "telegram_bot.py")],
        "new_console": False,
    },
    {
        "name": "TRACKER",
        "cmd": [sys.executable, "-u", str(ROOT / "tracker_testing" / "receiver.py")],
        "new_console": False,
    },
]


# ---------------------------------------------------------------------------
# Log streaming thread (for services running in this terminal)
# ---------------------------------------------------------------------------

def stream_output(proc: subprocess.Popen, prefix: str) -> None:
    print(f"[ORCHESTRATOR] Logging stream for {prefix} started.", flush=True)
    try:
        for line in proc.stdout:
            if line:
                print(f"[{prefix}] {line}", end="", flush=True)
    except Exception as e:
        print(f"[ORCHESTRATOR] Error reading output from {prefix}: {e}", flush=True)


# ---------------------------------------------------------------------------
# Launchers
# ---------------------------------------------------------------------------

def launch_dashboard() -> subprocess.Popen:
    """Launch dashboard in a separate console window or standard process."""
    creationflags = CREATE_NEW_CONSOLE if os.name == "nt" else 0
    proc = subprocess.Popen(
        DASHBOARD_CMD,
        cwd=str(ROOT),
        creationflags=creationflags,
    )
    _processes["DASHBOARD"] = proc
    print(f"[ORCHESTRATOR] Started DASHBOARD (pid={proc.pid})", flush=True)
    return proc


def launch_service(svc: dict) -> subprocess.Popen:
    """Launch a service. Dashboard gets a new console window; others pipe here."""
    name = svc["name"]
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    
    if svc.get("new_console"):
        # DASHBOARD specific: resolve port conflict if any
        if name == "DASHBOARD":
            port = 8080 # Default for Mazara Dashboard
            if _port_in_use(port):
                kill_port_process(port)

        # Dashboard → separate console window
        creationflags = CREATE_NEW_CONSOLE if os.name == "nt" else 0
        proc = subprocess.Popen(
            svc["cmd"],
            cwd=str(ROOT),
            env=env,
            creationflags=creationflags,
        )
        _processes[name] = proc
        print(f"[ORCHESTRATOR] Started {name} in new window (pid={proc.pid})", flush=True)
    else:
        # WATCHDOG / EXTRACTION → piped into this terminal
        proc = subprocess.Popen(
            svc["cmd"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
            cwd=str(ROOT),
            env=env,
        )
        _processes[name] = proc
        t = threading.Thread(target=stream_output, args=(proc, name), daemon=True)
        t.start()
        print(f"[ORCHESTRATOR] Started {name} (pid={proc.pid})", flush=True)
    
    return proc


# ---------------------------------------------------------------------------
# Health monitor
# ---------------------------------------------------------------------------

def _port_in_use(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(("127.0.0.1", port)) == 0


def kill_port_process(port: int):
    """Find and kill the process using the specified port on Windows."""
    if os.name != "nt": return
    try:
        # Find PID using netstat
        output = subprocess.check_output(f"netstat -ano | findstr :{port}", shell=True).decode()
        for line in output.splitlines():
            if "LISTENING" in line:
                parts = line.split()
                pid = parts[-1]
                print(f"[ORCHESTRATOR] Conflict: Port {port} is used by PID {pid}. Killing it...", flush=True)
                subprocess.run(["taskkill", "/F", "/PID", pid], capture_output=True)
                time.sleep(1) # Give OS time to release the socket
    except Exception:
        pass


def monitor_services() -> None:
    service_procs = {svc["name"]: launch_service(svc) for svc in SERVICES}

    while not _stop_event.is_set():
        time.sleep(1)

        # Restart WATCHDOG / EXTRACTION if they crash
        for svc in SERVICES:
            name = svc["name"]
            proc = service_procs.get(name)
            if proc is None:
                continue
            rc = proc.poll()
            if rc is not None and not _stop_event.is_set():
                print(
                    f"[ORCHESTRATOR] {name} exited (rc={rc}) — restarting in {RESTART_COOLDOWN}s",
                    flush=True,
                )
                time.sleep(RESTART_COOLDOWN)
                if not _stop_event.is_set():
                    service_procs[name] = launch_service(svc)


# ---------------------------------------------------------------------------
# Graceful shutdown
# ---------------------------------------------------------------------------

def shutdown(signum=None, frame=None) -> None:
    print("\n[ORCHESTRATOR] Shutting down all services…", flush=True)
    _stop_event.set()
    for name, proc in list(_processes.items()):
        try:
            proc.terminate()
            proc.wait(timeout=10)
            print(f"[ORCHESTRATOR] {name} stopped", flush=True)
        except Exception as e:
            print(f"[ORCHESTRATOR] Could not stop {name}: {e}", flush=True)
    sys.exit(0)


# ---------------------------------------------------------------------------
# Hot Reloader (Local CI/CD)
# ---------------------------------------------------------------------------

def get_last_mod_time(directory: Path) -> float:
    """Return the maximum modification time of all .py files in the project."""
    max_mtime = 0.0
    # Search root and dashboard folders
    for folder in [directory, directory / "dashboard", directory / "extraction_code"]:
        if not folder.exists(): continue
        for f in folder.glob("*.py"):
            mtime = f.stat().st_mtime
            if mtime > max_mtime:
                max_mtime = mtime
    return max_mtime

def hot_reloader(on_change_callback) -> None:
    """Watches for file changes and triggers a restart."""
    print("[ORCHESTRATOR] 🔄 Hot Reloader (CI/CD) is ACTIVE. Watching for code changes...", flush=True)
    last_mtime = get_last_mod_time(ROOT)
    
    while not _stop_event.is_set():
        time.sleep(2)  # Check every 2 seconds
        current_mtime = get_last_mod_time(ROOT)
        if current_mtime > last_mtime:
            print("\n[ORCHESTRATOR] 🚀 Code change detected! Triggering auto-reload...", flush=True)
            last_mtime = current_mtime
            on_change_callback()

def restart_all_services():
    """Stops all running processes and clears them for restart."""
    for name, proc in list(_processes.items()):
        try:
            print(f"[ORCHESTRATOR] Stopping {name} for reload...", flush=True)
            proc.terminate()
            # proc.wait(timeout=5)
        except: pass
    _processes.clear()

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    print("=" * 60, flush=True)
    print("   [ORCHESTRATOR] Mazara SCADA Monitor System Control", flush=True)
    print("=" * 60, flush=True)
    print(f"[*] Root Directory: {ROOT}", flush=True)
    print("------------------------------------------------------------", flush=True)

    # Start the hot reloader
    # reloader_thread = threading.Thread(
    #     target=hot_reloader, 
    #     args=(restart_all_services,), 
    #     daemon=True
    # )
    # reloader_thread.start()

    # Initial launch
    monitor_services()


if __name__ == "__main__":
    main()
