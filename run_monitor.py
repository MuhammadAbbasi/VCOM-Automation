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

import platform
import os
import sys
from pathlib import Path

# Monkeypatch platform.machine and platform.uname to bypass socket import deadlock on Python 3.14
platform.machine = lambda: 'AMD64'

class UnameResult:
    system = 'Windows'
    node = 'localhost'
    release = '10'
    version = '10.0.19045'
    machine = 'AMD64'
    processor = 'Intel64 Family 6 Model 158 Stepping 10, GenuineIntel'
    def __getitem__(self, item):
        return [self.system, self.node, self.release, self.version, self.machine, self.processor][item]

platform.uname = lambda: UnameResult()

ROOT = Path(__file__).resolve().parent
if "PYTHONPATH" not in os.environ:
    os.environ["PYTHONPATH"] = str(ROOT)
else:
    os.environ["PYTHONPATH"] = f"{ROOT}{os.pathsep}{os.environ['PYTHONPATH']}"

import socket
import subprocess
import threading
import time
import signal

from dashboard_doctor import maybe_backup_database, run_doctor

import ctypes
from ctypes import wintypes
import psutil

# Handle for keeping the Job Object alive
_job_handle = None

def check_interactive_terminal() -> None:
    """Ensure the orchestrator only runs inside an interactive, user-defined terminal."""
    # 1. Block agent environment execution
    agent_envs = ["ANTIGRAVITY_AGENT", "ANTIGRAVITY_TRAJECTORY_ID", "AGENT_ID"]
    if any(env in os.environ for env in agent_envs):
        print("[ORCHESTRATOR] Error: Execution blocked. This script cannot be run by the AI agent.", file=sys.stderr, flush=True)
        sys.exit(1)

    # 2. Verify standard console window & TTY
    if not sys.stdout.isatty():
        print("[ORCHESTRATOR] Error: This script must be run from an interactive terminal.", file=sys.stderr, flush=True)
        sys.exit(1)
        
    if sys.platform == 'win32':
        import ctypes
        if ctypes.windll.kernel32.GetConsoleWindow() == 0:
            print("[ORCHESTRATOR] Error: No console window detected. Must run in an interactive terminal.", file=sys.stderr, flush=True)
            sys.exit(1)

        # 3. Verify parent/grandparent is an allowed shell/terminal
        try:
            import psutil
            allowed_shells = {"powershell.exe", "cmd.exe", "pwsh.exe", "bash.exe", "wsl.exe", "wt.exe", "conhost.exe"}
            p = psutil.Process(os.getpid())
            
            ancestor_names = []
            curr = p.parent()
            while curr:
                ancestor_names.append(curr.name().lower())
                curr = curr.parent()
                
            agent_processes = {"language_server_windows_x64.exe", "language_server", "agent"}
            if any(any(ap in name for ap in agent_processes) for name in ancestor_names):
                print("[ORCHESTRATOR] Error: Execution blocked. Detected agent runner in process tree.", file=sys.stderr, flush=True)
                sys.exit(1)
                
            parent = p.parent()
            if parent:
                parent_name = parent.name().lower()
                if parent_name == "py.exe":
                    grandparent = parent.parent()
                    if grandparent:
                        parent_name = grandparent.name().lower()
                
                if not any(shell in parent_name for shell in allowed_shells):
                    print(f"[ORCHESTRATOR] Error: Execution blocked. Parent process '{parent_name}' is not an allowed interactive shell.", file=sys.stderr, flush=True)
                    sys.exit(1)
        except Exception as e:
            print(f"[ORCHESTRATOR] Warning during terminal check: {e}", flush=True)

def setup_job_object():
    """Configure Windows Job Object to kill child processes when parent exits."""
    if sys.platform != 'win32':
        return
    try:
        # Windows Job Object Constants
        JobObjectExtendedLimitInformation = 9
        JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x2000

        class IO_COUNTERS(ctypes.Structure):
            _fields_ = [
                ('ReadOperationCount', ctypes.c_uint64),
                ('WriteOperationCount', ctypes.c_uint64),
                ('OtherOperationCount', ctypes.c_uint64),
                ('ReadTransferCount', ctypes.c_uint64),
                ('WriteTransferCount', ctypes.c_uint64),
                ('OtherTransferCount', ctypes.c_uint64),
            ]

        class JOBOBJECT_BASIC_LIMIT_INFORMATION(ctypes.Structure):
            _fields_ = [
                ('LimitFlags', wintypes.DWORD),
                ('MinimumWorkingSetSize', ctypes.c_size_t),
                ('MaximumWorkingSetSize', ctypes.c_size_t),
                ('ActiveProcessLimit', wintypes.DWORD),
                ('Affinity', ctypes.c_size_t),
                ('PriorityClass', wintypes.DWORD),
                ('SchedulingClass', wintypes.DWORD),
            ]

        class JOBOBJECT_EXTENDED_LIMIT_INFORMATION(ctypes.Structure):
            _fields_ = [
                ('BasicLimitInformation', JOBOBJECT_BASIC_LIMIT_INFORMATION),
                ('IoInfo', IO_COUNTERS),
                ('ProcessMemoryLimit', ctypes.c_size_t),
                ('JobMemoryLimit', ctypes.c_size_t),
                ('PeakProcessMemoryUsed', ctypes.c_size_t),
                ('PeakJobMemoryUsed', ctypes.c_size_t),
            ]

        kernel32 = ctypes.windll.kernel32
        
        # Create Job Object
        job = kernel32.CreateJobObjectW(None, None)
        if not job:
            print("[ORCHESTRATOR] Warning: Failed to create Job Object.", flush=True)
            return

        # Enable Kill on Job Close
        limits = JOBOBJECT_EXTENDED_LIMIT_INFORMATION()
        limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
        
        res = kernel32.SetInformationJobObject(
            job,
            JobObjectExtendedLimitInformation,
            ctypes.byref(limits),
            ctypes.sizeof(limits)
        )
        if not res:
            print("[ORCHESTRATOR] Warning: Failed to set Job Object information.", flush=True)
            return
            
        # Assign this process to the Job Object
        current_proc = kernel32.GetCurrentProcess()
        if not kernel32.AssignProcessToJobObject(job, current_proc):
            print("[ORCHESTRATOR] Warning: Failed to assign process to Job Object.", flush=True)
            return
            
        global _job_handle
        _job_handle = job
        print("[ORCHESTRATOR] Windows Job Object configured (automatic child process cleanup enabled).", flush=True)
    except Exception as e:
        print(f"[ORCHESTRATOR] Warning: Could not setup Job Object: {e}", flush=True)

def kill_previous_scada_processes() -> None:
    """Scan and kill all previous SCADA-related python processes."""
    import psutil
    current_pid = os.getpid()
    target_scripts = [
        "run_monitor.py",
        "dashboard/app.py",
        "dashboard\\app.py",
        "processor_watchdog_final.py",
        "vcom_monitor.py",
        "telegram_bot.py",
        "broker.py",
        "receiver.py",
        "odoo_ticket_engine.py"
    ]
    print("[ORCHESTRATOR] Cleaning up previous SCADA processes...", flush=True)
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            if proc.info['pid'] == current_pid:
                continue
            cmdline = proc.info['cmdline']
            if not cmdline:
                continue
            cmdline_str = " ".join(cmdline).lower()
            
            is_scada_proc = False
            # Check if it's a Python process running one of our target scripts
            if "python" in proc.info['name'].lower() or "python" in cmdline[0].lower():
                for script in target_scripts:
                    script_name = os.path.basename(script).lower()
                    if script_name in cmdline_str:
                        is_scada_proc = True
                        break
            
            if is_scada_proc:
                print(f"[ORCHESTRATOR] Terminating existing process PID {proc.info['pid']} ({cmdline_str})", flush=True)
                proc.kill()
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass

# Fix for Windows console encoding issues
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding='utf-8')  # type: ignore
    except:
        pass

ROOT = Path(__file__).resolve().parent
PID_FILE = ROOT / "run_monitor.pid"

# ---------------------------------------------------------------------------
# Single-instance lock — abort if another instance is already running
# ---------------------------------------------------------------------------
def _acquire_pid_lock() -> None:
    if PID_FILE.exists():
        try:
            existing_pid = int(PID_FILE.read_text().strip())
            import psutil
            if psutil.pid_exists(existing_pid):
                try:
                    proc = psutil.Process(existing_pid)
                    cmdline = proc.cmdline()
                    if cmdline:
                        cmdline_str = " ".join(cmdline).lower()
                        if "run_monitor.py" in cmdline_str:
                            print(f"[ORCHESTRATOR] Found previous instance (PID {existing_pid}). Terminating it...", flush=True)
                            proc.kill()
                            proc.wait(timeout=5)
                except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.TimeoutExpired):
                    pass # Stale lock or inaccessible process — treat as stale
        except Exception:
            pass  # Stale PID file format/read error — overwrite it
    PID_FILE.write_text(str(os.getpid()))

def _release_pid_lock() -> None:
    try:
        PID_FILE.unlink(missing_ok=True)
    except Exception:
        pass

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
        "name": "BROKER",
        "cmd": [sys.executable, "-u", str(ROOT / "tracker_testing" / "broker.py")],
        "new_console": False,
        "optional": True,
    },
    {
        "name": "TRACKER",
        "cmd": [sys.executable, "-u", str(ROOT / "tracker_testing" / "receiver.py")],
        "new_console": False,
        "optional": True,
    },
    {
        "name": "TICKETS",
        "cmd": [sys.executable, "-u", str(ROOT / "odoo_ticket_engine.py")],
        "new_console": False,
    },
]


# ---------------------------------------------------------------------------
# Log streaming thread (for services running in this terminal)
# ---------------------------------------------------------------------------

import re
ANSI_ESCAPE = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')

def stream_output(proc: subprocess.Popen, prefix: str) -> None:
    print(f"[ORCHESTRATOR] Logging stream for {prefix} started.", flush=True)
    try:
        if proc.stdout is not None:
            for line in proc.stdout:
                if line:
                    # Strip ANSI escape codes
                    clean_line = ANSI_ESCAPE.sub('', line)
                    # For \r (progress bars), only keep the last segment to avoid console mangling
                    segments = clean_line.split('\r')
                    final_line = segments[-1]
                    if final_line.strip():
                        # Ensure it ends with a newline to prevent thread interleaving
                        if not final_line.endswith('\n'):
                            final_line += '\n'
                        sys.stdout.write(f"[{prefix}] {final_line}")
                        sys.stdout.flush()
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


_stream_threads: list[threading.Thread] = []


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
        # Non-daemon so stdout is fully drained before the process exits
        t = threading.Thread(target=stream_output, args=(proc, name), daemon=False, name=f"stream-{name}")
        t.start()
        _stream_threads.append(t)
        print(f"[ORCHESTRATOR] Started {name} (pid={proc.pid})", flush=True)

    return proc


def doctor_scheduler(interval_seconds: int = 3600) -> None:
    """Run the dashboard doctor cycle inside the orchestrator every hour."""
    print("[ORCHESTRATOR] Starting doctor scheduler thread", flush=True)
    next_run = time.time()
    while not _stop_event.is_set():
        if time.time() >= next_run:
            try:
                maybe_backup_database()
                run_doctor()
            except Exception as exc:
                print(f"[ORCHESTRATOR] Doctor scheduler error: {exc}", flush=True)
            next_run = time.time() + interval_seconds
        time.sleep(5)


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
    # Launch all services in parallel so they start simultaneously
    service_procs: dict[str, subprocess.Popen] = {}
    launch_lock = threading.Lock()

    def _launch_one(svc: dict) -> None:
        try:
            proc = launch_service(svc)
            with launch_lock:
                service_procs[svc["name"]] = proc
        except Exception as exc:
            print(f"[ORCHESTRATOR] Failed to start {svc['name']}: {exc}", flush=True)

    launch_threads = [threading.Thread(target=_launch_one, args=(svc,), daemon=True) for svc in SERVICES]
    for t in launch_threads:
        t.start()
    for t in launch_threads:
        t.join(timeout=30)

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
    print("\n[ORCHESTRATOR] Shutting down all services...", flush=True)
    _stop_event.set()

    # Flush any pending snapshot writes before terminating the analysis process
    try:
        from db.snapshot_queue import flush_snapshot_queue
        print("[ORCHESTRATOR] Flushing snapshot queue...", flush=True)
        flush_snapshot_queue(timeout=15)
        print("[ORCHESTRATOR] Snapshot queue flushed.", flush=True)
    except Exception as exc:
        print(f"[ORCHESTRATOR] Snapshot flush warning: {exc}", flush=True)

    for name, proc in list(_processes.items()):
        try:
            proc.terminate()
            proc.wait(timeout=10)
            print(f"[ORCHESTRATOR] {name} stopped", flush=True)
        except Exception as e:
            print(f"[ORCHESTRATOR] Could not stop {name}: {e}", flush=True)

    # Wait for stdout drain threads so no BufferedWriter is torn from under them
    for t in _stream_threads:
        t.join(timeout=5)

    _release_pid_lock()


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
    # 1. Enforce interactive terminal execution
    check_interactive_terminal()

    # 2. Terminate any previous SCADA processes to release ports and PID locks
    kill_previous_scada_processes()

    # 3. Now acquire the single-instance lock
    _acquire_pid_lock()

    # 4. Configure job object for automatic child process cleanup on exit
    setup_job_object()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    print("=" * 60, flush=True)
    print("   [ORCHESTRATOR] Mazara SCADA Monitor System Control", flush=True)
    print("=" * 60, flush=True)
    print(f"[*] Root Directory: {ROOT}", flush=True)
    print("------------------------------------------------------------", flush=True)

    # Start the periodic doctor scheduler thread inside this orchestrator.
    doctor_thread = threading.Thread(
        target=doctor_scheduler,
        daemon=True,
        name="doctor-scheduler",
    )
    doctor_thread.start()

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
