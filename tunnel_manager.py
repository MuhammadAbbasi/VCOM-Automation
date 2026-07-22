import os
import sys
import re
import time
import subprocess
import threading
import logging

logger = logging.getLogger(__name__)

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
CLOUDFLARED_EXE = os.path.join(ROOT_DIR, "cloudflared.exe")
PUBLIC_URL_FILE = os.path.join(ROOT_DIR, "public_url.txt")

_tunnel_process = None
_current_public_url = None

def kill_existing_tunnels():
    """Kill lingering tunnel processes (cloudflared, ngrok)."""
    try:
        if sys.platform == "win32":
            subprocess.run(["taskkill", "/F", "/IM", "cloudflared.exe"], capture_output=True)
            subprocess.run(["taskkill", "/F", "/IM", "ngrok.exe"], capture_output=True)
        else:
            subprocess.run(["pkill", "-9", "cloudflared"], capture_output=True)
            subprocess.run(["pkill", "-9", "ngrok"], capture_output=True)
    except Exception:
        pass

def get_public_url():
    """Return the active public tunnel URL (Cloudflare Tunnel)."""
    global _current_public_url
    if _current_public_url:
        return _current_public_url
    
    if os.path.exists(PUBLIC_URL_FILE):
        try:
            with open(PUBLIC_URL_FILE, "r", encoding="utf-8") as f:
                url = f.read().strip()
                if url and ("http://" in url or "https://" in url or "trycloudflare.com" in url):
                    _current_public_url = url
                    return url
        except Exception:
            pass

    return None

def start_cloudflare_tunnel(port=8080):
    """Start Cloudflare Tunnel background process and capture the public URL."""
    global _tunnel_process, _current_public_url

    if not os.path.exists(CLOUDFLARED_EXE):
        logger.error(f"[!] cloudflared.exe not found at {CLOUDFLARED_EXE}")
        return None

    token = None
    static_url = None
    try:
        from processor_watchdog_final import load_config
        cfg = load_config()
        token = cfg.get("CLOUDFLARE_TUNNEL_TOKEN")
        static_url = cfg.get("CLOUDFLARE_STATIC_URL")
    except Exception:
        pass

    if token and token.strip() and token != "YOUR_CLOUDFLARE_TOKEN_HERE":
        logger.info(f"[*] Cloudflare Named Tunnel token detected for {static_url or 'static domain'}")
        if static_url:
            _current_public_url = static_url
            try:
                with open(PUBLIC_URL_FILE, "w", encoding="utf-8") as f:
                    f.write(static_url)
            except Exception:
                pass
        return _current_public_url or "https://monitoraggioget.it"

    kill_existing_tunnels()
    time.sleep(1)

    try:
        if token and token.strip() and token != "YOUR_CLOUDFLARE_TOKEN_HERE":
            cmd = [CLOUDFLARED_EXE, "tunnel", "run", "--token", token.strip()]
            logger.info(f"[*] Starting Cloudflare Named Tunnel with static token...")
            if static_url:
                _current_public_url = static_url
                with open(PUBLIC_URL_FILE, "w", encoding="utf-8") as f:
                    f.write(static_url)
        else:
            cmd = [CLOUDFLARED_EXE, "tunnel", "--url", f"http://127.0.0.1:{port}"]
            logger.info(f"[*] Starting Cloudflare Quick Tunnel for port {port}...")

        _tunnel_process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="ignore",
            creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0
        )

        captured_url = static_url
        start_time = time.time()

        # Read initial output to grab the trycloudflare.com URL if using quick tunnel
        while time.time() - start_time < 12:
            line = _tunnel_process.stdout.readline()
            if not line:
                break
            
            match = re.search(r"https://[a-zA-Z0-9-]+\.trycloudflare\.com", line)
            if match:
                captured_url = match.group(0)
                _current_public_url = captured_url
                
                with open(PUBLIC_URL_FILE, "w", encoding="utf-8") as f:
                    f.write(captured_url)

                logger.info(f"\n{'='*60}\n[*] CLOUDFLARE TUNNEL ACTIVE (100% FREE - UNLIMITED BANDWIDTH)\n[*] Remote Access URL: {captured_url}\n{'='*60}\n")
                print(f"\n{'='*60}\n[*] CLOUDFLARE TUNNEL ACTIVE (100% FREE - UNLIMITED BANDWIDTH)\n[*] Remote Access URL: {captured_url}\n{'='*60}\n", flush=True)
                break

        # Background thread to keep reading stdout to prevent buffer deadlock
        def _stream_reader():
            while _tunnel_process and _tunnel_process.poll() is None:
                try:
                    line = _tunnel_process.stdout.readline()
                    if not line:
                        break
                except Exception:
                    break

        threading.Thread(target=_stream_reader, daemon=True).start()
        return captured_url or static_url

    except Exception as e:
        logger.error(f"[!] Failed to launch Cloudflare Tunnel: {e}")
        print(f"[!] Cloudflare Tunnel error: {e}", flush=True)
        return None

if __name__ == "__main__":
    url = start_cloudflare_tunnel(8080)
    if url:
        print(f"Tunnel successfully created: {url}")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            kill_existing_tunnels()
