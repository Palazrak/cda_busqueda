import os
import time
import signal
import sys
import requests
from apscheduler.schedulers.background import BackgroundScheduler

EVERY_SECONDS = int(os.getenv("SCHEDULE_EVERY_SECONDS", "600"))  # 10 min por defecto
TRIGGER_URL = os.getenv("BACKEND_TRIGGER_URL", "http://backend:8000/run-scrapers")
TIMEOUT = int(os.getenv("TRIGGER_TIMEOUT_SECONDS", "180"))

def run_scrapers():
    """Dispara los scrapers a través del backend."""
    try:
        r = requests.post(TRIGGER_URL, timeout=TIMEOUT)
        print(f"[scheduler] {time.ctime()} -> POST {TRIGGER_URL} :: {r.status_code} :: {r.text[:200]}")
    except Exception as e:
        print(f"[scheduler] {time.ctime()} -> ERROR: {e}")

def graceful_exit(signum, frame):
    print(f"[scheduler] signal {signum} recibido, cerrando…")
    scheduler.shutdown(wait=False)
    sys.exit(0)

if __name__ == "__main__":
    print(f"[scheduler] iniciando… cada {EVERY_SECONDS}s -> {TRIGGER_URL}")
    scheduler = BackgroundScheduler()
    scheduler.add_job(run_scrapers, 'interval', seconds=EVERY_SECONDS)
    scheduler.start()

    signal.signal(signal.SIGTERM, graceful_exit)
    signal.signal(signal.SIGINT, graceful_exit)

    try:
        while True:
            time.sleep(2)  # mantener vivo el hilo principal
    except (KeyboardInterrupt, SystemExit):
        graceful_exit(signal.SIGINT, None)
