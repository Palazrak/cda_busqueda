import os
import time
import signal
import sys
import requests
# EDIT: Changed to BlockingScheduler for more robust execution in a container
from apscheduler.schedulers.blocking import BlockingScheduler

EVERY_SECONDS = int(os.getenv("SCHEDULE_EVERY_SECONDS", "600"))
TRIGGER_URL = os.getenv("BACKEND_TRIGGER_URL", "http://backend:8000/run-scrapers")
TIMEOUT = int(os.getenv("TRIGGER_TIMEOUT_SECONDS", "180"))

def run_scrapers():
    """Dispara los scrapers a través del backend."""
    try:
        print(f"[scheduler] {time.ctime()} -> Disparando POST a {TRIGGER_URL}") # EDIT: Added log to show it's trying
        r = requests.post(TRIGGER_URL, timeout=TIMEOUT)
        print(f"[scheduler] {time.ctime()} -> RESPUESTA :: {r.status_code} :: {r.text[:200]}")
    except Exception as e:
        print(f"[scheduler] {time.ctime()} -> ERROR: {e}")

if __name__ == "__main__":
    print(f"[scheduler] iniciando… cada {EVERY_SECONDS}s -> {TRIGGER_URL}")
    # EDIT: Using BlockingScheduler now
    scheduler = BlockingScheduler()
    scheduler.add_job(run_scrapers, 'interval', seconds=EVERY_SECONDS)

    print("[scheduler] El scheduler está corriendo. Presiona Ctrl+C para salir.")
    try:
        # EDIT: The scheduler now runs in the main thread, no need for a while loop
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        print("[scheduler] Cerrando scheduler.")
        pass
