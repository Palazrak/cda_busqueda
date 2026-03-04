"""
Worker Pool - Semáforo global de slots de ejecución.

Controla cuántos procesos de scraping corren simultáneamente.
Cada scraper reserva N slots (uno por shard). Thread-safe.
"""

import threading
import logging
from typing import Dict


class WorkerPool:
    def __init__(self, max_workers: int):
        if max_workers < 1:
            raise ValueError(f"max_workers debe ser >= 1 (recibido: {max_workers})")
        self._max: int = max_workers
        self._used: int = 0
        self._by_scraper: Dict[str, int] = {}
        self._lock = threading.Lock()
        self.logger = logging.getLogger("WorkerPool")
        self.logger.info(f"✅ WorkerPool: {max_workers} slots totales")

    def acquire(self, scraper_name: str, n_slots: int = 1) -> bool:
        """
        Intenta reservar n_slots para scraper_name. Atómico.
        Returns True si reservado, False si no hay slots suficientes.
        """
        with self._lock:
            if scraper_name in self._by_scraper:
                self.logger.warning(
                    f"⚠️  {scraper_name} ya tiene slots reservados — ignorando acquire"
                )
                return False
            available = self._max - self._used
            if available < n_slots:
                self.logger.debug(
                    f"🚫 {scraper_name}: necesita {n_slots}, "
                    f"disponibles {available} ({self._used}/{self._max})"
                )
                return False
            self._used += n_slots
            self._by_scraper[scraper_name] = n_slots
            self.logger.debug(
                f"✅ {scraper_name}: {n_slots} slot(s) adquiridos "
                f"[{self._used}/{self._max}]"
            )
            return True

    def release(self, scraper_name: str):
        """Libera todos los slots de scraper_name. No-op si no estaba registrado."""
        with self._lock:
            n = self._by_scraper.pop(scraper_name, 0)
            if n == 0:
                return
            self._used = max(0, self._used - n)
            self.logger.debug(
                f"🔓 {scraper_name}: {n} slot(s) liberados [{self._used}/{self._max}]"
            )

    def is_registered(self, scraper_name: str) -> bool:
        with self._lock:
            return scraper_name in self._by_scraper

    def slots_free(self) -> int:
        with self._lock:
            return self._max - self._used

    def get_status(self) -> dict:
        with self._lock:
            return {
                "slots_used": self._used,
                "slots_total": self._max,
                "slots_free": self._max - self._used,
                "by_scraper": dict(self._by_scraper),
            }

    def format_status_line(self) -> str:
        """Línea compacta para el dashboard: '[Slots: ██░░ 2/4] [amber_nacional(2)]'"""
        with self._lock:
            bar = "█" * self._used + "░" * (self._max - self._used)
            running = " ".join(f"{n}({s})" for n, s in self._by_scraper.items())
            base = f"[Slots: {bar} {self._used}/{self._max}]"
            return f"{base} [{running}]" if running else base