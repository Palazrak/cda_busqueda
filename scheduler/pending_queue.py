"""
Pending Queue - Cola priorizada para scrapers en espera de slots.

Un scraper solo puede aparecer una vez (enqueue idempotente).
Thread-safe.
"""

import threading
import logging
import time
from dataclasses import dataclass
from typing import Callable, List, Optional


@dataclass
class QueuedScraper:
    name: str
    enqueued_at: float


class PendingQueue:
    def __init__(self):
        self._queue: List[QueuedScraper] = []
        self._in_queue: set = set()
        self._lock = threading.Lock()
        self.logger = logging.getLogger("PendingQueue")

    def enqueue(self, scraper_name: str, enqueued_at: Optional[float] = None) -> bool:
        """
        Añade a la cola. Idempotente: si ya está, no hace nada.
        Returns True si fue añadido, False si ya estaba.
        """
        with self._lock:
            if scraper_name in self._in_queue:
                self.logger.debug(f"📋 {scraper_name} ya está en cola, ignorando")
                return False
            item = QueuedScraper(
                name=scraper_name,
                enqueued_at=time.time() if enqueued_at is None else float(enqueued_at),
            )
            self._queue.append(item)
            self._in_queue.add(scraper_name)
            queue_snapshot = [queued.name for queued in self._queue]
            self.logger.info(
                f"📋 Encolado: {scraper_name} "
                f"(cola actual: {queue_snapshot})"
            )
            return True

    def dequeue_one(self) -> Optional[str]:
        """Extrae y retorna el primer elemento (FIFO). None si vacía."""
        with self._lock:
            if not self._queue:
                return None
            item = self._queue.pop(0)
            name = item.name
            self._in_queue.discard(name)
            return name

    def dequeue_best(
        self,
        free_slots: int,
        required_slots: Callable[[str], Optional[int]],
        score_func: Callable[[str, float], float],
        now: Optional[float] = None,
    ) -> Optional[str]:
        """
        Extrae el scraper con mejor score que quepa en los slots libres.

        required_slots puede retornar None para mantener un item bloqueado
        en cola, por ejemplo si el scraper sigue corriendo.
        """
        with self._lock:
            if not self._queue:
                return None

            now_ts = time.time() if now is None else float(now)
            best_index = None
            best_key = None

            for index, item in enumerate(self._queue):
                slots = required_slots(item.name)
                if slots is None or slots > free_slots:
                    continue
                age_sec = max(0.0, now_ts - item.enqueued_at)
                score = score_func(item.name, age_sec)
                # Mayor score primero; en empate gana el item más viejo.
                candidate_key = (score, age_sec, -index)
                if best_key is None or candidate_key > best_key:
                    best_key = candidate_key
                    best_index = index

            if best_index is None:
                return None

            item = self._queue.pop(best_index)
            self._in_queue.discard(item.name)
            return item.name

    def peek_front(self) -> Optional[str]:
        """Retorna el primer elemento sin extraerlo. None si vacía."""
        with self._lock:
            return self._queue[0].name if self._queue else None

    def peek_all(self) -> List[str]:
        """Snapshot de la cola sin modificarla."""
        with self._lock:
            return [item.name for item in self._queue]

    def contains(self, scraper_name: str) -> bool:
        with self._lock:
            return scraper_name in self._in_queue

    def size(self) -> int:
        with self._lock:
            return len(self._queue)

    def remove(self, scraper_name: str) -> bool:
        """Elimina una entrada específica (p.ej. al desactivar un scraper)."""
        with self._lock:
            if scraper_name not in self._in_queue:
                return False
            before = len(self._queue)
            self._queue = [item for item in self._queue if item.name != scraper_name]
            self._in_queue.discard(scraper_name)
            return len(self._queue) != before
