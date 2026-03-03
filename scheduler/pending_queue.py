"""
Pending Queue - Cola FIFO para scrapers en espera de slots.

Un scraper solo puede aparecer una vez (enqueue idempotente).
Thread-safe.
"""

import threading
import logging
from collections import deque
from typing import List, Optional

class PendingQueue:
    def __init__(self):
        self._queue: deque = deque()
        self._in_queue: set = set()
        self._lock = threading.Lock()
        self.logger = logging.getLogger("PendingQueue")

    def enqueue(self, scraper_name: str) -> bool:
        """
        Añade al final de la cola. Idempotente: si ya está, no hace nada.
        Returns True si fue añadido, False si ya estaba.
        """
        with self._lock:
            if scraper_name in self._in_queue:
                self.logger.debug(f"📋 {scraper_name} ya está en cola, ignorando")
                return False
            self._queue.append(scraper_name)
            self._in_queue.add(scraper_name)
            self.logger.info(
                f"📋 Encolado: {scraper_name} "
                f"(cola actual: {list(self._queue)})"
            )
            return True

    def dequeue_one(self) -> Optional[str]:
        """Extrae y retorna el primer elemento (FIFO). None si vacía."""
        with self._lock:
            if not self._queue:
                return None
            name = self._queue.popleft()
            self._in_queue.discard(name)
            return name

    def peek_front(self) -> Optional[str]:
        """Retorna el primer elemento sin extraerlo. None si vacía."""
        with self._lock:
            return self._queue[0] if self._queue else None

    def peek_all(self) -> List[str]:
        """Snapshot de la cola sin modificarla."""
        with self._lock:
            return list(self._queue)

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
            try:
                self._queue.remove(scraper_name)
                self._in_queue.discard(scraper_name)
                return True
            except ValueError:
                self._in_queue.discard(scraper_name)
                return False