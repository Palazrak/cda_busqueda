"""
Stats Tracker - Estadísticas por scraper con EMA de duración.

La duración efectiva se actualiza tras cada run exitoso usando
Exponential Moving Average:
    ema = α * last_duration + (1 - α) * prev_ema    (α = 0.3)

Esto permite que el scheduler adapte intervalos y shards a la
velocidad real del scraper a lo largo del tiempo.
"""

import threading
import logging
from typing import Dict, List, Optional
from datetime import datetime


EMA_ALPHA = 0.3       # Peso de la observación más reciente (0.3 = moderado)
MAX_HISTORY = 10      # Historial de duraciones brutas a conservar


class ScraperStats:
    def __init__(self, name: str, initial_duration_sec: float):
        self.name = name
        # Contadores
        self.total_runs: int = 0
        self.successful_runs: int = 0
        self.failed_runs: int = 0
        self.stuck_kills: int = 0
        self.skip_already_running: int = 0
        self.skip_slots_full: int = 0
        # Duración adaptativa
        self.ema_duration: float = initial_duration_sec   # seed = valor del config
        self.duration_history: List[float] = []
        self.last_duration: Optional[float] = None
        self.last_run_at: Optional[datetime] = None
        self.last_success: Optional[bool] = None

    def record_run(self, duration_sec: float, success: bool):
        self.total_runs += 1
        self.last_run_at = datetime.now()
        self.last_duration = duration_sec
        self.last_success = success
        if success:
            self.successful_runs += 1
            # Solo actualizar EMA en runs exitosos para no inflar la estimación
            # con tiempos de procesos que fueron matados
            self.ema_duration = (
                EMA_ALPHA * duration_sec + (1 - EMA_ALPHA) * self.ema_duration
            )
        else:
            self.failed_runs += 1
        # Historial bruto (todos los runs)
        self.duration_history.append(duration_sec)
        if len(self.duration_history) > MAX_HISTORY:
            self.duration_history.pop(0)

    def record_skip(self, reason: str):
        if reason == "already_running":
            self.skip_already_running += 1
        elif reason == "slots_full":
            self.skip_slots_full += 1

    def record_stuck(self):
        self.stuck_kills += 1
        self.total_runs += 1
        self.failed_runs += 1

    @property
    def effective_duration(self) -> float:
        """Duración estimada actual basada en EMA."""
        return self.ema_duration

    def summary_line(self) -> str:
        total_skips = self.skip_already_running + self.skip_slots_full
        last_dur = f"{self.last_duration:.0f}s" if self.last_duration else "—"
        return (
            f"{self.name}: "
            f"runs={self.successful_runs}/{self.total_runs} "
            f"stuck={self.stuck_kills} "
            f"ema={self.ema_duration:.0f}s "
            f"last={last_dur} "
            f"skips={total_skips}"
            f"(busy={self.skip_already_running}/slots={self.skip_slots_full})"
        )


class StatsTracker:
    def __init__(self):
        self._lock = threading.Lock()
        self._stats: Dict[str, ScraperStats] = {}
        self.logger = logging.getLogger("StatsTracker")

    def register(self, name: str, initial_duration_sec: float):
        """Registra un scraper con su duración semilla del config."""
        with self._lock:
            if name not in self._stats:
                self._stats[name] = ScraperStats(name, initial_duration_sec)
                self.logger.debug(
                    f"📊 Registrado {name} (seed={initial_duration_sec:.0f}s)"
                )

    def record_run(self, name: str, duration_sec: float, success: bool):
        with self._lock:
            if name not in self._stats:
                return
            self._stats[name].record_run(duration_sec, success)
            new_ema = self._stats[name].ema_duration
            self.logger.info(
                f"📊 {name}: {'✅' if success else '❌'} "
                f"dur={duration_sec:.1f}s → ema={new_ema:.1f}s"
            )

    def record_skip(self, name: str, reason: str):
        with self._lock:
            if name in self._stats:
                self._stats[name].record_skip(reason)

    def record_stuck(self, name: str):
        with self._lock:
            if name in self._stats:
                self._stats[name].record_stuck()

    def get_effective_duration(self, name: str) -> Optional[float]:
        """Retorna la duración EMA actual, o None si no registrado."""
        with self._lock:
            s = self._stats.get(name)
            return s.effective_duration if s else None

    def get_all_durations(self) -> Dict[str, float]:
        """Diccionario {nombre: ema_duration} para todos los scrapers."""
        with self._lock:
            return {n: s.effective_duration for n, s in self._stats.items()}

    def log_summary(self):
        """Imprime resumen completo en logs."""
        with self._lock:
            self.logger.info("─" * 65)
            self.logger.info("📊 RESUMEN DE SCRAPERS")
            for s in self._stats.values():
                self.logger.info(f"  {s.summary_line()}")
            self.logger.info("─" * 65)