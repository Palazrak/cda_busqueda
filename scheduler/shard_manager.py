"""
Shard Manager - Divide el trabajo de un scraper en N shards paralelos.

Estrategias:
  "states"  → parte la lista de estados (para amber_nacional)
  "generic" → pasa --shard-index N --shard-count N al script (futuros scrapers)

Para n_shards=1 no se añaden args extra (retrocompatible).

Cálculo automático de shards:
    ratio = effective_duration / min_duration_among_all_scrapers
    n_shards = clamp(round(ratio), 1, max_shards)

Con los valores actuales:
    amber_chiapas:    60s / 60s = 1.0  → 1 shard
    havistoa_chiapas: 60s / 60s = 1.0  → 1 shard
    amber_nacional:  400s / 60s = 6.7  → min(6.7, max_shards=2) = 2 shards
"""

import math
import logging
from typing import List, Optional


# Lista completa de estados de amber_nacional
AMBER_NACIONAL_STATES: List[int] = [0] + list(range(2, 34))


class ShardManager:
    def __init__(self):
        self.logger = logging.getLogger("ShardManager")

    # ------------------------------------------------------------------
    # Cálculo de cantidad de shards
    # ------------------------------------------------------------------

    def resolve_shard_count(
        self,
        scraper_name: str,
        config_shards: Optional[int],
        effective_duration_sec: float,
        reference_duration_sec: float,
        max_shards: int = 2,
    ) -> int:
        """
        Determina cuántos shards usar para un scraper.

        Args:
            scraper_name:          Nombre del scraper.
            config_shards:         Valor explícito del config (None = auto).
            effective_duration_sec: Duración EMA actual del scraper.
            reference_duration_sec: Duración del scraper MÁS RÁPIDO (mínimo).
            max_shards:             Tope máximo de shards (del config).

        Returns:
            Número de shards >= 1.
        """
        if config_shards is not None:
            n = max(1, int(config_shards))
            self.logger.debug(f"🔀 {scraper_name}: {n} shards (config explícito)")
            return n

        # Auto: ratio respecto al scraper más rápido
        if reference_duration_sec <= 0:
            return 1
        ratio = effective_duration_sec / reference_duration_sec
        n = max(1, min(max_shards, round(ratio)))
        self.logger.info(
            f"🔀 {scraper_name}: {n} shards (auto) "
            f"[{effective_duration_sec:.0f}s / ref={reference_duration_sec:.0f}s"
            f" = {ratio:.2f} → clamped a max={max_shards}]"
        )
        return n

    # ------------------------------------------------------------------
    # Construcción de args por shard
    # ------------------------------------------------------------------

    def build_shard_args(
        self,
        scraper_name: str,
        shard_strategy: str,
        n_shards: int,
    ) -> List[List[str]]:
        """
        Construye la lista de args CLI para cada shard.

        Returns:
            Lista de n_shards listas de args. Cada una se añade al comando
            [python, script.py] al lanzar ese shard.
            Para n_shards=1 retorna [[]] (sin args extra).
        """
        if n_shards <= 1:
            return [[]]

        if shard_strategy == "states" and scraper_name == "amber_nacional":
            result = self._split_states(n_shards)
            self.logger.info(
                f"🔀 {scraper_name}: {len(result)} shards por estados → "
                + " | ".join(
                    (a[1][:40] + "..." if len(a[1]) > 40 else a[1])
                    for a in result
                )
            )
            return result

        # Estrategia genérica: --shard-index i --shard-count n
        result = [
            ["--shard-index", str(i), "--shard-count", str(n_shards)]
            for i in range(n_shards)
        ]
        self.logger.info(f"🔀 {scraper_name}: {n_shards} shards genéricos")
        return result

    # ------------------------------------------------------------------
    # Helpers internos
    # ------------------------------------------------------------------

    def _split_states(self, n_shards: int) -> List[List[str]]:
        """
        Divide AMBER_NACIONAL_STATES en n_shards grupos disjuntos y exhaustivos.

        Con 34 estados y 2 shards:
            shard 0: [0, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17]
            shard 1: [18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33]
        """
        states = AMBER_NACIONAL_STATES[:]
        chunk_size = math.ceil(len(states) / n_shards)
        chunks = [
            states[i: i + chunk_size]
            for i in range(0, len(states), chunk_size)
        ]
        # Filtrar chunks vacíos (puede pasar si n_shards > len(states))
        return [
            ["--states", ",".join(str(s) for s in chunk)]
            for chunk in chunks
            if chunk
        ]