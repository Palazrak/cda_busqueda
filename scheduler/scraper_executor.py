"""
Scraper Executor - Gestión de ejecución multi-shard de scrapers.

Cambios respecto a la versión anterior:
- active_processes ahora almacena LISTAS de shards por scraper
- execute_shards() lanza N procesos en paralelo para un mismo scraper
- cleanup_finished() retorna lista de (nombre, duración_total, éxito)
  cuando TODOS los shards de un scraper terminaron
- is_stuck() y kill_scraper() operan sobre todos los shards del scraper
- kill_scraper() NO llama a cleanup_finished() internamente (lo hace el caller)
"""

import subprocess
import logging
import time
import sys
import os
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False


class ScraperExecutor:
    """
    Gestor de ejecución de scrapers con soporte multi-shard.

    Estructura de active_processes:
    {
        "amber_nacional": {
            "shards": [
                {"process": Popen, "shard_id": 0, "pid": 123,
                 "extra_args": ["--states", "0,2,3,..."], "start_time": 1234.0},
                {"process": Popen, "shard_id": 1, "pid": 124,
                 "extra_args": ["--states", "17,18,..."], "start_time": 1234.1},
            ],
            "start_time": 1234.0,        # tiempo del primer shard
            "script_filename": "paralelo_amber_nacional.py",
            "n_shards": 2,
        }
    }
    """

    def __init__(
        self,
        scripts_dir: str = "/app/scripts/paralelizado",
        timeout_sec: Optional[int] = 900,
    ):
        self.scripts_dir = Path(scripts_dir)
        self.timeout_sec = timeout_sec
        self.logger = logging.getLogger("ScraperExecutor")

        # Procesos activos por scraper
        self.active_processes: Dict[str, Dict[str, Any]] = {}

        # Estadísticas de ejecución (compatibilidad con scheduler.py existente)
        self.execution_stats: Dict[str, Dict[str, Any]] = {}

        if not self.scripts_dir.exists():
            self.logger.warning(
                f"⚠️  Directorio de scripts no existe: {self.scripts_dir}"
            )

        self.logger.info(
            f"✅ ScraperExecutor inicializado "
            f"(scripts_dir={scripts_dir}, timeout={timeout_sec}s)"
        )

    # ------------------------------------------------------------------
    # Lanzamiento de procesos
    # ------------------------------------------------------------------

    def execute_shards(
        self,
        scraper_name: str,
        script_filename: str,
        shard_args_list: List[List[str]],
        env_vars: Optional[Dict[str, str]] = None,
    ) -> bool:
        """
        Lanza N shards del scraper como subprocesos independientes.

        Args:
            scraper_name:    Nombre identificador del scraper.
            script_filename: Archivo Python a ejecutar.
            shard_args_list: Lista de listas de args, una por shard.
                             [[]] para scraper sin sharding.
            env_vars:        Variables de entorno adicionales.

        Returns:
            True si todos los shards se lanzaron correctamente.
            False si el scraper ya está corriendo o hubo error.
        """
        if self.is_running(scraper_name):
            self.logger.warning(f"⏭️  {scraper_name} ya está en ejecución, skip")
            return False

        script_path = self.scripts_dir / script_filename
        if not script_path.exists():
            self.logger.error(f"❌ Script no encontrado: {script_path}")
            raise FileNotFoundError(f"Script no existe: {script_path}")

        process_env = os.environ.copy()
        if env_vars:
            process_env.update(env_vars)

        n_shards = len(shard_args_list)
        launched_shards = []
        overall_start = time.time()

        for shard_id, extra_args in enumerate(shard_args_list):
            cmd = [sys.executable, str(script_path)] + extra_args
            try:
                process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    cwd=str(self.scripts_dir.parent),
                    env=process_env,
                    text=True,
                    bufsize=1,
                )
                shard_info = {
                    "process": process,
                    "shard_id": shard_id,
                    "pid": process.pid,
                    "extra_args": extra_args,
                    "start_time": time.time(),
                }
                launched_shards.append(shard_info)
                self.logger.info(
                    f"🚀 {scraper_name} shard {shard_id}/{n_shards - 1} "
                    f"lanzado (PID={process.pid}) "
                    f"args={extra_args if extra_args else '(ninguno)'}"
                )
            except Exception as e:
                self.logger.error(
                    f"❌ Error lanzando shard {shard_id} de {scraper_name}: {e}",
                    exc_info=True,
                )
                # Matar shards ya lanzados si este falla
                for sh in launched_shards:
                    try:
                        sh["process"].kill()
                    except Exception:
                        pass
                return False

        # Registrar como activo
        self.active_processes[scraper_name] = {
            "shards": launched_shards,
            "start_time": overall_start,
            "start_datetime": datetime.now(),
            "script_filename": script_filename,
            "n_shards": n_shards,
        }

        # Inicializar stats si primera ejecución
        if scraper_name not in self.execution_stats:
            self.execution_stats[scraper_name] = {
                "total_runs": 0,
                "successful_runs": 0,
                "failed_runs": 0,
                "total_duration_sec": 0.0,
                "last_start": None,
                "last_duration": None,
                "last_exit_code": None,
                "avg_duration": None,
            }
        self.execution_stats[scraper_name]["total_runs"] += 1
        self.execution_stats[scraper_name]["last_start"] = datetime.now()

        if n_shards > 1:
            self.logger.info(
                f"✅ {scraper_name}: {n_shards} shards corriendo en paralelo"
            )
        return True

    def execute(
        self,
        scraper_name: str,
        script_filename: str,
        env_vars: Optional[Dict[str, str]] = None,
    ) -> bool:
        """
        Retrocompatibilidad: ejecuta un scraper como shard único.
        Delega a execute_shards con shard_args_list=[[]].
        """
        return self.execute_shards(
            scraper_name=scraper_name,
            script_filename=script_filename,
            shard_args_list=[[]],
            env_vars=env_vars,
        )

    # ------------------------------------------------------------------
    # Consultas de estado
    # ------------------------------------------------------------------

    def is_running(self, scraper_name: str) -> bool:
        """
        True si el scraper tiene al menos un shard vivo.
        Un shard está vivo si process.poll() es None.
        """
        if scraper_name not in self.active_processes:
            return False
        shards = self.active_processes[scraper_name]["shards"]
        return any(sh["process"].poll() is None for sh in shards)

    def is_stuck(self, scraper_name: str) -> bool:
        """
        True si el scraper lleva más tiempo que timeout_sec corriendo.
        Se mide desde el start_time del primer shard.
        """
        if not self.is_running(scraper_name):
            return False
        if self.timeout_sec is None:
            return False
        elapsed = time.time() - self.active_processes[scraper_name]["start_time"]
        return elapsed > self.timeout_sec

    def get_uptime(self, scraper_name: str) -> Optional[float]:
        """Segundos desde que el scraper fue lanzado. None si no corre."""
        if scraper_name not in self.active_processes:
            return None
        return time.time() - self.active_processes[scraper_name]["start_time"]

    def get_active_scrapers(self) -> List[str]:
        """Lista de scrapers con al menos un shard vivo."""
        return [n for n in list(self.active_processes.keys()) if self.is_running(n)]

    # ------------------------------------------------------------------
    # Cleanup y kill
    # ------------------------------------------------------------------

    def cleanup_finished(self) -> List[Tuple[str, float, bool]]:
        """
        Detecta scrapers cuyos TODOS los shards terminaron.
        Actualiza estadísticas internas y los remueve de active_processes.

        Returns:
            Lista de (scraper_name, total_duration_sec, success) para
            cada scraper que acaba de completarse.
            success=True si TODOS los shards terminaron con exit_code=0.
        """
        completed = []
        to_remove = []

        for scraper_name, proc_info in self.active_processes.items():
            shards = proc_info["shards"]

            # ¿Terminaron TODOS los shards?
            exit_codes = [sh["process"].poll() for sh in shards]
            if any(code is None for code in exit_codes):
                continue  # Al menos uno sigue corriendo

            # Todos los shards terminaron
            to_remove.append(scraper_name)
            duration = time.time() - proc_info["start_time"]
            success = all(code == 0 for code in exit_codes)

            # Log por shard
            for sh, code in zip(shards, exit_codes):
                if code == 0:
                    self.logger.info(
                        f"✅ {scraper_name} shard {sh['shard_id']}: "
                        f"exit=0 (duración total estimada: {duration:.1f}s)"
                    )
                else:
                    self.logger.error(
                        f"❌ {scraper_name} shard {sh['shard_id']}: "
                        f"exit={code}"
                    )
                    # Capturar stderr del shard fallido
                    try:
                        _, stderr = sh["process"].communicate(timeout=1)
                        if stderr:
                            self.logger.error(
                                f"   stderr: {stderr[-400:]}"
                            )
                    except subprocess.TimeoutExpired:
                        pass

            # Actualizar execution_stats de compatibilidad
            stats = self.execution_stats.get(scraper_name, {})
            stats["last_duration"] = duration
            stats["last_exit_code"] = 0 if success else 1
            stats["total_duration_sec"] = stats.get("total_duration_sec", 0) + duration
            if stats["total_runs"]:
                stats["avg_duration"] = (
                    stats["total_duration_sec"] / stats["total_runs"]
                )
            if success:
                stats["successful_runs"] = stats.get("successful_runs", 0) + 1
            else:
                stats["failed_runs"] = stats.get("failed_runs", 0) + 1
            self.execution_stats[scraper_name] = stats

            if PSUTIL_AVAILABLE:
                self._log_resource_usage(scraper_name)

            completed.append((scraper_name, duration, success))

        for name in to_remove:
            del self.active_processes[name]

        return completed

    def kill_scraper(self, scraper_name: str, force: bool = False) -> bool:
        """
        Mata todos los shards de un scraper.
        NO llama a cleanup_finished() — el caller es responsable.

        Returns:
            True si había procesos que matar, False si no estaba corriendo.
        """
        if scraper_name not in self.active_processes:
            self.logger.warning(f"⚠️  kill_scraper: {scraper_name} no está activo")
            return False

        shards = self.active_processes[scraper_name]["shards"]
        signal_name = "SIGKILL" if force else "SIGTERM"
        self.logger.warning(
            f"🔪 Matando {scraper_name} ({len(shards)} shard(s), {signal_name})"
        )

        for sh in shards:
            proc = sh["process"]
            if proc.poll() is not None:
                continue  # ya terminó
            try:
                if force:
                    proc.kill()
                else:
                    proc.terminate()
            except Exception as e:
                self.logger.error(f"   Error matando PID {sh['pid']}: {e}")

        # Esperar hasta 5s a que terminen
        deadline = time.time() + 5
        for sh in shards:
            remaining = deadline - time.time()
            if remaining <= 0:
                break
            try:
                sh["process"].wait(timeout=max(0.1, remaining))
            except subprocess.TimeoutExpired:
                if not force:
                    try:
                        sh["process"].kill()
                    except Exception:
                        pass

        # Remover de active_processes para que cleanup no lo procese de nuevo
        del self.active_processes[scraper_name]
        return True

    def kill_all(self, force: bool = False) -> int:
        active = list(self.active_processes.keys())
        count = 0
        for name in active:
            if self.kill_scraper(name, force):
                count += 1
        return count

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _log_resource_usage(self, scraper_name: str):
        if not PSUTIL_AVAILABLE:
            return
        try:
            cpu = psutil.cpu_percent(interval=0.1)
            mem = psutil.virtual_memory()
            self.logger.info(
                f"📊 {scraper_name}: CPU={cpu:.1f}% "
                f"RAM={mem.percent:.1f}% ({mem.used / (1024**3):.2f}GB)"
            )
        except Exception:
            pass

    def get_stats(self, scraper_name: str) -> Optional[Dict[str, Any]]:
        return self.execution_stats.get(scraper_name)

    def get_all_stats(self) -> Dict[str, Dict[str, Any]]:
        return self.execution_stats.copy()

    def print_status(self):
        print("\n" + "=" * 70)
        print("ESTADO DE SCRAPERS")
        print("=" * 70)
        active = self.get_active_scrapers()
        if active:
            print(f"\n🔄 Scrapers Activos ({len(active)}):")
            for name in active:
                info = self.active_processes[name]
                uptime = self.get_uptime(name)
                shards = info["shards"]
                alive = sum(1 for sh in shards if sh["process"].poll() is None)
                print(
                    f"  • {name}: {alive}/{info['n_shards']} shards vivos, "
                    f"uptime={uptime:.0f}s"
                )
                if self.is_stuck(name):
                    print(f"    ⚠️  POSIBLE TIMEOUT (>{self.timeout_sec}s)")
        else:
            print("\n✅ No hay scrapers en ejecución")
        print("=" * 70 + "\n")

    def __repr__(self) -> str:
        active = len(self.get_active_scrapers())
        total = sum(s.get("total_runs", 0) for s in self.execution_stats.values())
        return (
            f"ScraperExecutor(active={active}, total_runs={total}, "
            f"scripts_dir={self.scripts_dir})"
        )