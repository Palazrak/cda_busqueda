"""
Scheduler Principal - Orquestador de Scrapers con Worker Pool y Cola Pendiente.

Flujo por trigger de APScheduler:
  1. ¿El scraper ya corre? → enqueue (FIFO), skip
  2. Calcular n_shards según duración EMA y ratio respecto al más rápido
  3. ¿Hay suficientes slots? → si no, enqueue, skip
  4. Adquirir slots, lanzar shards, retornar (non-blocking)

Flujo del cleanup job (cada cleanup_interval_sec):
  1. Matar scrapers stuck → liberar slots → registrar fallo
  2. Detectar scrapers completados → liberar slots → actualizar EMA → reprogramar
  3. Drenar cola FIFO (estricto): lanzar pendientes si hay slots
  4. Imprimir dashboard de estado
"""

import logging
import signal
import sys
import time
from datetime import datetime

from scheduler_config import SchedulerConfig
from jitter_calculator import calcular_intervalo_con_jitter, validar_config_jitter
from scraper_executor import ScraperExecutor
from worker_pool import WorkerPool
from pending_queue import PendingQueue
from stats_tracker import StatsTracker
from shard_manager import ShardManager

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR


# ──────────────────────────────────────────────────────────
# Setup de logging
# ──────────────────────────────────────────────────────────

def setup_logging(log_level: str = "INFO"):
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)
    logging.basicConfig(
        level=numeric_level,
        format='[%(asctime)s] [%(name)s] %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )
    logging.getLogger('apscheduler').setLevel(logging.WARNING)


# ──────────────────────────────────────────────────────────
# Instancias globales (inicializadas en initialize_scheduler)
# ──────────────────────────────────────────────────────────

config: SchedulerConfig = None
executor: ScraperExecutor = None
scheduler: BlockingScheduler = None
worker_pool: WorkerPool = None
pending_queue: PendingQueue = None
stats_tracker: StatsTracker = None
shard_manager: ShardManager = None

# Contador para loggear dashboard periódico aunque no haya cambios
_cleanup_call_count: int = 0
DASHBOARD_EVERY_N_CLEANUPS: int = 5


# ──────────────────────────────────────────────────────────
# Funciones de scheduling
# ──────────────────────────────────────────────────────────

def _get_reference_duration() -> float:
    """
    Duración de referencia = mínima duración EMA entre scrapers habilitados.
    Usada para calcular el ratio de shards en modo auto.
    """
    durations = stats_tracker.get_all_durations()
    if not durations:
        return 60.0
    return max(1.0, min(durations.values()))


def _calculate_shards_for(scraper_name: str) -> int:
    """Calcula cuántos shards necesita este scraper ahora."""
    scraper_cfg = config.get_scraper_config(scraper_name) or {}
    shard_cfg = config.get_shard_config(scraper_name)
    effective_dur = (
        stats_tracker.get_effective_duration(scraper_name)
        or scraper_cfg.get('estimated_duration_sec', 60)
    )
    ref_dur = _get_reference_duration()
    return shard_manager.resolve_shard_count(
        scraper_name=scraper_name,
        config_shards=shard_cfg['shards'],
        effective_duration_sec=effective_dur,
        reference_duration_sec=ref_dur,
        max_shards=shard_cfg['max_shards'],
    )


def execute_scraper(scraper_name: str):
    """
    Punto de entrada para el trigger de APScheduler.

    Si el scraper está corriendo o no hay slots: encola y retorna.
    Si hay slots: adquiere, lanza shards, retorna sin bloquear.
    """
    logger = logging.getLogger("Scheduler")

    # ── Guardia 1: ya está corriendo ────────────────────────────────
    if executor.is_running(scraper_name):
        pending_queue.enqueue(scraper_name)
        stats_tracker.record_skip(scraper_name, "already_running")
        logger.info(
            f"⏭️  {scraper_name} en ejecución → encolado "
            f"(cola: {pending_queue.peek_all()})"
        )
        return

    # ── Guardia 2: deshabilitado ─────────────────────────────────────
    scraper_cfg = config.get_scraper_config(scraper_name)
    if not scraper_cfg or not scraper_cfg.get('enabled', True):
        logger.info(f"⏸️  {scraper_name} deshabilitado, skip")
        return

    # ── Calcular shards ───────────────────────────────────────────────
    n_shards = _calculate_shards_for(scraper_name)
    shard_cfg = config.get_shard_config(scraper_name)

    # ── Guardia 3: slots disponibles ─────────────────────────────────
    if not worker_pool.acquire(scraper_name, n_shards):
        pending_queue.enqueue(scraper_name)
        stats_tracker.record_skip(scraper_name, "slots_full")
        logger.info(
            f"🚫 {scraper_name}: necesita {n_shards} slot(s), "
            f"no disponibles → encolado | "
            f"{worker_pool.format_status_line()} | "
            f"cola: {pending_queue.peek_all()}"
        )
        return

    # ── Lanzar shards ─────────────────────────────────────────────────
    shard_args_list = shard_manager.build_shard_args(
        scraper_name=scraper_name,
        shard_strategy=shard_cfg['shard_strategy'],
        n_shards=n_shards,
    )

    success = executor.execute_shards(
        scraper_name=scraper_name,
        script_filename=scraper_cfg['script_filename'],
        shard_args_list=shard_args_list,
    )

    if not success:
        # Fallo al lanzar: liberar slots inmediatamente
        worker_pool.release(scraper_name)
        logger.error(f"❌ {scraper_name}: fallo al lanzar procesos")
        return

    logger.info(
        f"🚀 {scraper_name} lanzado ({n_shards} shard(s)) | "
        f"{worker_pool.format_status_line()}"
    )


def _launch_from_queue(scraper_name: str):
    """
    Versión interna de execute_scraper para uso desde la cola pendiente.
    Asume que la disponibilidad de slots ya fue verificada externamente.
    No re-encola en caso de fallo (evita loops).
    """
    logger = logging.getLogger("Scheduler")

    scraper_cfg = config.get_scraper_config(scraper_name)
    if not scraper_cfg or not scraper_cfg.get('enabled', True):
        logger.info(f"⏸️  {scraper_name} (cola) deshabilitado al intentar lanzar")
        return

    n_shards = _calculate_shards_for(scraper_name)
    shard_cfg = config.get_shard_config(scraper_name)

    if not worker_pool.acquire(scraper_name, n_shards):
        # Slots ocupados (race condition entre threads); re-encolar para el próximo ciclo
        pending_queue.enqueue(scraper_name)
        logger.warning(
            f"⚠️  {scraper_name} (cola): race condition en slots → re-encolado"
        )
        return

    shard_args_list = shard_manager.build_shard_args(
        scraper_name=scraper_name,
        shard_strategy=shard_cfg['shard_strategy'],
        n_shards=n_shards,
    )
    success = executor.execute_shards(
        scraper_name=scraper_name,
        script_filename=scraper_cfg['script_filename'],
        shard_args_list=shard_args_list,
    )
    if not success:
        worker_pool.release(scraper_name)
        logger.error(f"❌ {scraper_name} (cola): fallo al lanzar procesos")
        return

    logger.info(
        f"▶️  {scraper_name} ejecutado desde cola pendiente ({n_shards} shard(s)) | "
        f"{worker_pool.format_status_line()}"
    )


def _try_drain_queue():
    """
    Intenta ejecutar items de la cola pendiente en orden FIFO estricto.

    Solo avanza al siguiente si el frente de la cola tiene slots disponibles.
    Esto preserva el FIFO: si el primero no cabe, los demás esperan.
    """
    logger = logging.getLogger("Scheduler")

    while pending_queue.size() > 0:
        front = pending_queue.peek_front()
        if front is None:
            break

        # Verificar si ya está corriendo (puede haber cambiado desde que se encoló)
        if executor.is_running(front):
            # Sigue corriendo; dejar en cola para el próximo ciclo
            break

        n_shards = _calculate_shards_for(front)
        if worker_pool.slots_free() < n_shards:
            logger.debug(
                f"📋 Cola: {front} necesita {n_shards} slots, "
                f"solo {worker_pool.slots_free()} disponibles — esperando"
            )
            break  # FIFO estricto: no saltar al siguiente

        # Dequeue y lanzar
        dequeued = pending_queue.dequeue_one()
        if dequeued:
            logger.info(f"▶️  Drenando cola: {dequeued} ({pending_queue.size()} restantes)")
            _launch_from_queue(dequeued)


def _reschedule_scraper(scraper_name: str):
    """
    Recalcula el intervalo base usando duración EMA y reprograma el job.
    El nuevo intervalo incorpora jitter log-normal.
    """
    logger = logging.getLogger("Scheduler")
    try:
        # Usar duración EMA si disponible; si no, la del config
        effective_dur = stats_tracker.get_effective_duration(scraper_name)
        if effective_dur and effective_dur > 0:
            base_interval = config.calculate_base_interval_from_duration(
                scraper_name, effective_dur
            )
            logger.debug(
                f"⏰ {scraper_name}: intervalo base recalculado con "
                f"EMA={effective_dur:.0f}s → {base_interval:.2f}min"
            )
        else:
            base_interval = config.calculate_base_interval(scraper_name)

        jitter_cfg = config.get_jitter_config(scraper_name)
        next_interval = calcular_intervalo_con_jitter(
            intervalo_base_min=base_interval,
            k=jitter_cfg['k'],
            min_factor=jitter_cfg['min_factor'],
            max_factor=jitter_cfg['max_factor'],
            logger=logging.getLogger("JitterCalculator"),
        )

        scheduler.reschedule_job(
            job_id=f"job_{scraper_name}",
            trigger='interval',
            minutes=next_interval,
        )
        logger.info(
            f"⏰ {scraper_name} reprogramado: próxima en {next_interval:.2f}min "
            f"(base={base_interval:.2f}min)"
        )
    except Exception as e:
        logger.error(f"❌ Error reprogramando {scraper_name}: {e}", exc_info=True)


# ──────────────────────────────────────────────────────────
# Funciones de job para APScheduler (una por scraper)
# ──────────────────────────────────────────────────────────
# Se generan dinámicamente en initialize_scheduler() para
# que nuevos scrapers en config.yaml no requieran cambios aquí.

def _make_job_func(name: str):
    def job():
        execute_scraper(name)
    job.__name__ = f"run_{name}"
    return job


# ──────────────────────────────────────────────────────────
# Cleanup job
# ──────────────────────────────────────────────────────────

def cleanup_finished_processes():
    """
    Job periódico que:
      1. Mata scrapers stuck → libera slots → registra fallo
      2. Detecta scrapers completados → libera slots → actualiza EMA → reprograma
      3. Drena la cola FIFO
      4. Loggea dashboard de estado
    """
    global _cleanup_call_count
    _cleanup_call_count += 1
    logger = logging.getLogger("Scheduler")

    ended_scrapers = []   # scrapers que terminaron en este ciclo (stuck o natural)

    # ── Fase 1: Detectar y matar stuck ───────────────────────────────
    for scraper_name in executor.get_active_scrapers():
        if executor.is_stuck(scraper_name):
            uptime = executor.get_uptime(scraper_name) or 0
            logger.warning(
                f"💀 {scraper_name} ATASCADO ({uptime:.0f}s ≥ "
                f"{executor.timeout_sec}s) → matando y liberando slots"
            )
            executor.kill_scraper(scraper_name, force=True)
            worker_pool.release(scraper_name)
            stats_tracker.record_stuck(scraper_name)
            ended_scrapers.append(scraper_name)

    # ── Fase 2: Cleanup de procesos terminados naturalmente ───────────
    completed = executor.cleanup_finished()
    # completed = list of (scraper_name, duration_sec, success)

    for scraper_name, duration, success in completed:
        worker_pool.release(scraper_name)
        stats_tracker.record_run(scraper_name, duration, success)
        ended_scrapers.append(scraper_name)

    # ── Fase 3: Reprogramar los que terminaron ────────────────────────
    if config.should_recalculate_intervals():
        for scraper_name in ended_scrapers:
            _reschedule_scraper(scraper_name)

    # ── Fase 4: Drenar cola FIFO ──────────────────────────────────────
    if ended_scrapers or pending_queue.size() > 0:
        _try_drain_queue()

    # ── Fase 5: Dashboard de estado ───────────────────────────────────
    should_log = (
        ended_scrapers
        or pending_queue.size() > 0
        or (_cleanup_call_count % DASHBOARD_EVERY_N_CLEANUPS == 0)
    )
    if should_log:
        _log_dashboard()


def _log_dashboard():
    """Imprime una línea de estado del sistema completo."""
    logger = logging.getLogger("Scheduler")
    pool_status = worker_pool.get_status()
    pending = pending_queue.peek_all()
    active = executor.get_active_scrapers()

    # Calcular uptimes de activos
    active_str_parts = []
    for name in active:
        uptime = executor.get_uptime(name) or 0
        info = executor.active_processes.get(name, {})
        n_shards = info.get('n_shards', 1)
        active_str_parts.append(f"{name}({n_shards}sh/{uptime:.0f}s)")

    pool_line = worker_pool.format_status_line()
    pending_line = f"[Cola: {pending}]" if pending else "[Cola: vacía]"
    active_line = f"[Activos: {', '.join(active_str_parts)}]" if active_str_parts else "[Activos: ninguno]"

    logger.info(f"📊 {pool_line} {active_line} {pending_line}")

    # Stats completos (solo periódicamente para no saturar logs)
    if _cleanup_call_count % DASHBOARD_EVERY_N_CLEANUPS == 0:
        stats_tracker.log_summary()


# ──────────────────────────────────────────────────────────
# Listener de APScheduler
# ──────────────────────────────────────────────────────────

def job_listener(event):
    logger = logging.getLogger("Scheduler")
    if event.exception:
        logger.error(
            f"❌ Job {event.job_id} lanzó excepción: {event.exception}",
            exc_info=True,
        )


# ──────────────────────────────────────────────────────────
# Signal handler
# ──────────────────────────────────────────────────────────

def signal_handler(sig, frame):
    logger = logging.getLogger("Scheduler")
    names = {signal.SIGINT: "SIGINT (Ctrl+C)", signal.SIGTERM: "SIGTERM"}
    logger.info(f"🛑 {names.get(sig, sig)} recibido → shutdown graceful")

    if scheduler and scheduler.running:
        scheduler.shutdown(wait=False)

    active = executor.get_active_scrapers()
    if active:
        logger.info(f"⏳ Esperando scrapers activos: {active}")
        for _ in range(30):
            if not executor.get_active_scrapers():
                break
            time.sleep(1)
        else:
            killed = executor.kill_all(force=True)
            logger.warning(f"⏱️  Timeout: {killed} scraper(s) matados forzadamente")

    logger.info("📊 Estadísticas finales:")
    for name, stats in executor.get_all_stats().items():
        logger.info(
            f"   {name}: {stats.get('successful_runs', 0)}/"
            f"{stats.get('total_runs', 0)} exitosas, "
            f"avg={stats.get('avg_duration', 0) or 0:.1f}s"
        )

    logger.info("👋 Scheduler detenido")
    sys.exit(0)


# ──────────────────────────────────────────────────────────
# Inicialización
# ──────────────────────────────────────────────────────────

def initialize_scheduler():
    global config, executor, scheduler, worker_pool, pending_queue
    global stats_tracker, shard_manager

    logger = logging.getLogger("Scheduler")

    # 1. Config
    logger.info("🔧 Cargando configuración...")
    config = SchedulerConfig("config.yaml")

    cpu_info = config.get_machine_capacity()
    logger.info(
        f"💻 CPU: {cpu_info['cores']} cores, "
        f"{cpu_info['threads']} threads, "
        f"score={cpu_info['performance_score']}/100"
    )

    # 2. Validar jitter global
    jitter_global = config.config['jitter']
    ok, msg = validar_config_jitter(
        jitter_global['k'], jitter_global['min_factor'], jitter_global['max_factor']
    )
    if not ok:
        logger.error(f"❌ Jitter global inválido: {msg} → usando defaults")
        jitter_global.update({'k': 2, 'min_factor': 0.5, 'max_factor': 2.2})
    else:
        logger.info(
            f"✅ Jitter global: k={jitter_global['k']}, "
            f"rango=[{jitter_global['min_factor']}x, {jitter_global['max_factor']}x]"
        )

    # 3. Módulos nuevos
    max_workers = config.get_max_total_workers()
    logger.info(f"🔧 max_total_workers={max_workers}")

    worker_pool = WorkerPool(max_workers=max_workers)
    pending_queue = PendingQueue()
    stats_tracker = StatsTracker()
    shard_manager = ShardManager()

    # 4. Executor
    executor = ScraperExecutor(
        scripts_dir="/app/scripts/paralelizado",
        timeout_sec=config.get_scraper_timeout(),
    )

    # 5. Registrar scrapers en StatsTracker (seed = duración del config)
    for name in config.get_enabled_scrapers():
        scraper_cfg = config.get_scraper_config(name)
        seed_dur = scraper_cfg.get('estimated_duration_sec', 60)
        stats_tracker.register(name, seed_dur)
        logger.info(
            f"📊 {name} registrado: seed={seed_dur}s, "
            f"shard_cfg={config.get_shard_config(name)}"
        )

    # 6. APScheduler
    aps_executors = {'default': ThreadPoolExecutor(max_workers=10)}
    job_defaults = {
        'coalesce': False,
        'max_instances': 1,
        'misfire_grace_time': 30,
    }
    scheduler = BlockingScheduler(
        executors=aps_executors,
        job_defaults=job_defaults,
    )
    scheduler.add_listener(job_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)

    # 7. Programar scrapers habilitados (funciones generadas dinámicamente)
    enabled = config.get_enabled_scrapers()
    logger.info(f"📋 Scrapers habilitados: {', '.join(enabled)}")

    for scraper_name in enabled:
        base_interval = config.calculate_base_interval(scraper_name)
        jitter_cfg = config.get_jitter_config(scraper_name)
        initial_interval = calcular_intervalo_con_jitter(
            intervalo_base_min=base_interval,
            k=jitter_cfg['k'],
            min_factor=jitter_cfg['min_factor'],
            max_factor=jitter_cfg['max_factor'],
        )
        shard_info = config.get_shard_config(scraper_name)
        scheduler.add_job(
            func=_make_job_func(scraper_name),
            trigger='interval',
            minutes=initial_interval,
            id=f"job_{scraper_name}",
            name=scraper_name,
            max_instances=1,
        )
        logger.info(
            f"✅ {scraper_name} programado: primera en {initial_interval:.2f}min, "
            f"base~{base_interval:.2f}min | "
            f"shards_cfg={shard_info}"
        )

    # 8. Job de limpieza
    cleanup_interval = config.get_cleanup_interval()
    scheduler.add_job(
        func=cleanup_finished_processes,
        trigger='interval',
        seconds=cleanup_interval,
        id='job_cleanup',
        name='cleanup',
        max_instances=1,
    )
    logger.info(f"✅ Cleanup programado cada {cleanup_interval}s")

    return scheduler


# ──────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────

def main():
    log_cfg = SchedulerConfig("config.yaml").get_logging_config()
    setup_logging(log_cfg.get('level', 'INFO'))

    logger = logging.getLogger("Scheduler")
    logger.info("=" * 70)
    logger.info("🤖 SCHEDULER DE SCRAPERS - CDA BÚSQUEDA")
    logger.info("=" * 70)
    logger.info(f"🕐 Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 70)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    global scheduler
    try:
        scheduler = initialize_scheduler()
    except Exception as e:
        logger.critical(f"❌ Error inicializando: {e}", exc_info=True)
        sys.exit(1)

    logger.info("🚀 Scheduler iniciado (Ctrl+C para detener)")
    logger.info("=" * 70)

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass
    except Exception as e:
        logger.critical(f"❌ Error fatal: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()