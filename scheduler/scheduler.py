#edittt - Archivo nuevo: Scheduler principal con APScheduler
"""
Scheduler Principal - Orquestador de Scrapers

Este módulo:
1. Carga configuración (config.yaml)
2. Detecta capacidad de CPU
3. Crea jobs independientes para cada scraper
4. Aplica jitter log-normal a intervalos
5. Reprograma dinámicamente después de cada ejecución
6. Limpia procesos terminados periódicamente
7. Maneja señales de shutdown gracefully

Ejemplo de ejecución:
    python scheduler.py
    
    # Output:
    # [Scheduler] INFO: 💻 CPU detectado: 4 cores, 8 threads, score=48/100
    # [Scheduler] INFO: ✅ amber_chiapas programado cada ~12.5 min
    # [Scheduler] INFO: ✅ havistoa_chiapas programado cada ~12.5 min
    # [Scheduler] INFO: ✅ amber_nacional programado cada ~30.0 min
    # [Scheduler] INFO: 🚀 Scheduler iniciado con 3 jobs activos
"""

import logging
import signal
import sys
import time
from datetime import datetime, timedelta

#edittt - Imports de nuestros módulos
from scheduler_config import SchedulerConfig
from jitter_calculator import calcular_intervalo_con_jitter, validar_config_jitter
from scraper_executor import ScraperExecutor

#edittt - Imports de APScheduler
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR


#Configuración de logging
def setup_logging(log_level: str = "INFO"):
    """
    Configura el sistema de logging.
    
    Args:
        log_level: Nivel de logging (DEBUG, INFO, WARNING, ERROR)
    """
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)
    
    logging.basicConfig(
        level=numeric_level,
        format='[%(asctime)s] [%(name)s] %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Reducir verbosidad de APScheduler (muy ruidoso en DEBUG)
    logging.getLogger('apscheduler').setLevel(logging.WARNING)


#Variables globales (instancias únicas)
config: SchedulerConfig = None
executor: ScraperExecutor = None
scheduler: BlockingScheduler = None


#Función para ejecutar un scraper específico
def execute_scraper(scraper_name: str):
    """
    Ejecuta un scraper y reprograma su próxima ejecución con jitter.
    
    Esta función es llamada por APScheduler según el intervalo configurado.
    Después de ejecutar, recalcula el próximo intervalo con jitter y
    reprograma el job.
    
    Args:
        scraper_name: Nombre del scraper (ej: 'amber_chiapas')
    """
    logger = logging.getLogger("Scheduler")
    
    #Verificar si el scraper ya está corriendo
    if executor.is_running(scraper_name):
        logger.warning(f"⏭️  {scraper_name} ya está en ejecución, skip")
        return
    
    #Obtener configuración del scraper
    scraper_cfg = config.get_scraper_config(scraper_name)
    if not scraper_cfg:
        logger.error(f"❌ Configuración de {scraper_name} no encontrada")
        return
    
    #Verificar que está habilitado
    if not scraper_cfg.get('enabled', True):
        logger.info(f"⏸️  {scraper_name} está deshabilitado, skip")
        return
    
    #Ejecutar el scraper
    logger.info(f"🚀 Disparando {scraper_name}")
    start_time = time.time()
    
    success = executor.execute(
        scraper_name=scraper_name,
        script_filename=scraper_cfg['script_filename']
    )
    
    if not success:
        logger.error(f"❌ No se pudo ejecutar {scraper_name}")
        return
    
    execution_time = time.time() - start_time
    logger.debug(f"⚡ {scraper_name} lanzado en {execution_time*1000:.0f}ms")
    
    #Reprogramar con nuevo intervalo (si dynamic_interval_recalculation=True)
    if config.should_recalculate_intervals():
        _reschedule_scraper(scraper_name)


#Función para reprogramar un scraper con jitter
def _reschedule_scraper(scraper_name: str):
    """
    Recalcula el intervalo con jitter y reprograma el job.
    
    Args:
        scraper_name: Nombre del scraper
    """
    logger = logging.getLogger("Scheduler")
    
    try:
        base_interval = config.calculate_base_interval(scraper_name)
        
        #Aplicar jitter
        #EDITTT - Usar configuración de jitter efectiva por scraper (override local + fallback global)
        jitter_cfg = config.get_jitter_config(scraper_name)
        next_interval = calcular_intervalo_con_jitter(
            intervalo_base_min=base_interval,
            k=jitter_cfg['k'],
            min_factor=jitter_cfg['min_factor'],
            max_factor=jitter_cfg['max_factor'],
            logger=logging.getLogger("JitterCalculator")
        )

        #Reprogramar job en APScheduler
        job_id = f"job_{scraper_name}"
        scheduler.reschedule_job(
            job_id=job_id,
            trigger='interval',
            minutes=next_interval
        )
        
        logger.info(
            f"⏰ {scraper_name} reprogramado: próxima ejecución en {next_interval:.2f} min"
        )
        
    except Exception as e:
        logger.error(f"❌ Error reprogramando {scraper_name}: {e}", exc_info=True)


#Funciones específicas para cada scraper (para APScheduler)
def run_amber_chiapas():
    """Job para scraper amber_chiapas."""
    execute_scraper('amber_chiapas')


def run_havistoa_chiapas():
    """Job para scraper havistoa_chiapas."""
    execute_scraper('havistoa_chiapas')


def run_amber_nacional():
    """Job para scraper amber_nacional."""
    execute_scraper('amber_nacional')


#Mapeo de scrapers a funciones
SCRAPER_FUNCTIONS = {
    'amber_chiapas': run_amber_chiapas,
    'havistoa_chiapas': run_havistoa_chiapas,
    'amber_nacional': run_amber_nacional
}


#Función de limpieza periódica
def cleanup_finished_processes():
    """
    Job periódico que limpia procesos terminados.
    Ejecutado por APScheduler cada X segundos.
    """
    logger = logging.getLogger("Scheduler")
    
    cleaned_count = executor.cleanup_finished()
    
    if cleaned_count > 0:
        logger.debug(f"🧹 Limpiados {cleaned_count} procesos terminados")
    
    #edittt - Verificar scrapers atascados (stuck)
    active_scrapers = executor.get_active_scrapers()
    for scraper_name in active_scrapers:
        if executor.is_stuck(scraper_name):
            uptime = executor.get_uptime(scraper_name)
            timeout = executor.timeout_sec
            logger.warning(
                f"⚠️  {scraper_name} posiblemente atascado "
                f"(corriendo {uptime:.0f}s, timeout={timeout}s)"
            )


#edittt - Listener de eventos de APScheduler
def job_listener(event):
    """
    Listener de eventos de APScheduler para logging adicional.
    
    Args:
        event: Evento de APScheduler (JOB_EXECUTED, JOB_ERROR, etc.)
    """
    logger = logging.getLogger("Scheduler")
    
    if event.exception:
        logger.error(
            f"❌ Job {event.job_id} falló con excepción: {event.exception}",
            exc_info=True
        )
    else:
        logger.debug(f"✅ Job {event.job_id} ejecutado exitosamente")


#edittt - Manejador de señales para shutdown graceful
def signal_handler(sig, frame):
    """
    Maneja señales SIGINT (Ctrl+C) y SIGTERM (docker stop) para shutdown graceful.
    
    Args:
        sig: Número de señal
        frame: Frame actual
    """
    logger = logging.getLogger("Scheduler")
    
    signal_names = {
        signal.SIGINT: "SIGINT (Ctrl+C)",
        signal.SIGTERM: "SIGTERM"
    }
    signal_name = signal_names.get(sig, f"Signal {sig}")
    
    logger.info(f"🛑 {signal_name} recibido, iniciando shutdown graceful...")
    
    #edittt - Detener scheduler (no acepta nuevos jobs)
    if scheduler and scheduler.running:
        logger.info("⏸️  Deteniendo scheduler...")
        scheduler.shutdown(wait=False)
    
    #edittt - Dar chance a scrapers de terminar (hasta 30 segundos)
    active_scrapers = executor.get_active_scrapers()
    if active_scrapers:
        logger.info(f"⏳ Esperando a {len(active_scrapers)} scraper(s) activo(s)...")
        logger.info(f"   Scrapers: {', '.join(active_scrapers)}")
        
        # Esperar hasta 30 segundos
        wait_time = 30
        for i in range(wait_time):
            active_scrapers = executor.get_active_scrapers()
            if not active_scrapers:
                logger.info("✅ Todos los scrapers terminaron")
                break
            time.sleep(1)
        else:
            # Timeout alcanzado, forzar kill
            logger.warning(f"⏱️  Timeout de {wait_time}s alcanzado, terminando scrapers forzadamente...")
            killed = executor.kill_all(force=True)
            logger.warning(f"🔪 {killed} scraper(s) terminados forzadamente")
    
    #edittt - Mostrar estadísticas finales
    logger.info("📊 Estadísticas finales:")
    all_stats = executor.get_all_stats()
    for scraper_name, stats in all_stats.items():
        logger.info(
            f"   {scraper_name}: {stats['successful_runs']}/{stats['total_runs']} exitosas, "
            f"avg={stats.get('avg_duration', 0):.1f}s"
        )
    
    logger.info("👋 Scheduler detenido exitosamente")
    sys.exit(0)


#edittt - Función principal de inicialización
def initialize_scheduler():
    """
    Inicializa el scheduler con toda la configuración.
    
    Returns:
        BlockingScheduler configurado
    """
    global config, executor, scheduler
    
    logger = logging.getLogger("Scheduler")
    
    #1. Cargar configuración
    logger.info("🔧 Cargando configuración...")
    config = SchedulerConfig("config.yaml")
    
    #2. Mostrar info de CPU detectada
    cpu_info = config.get_machine_capacity()
    logger.info(
        f"💻 CPU detectado: {cpu_info['cores']} cores, "
        f"{cpu_info['threads']} threads, "
        f"score={cpu_info['performance_score']}/100"
    )
    
    #3. Validar configuración de jitter
    #EDITTT - Validar jitter GLOBAL (fallback). El jitter por scraper se valida en SchedulerConfig.
    jitter_cfg_global = config.config['jitter']  # EDITTT (antes jitter_cfg)
    es_valido, mensaje = validar_config_jitter(
        jitter_cfg_global['k'],
        jitter_cfg_global['min_factor'],
        jitter_cfg_global['max_factor']
    )
    if not es_valido:
        logger.error(f"❌ Configuración de jitter GLOBAL inválida: {mensaje}")
        logger.error("   Usando valores por defecto seguros")
        #EDITTT - Ajuste explícito sobre jitter global
        jitter_cfg_global['k'] = 2
        jitter_cfg_global['min_factor'] = 0.5
        jitter_cfg_global['max_factor'] = 2.2
    else:
        logger.info(
            f"✅ Jitter GLOBAL configurado: "
            f"k={jitter_cfg_global['k']}, "
            f"rango=[{jitter_cfg_global['min_factor']}x, {jitter_cfg_global['max_factor']}x]"
        )
    #4. Inicializar executor
    logger.info("🔧 Inicializando executor de scrapers...")
    executor = ScraperExecutor(
        scripts_dir="/app/scripts/paralelizado",
        timeout_sec=config.get_scraper_timeout()
    )
    
    #5. Configurar APScheduler
    logger.info("🔧 Configurando APScheduler...")
    
    executors = {
        'default': ThreadPoolExecutor(max_workers=10)
    }
    
    job_defaults = {
        'coalesce': False,      # No combinar ejecuciones perdidas
        'max_instances': 1,     # Solo 1 instancia por job
        'misfire_grace_time': 30  # 30s de gracia si se retrasa
    }
    
    scheduler = BlockingScheduler(
        executors=executors,
        job_defaults=job_defaults
    )
    
    #6. Agregar listener de eventos
    scheduler.add_listener(job_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
    
    #7. Programar scrapers habilitados
    enabled_scrapers = config.get_enabled_scrapers()
    logger.info(f"📋 Scrapers habilitados: {', '.join(enabled_scrapers)}")
    
    for scraper_name in enabled_scrapers:
        #Verificar que tenemos función para este scraper
        if scraper_name not in SCRAPER_FUNCTIONS:
            logger.error(f"❌ No hay función definida para {scraper_name}, skip")
            continue
        
        #Calcular intervalo inicial
        base_interval = config.calculate_base_interval(scraper_name)
        
        #Aplicar jitter al primer intervalo
        #EDITTT - Usar jitter efectivo específico para cada scraper
        jitter_cfg_scraper = config.get_jitter_config(scraper_name)
        initial_interval = calcular_intervalo_con_jitter(
            intervalo_base_min=base_interval,
            k=jitter_cfg_scraper['k'],
            min_factor=jitter_cfg_scraper['min_factor'],
            max_factor=jitter_cfg_scraper['max_factor']
        )
        
        #Agregar job a APScheduler
        job_func = SCRAPER_FUNCTIONS[scraper_name]
        scheduler.add_job(
            func=job_func,
            trigger='interval',
            minutes=initial_interval,
            id=f"job_{scraper_name}",
            name=scraper_name,
            max_instances=1
        )
        
        logger.info(
            f"✅ {scraper_name} programado: primera ejecución en {initial_interval:.2f} min, "
            f"luego cada ~{base_interval:.2f} min (±jitter)"
        )
    
    #edittt - 8. Agregar job de limpieza periódica
    cleanup_interval = config.get_cleanup_interval()
    scheduler.add_job(
        func=cleanup_finished_processes,
        trigger='interval',
        seconds=cleanup_interval,
        id='job_cleanup',
        name='cleanup',
        max_instances=1
    )
    logger.info(f"✅ Limpieza periódica programada cada {cleanup_interval}s")
    
    return scheduler


#Función principal
def main():
    #1. Configurar logging
    log_config = SchedulerConfig("config.yaml").get_logging_config()
    setup_logging(log_config.get('level', 'INFO'))
    
    logger = logging.getLogger("Scheduler")
    
    #2. Mostrar banner
    logger.info("="*70)
    logger.info("🤖 SCHEDULER DE SCRAPERS - CDA BÚSQUEDA")
    logger.info("="*70)
    logger.info(f"🕐 Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("="*70)
    
    #3. Registrar manejadores de señales
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    logger.info("✅ Manejadores de señales registrados (Ctrl+C para detener)")
    
    #4. Inicializar scheduler
    try:
        global scheduler
        scheduler = initialize_scheduler()
    except Exception as e:
        logger.critical(f"❌ Error inicializando scheduler: {e}", exc_info=True)
        sys.exit(1)
    
    #5. Mostrar resumen pre-inicio
    logger.info("="*70)
    logger.info("📊 RESUMEN DE CONFIGURACIÓN")
    logger.info("="*70)
    
    jobs = scheduler.get_jobs()
    scraper_jobs = [j for j in jobs if j.id.startswith('job_') and j.id != 'job_cleanup']
    
    logger.info(f"Total de scrapers programados: {len(scraper_jobs)}")

    for job in scraper_jobs:
        try:
            # Intentar obtener next_run_time (APScheduler 3.x)
            trigger = job.trigger
            if hasattr(trigger, 'run_date'):
                # Trigger tipo 'date'
                next_run = trigger.run_date
            else:
                # Trigger tipo 'interval' o 'cron'
                # Calcular manualmente la próxima ejecución
                if hasattr(job, '_scheduler') and hasattr(job._scheduler, '_real_add_job'):
                    # Job ya fue agregado, calcular próxima ejecución
                    now = datetime.now(job.trigger.timezone if hasattr(job.trigger, 'timezone') else None)
                    next_run = now + timedelta(minutes=job.trigger.interval.total_seconds()/60 if hasattr(job.trigger, 'interval') else 0)
                else:
                    next_run = None
            
            if next_run:
                logger.info(f"  • {job.name}: próxima ejecución a las {next_run.strftime('%H:%M:%S')}")
            else:
                logger.info(f"  • {job.name}: programado")
        except Exception as e:
            # Si todo falla, mostrar info básica
            logger.info(f"  • {job.name}: programado (próxima ejecución no disponible)")
    
    #SOLUCIÓN MÁS SIMPLE (más ligera, menos visible en logs)
    # for job in scraper_jobs:
    # # En algunas versiones de APScheduler, next_run_time puede no existir o no estar inicializado
    # next_run = getattr(job, "next_run_time", None)

    # if next_run is None:
    #     logger.info(f"  • {job.name}: próxima ejecución programada (hora exacta no disponible)")
    # else:
    #     try:
    #         logger.info(
    #             f"  • {job.name}: próxima ejecución a las {next_run.strftime('%H:%M:%S')}"
    #         )
    #     except Exception:
    #         logger.info(
    #             f"  • {job.name}: próxima ejecución en {next_run}"
    #         )

    logger.info("="*70)
    
    #6. Iniciar scheduler (bloquea aquí)
    logger.info("🚀 Iniciando scheduler... (presiona Ctrl+C para detener)")
    logger.info("="*70)
    
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        # Esto normalmente no se alcanza porque signal_handler maneja las señales
        pass
    except Exception as e:
        logger.critical(f"❌ Error fatal en scheduler: {e}", exc_info=True)
        sys.exit(1)

if __name__ == '__main__':
    main()