#edittt - Archivo nuevo: Ejecutor robusto de scrapers con subprocess
"""
Scraper Executor - Gestión de Ejecución de Scrapers

Este módulo maneja:
1. Ejecución de scrapers vía subprocess.Popen (non-blocking)
2. Tracking de procesos activos (evitar duplicados)
3. Limpieza de procesos terminados (evitar zombies)
4. Estadísticas de ejecución (duración, exit codes)
5. Logging detallado para observabilidad

Ejemplo de uso:
    executor = ScraperExecutor("/app/scripts/paralelizado")
    
    # Verificar si ya está corriendo
    if not executor.is_running('amber_chiapas'):
        executor.execute('amber_chiapas', 'paralelo_amber_chiapas.py')
    
    # Limpiar procesos terminados
    executor.cleanup_finished()
    
    # Obtener estadísticas
    stats = executor.get_stats('amber_chiapas')
"""

import subprocess
import logging
import time
import sys
from pathlib import Path
from typing import Optional, Dict, Any
from datetime import datetime, timedelta

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False


#edittt - Clase principal para ejecutar scrapers
class ScraperExecutor:
    """
    Gestor de ejecución de scrapers con control de procesos.
    
    Responsabilidades:
    - Ejecutar scrapers como subprocesos independientes
    - Mantener registro de procesos activos
    - Evitar ejecuciones duplicadas del mismo scraper
    - Recolectar estadísticas de ejecución
    - Limpiar procesos zombies
    """
    
    def __init__(
        self,
        scripts_dir: str = "/app/scripts/paralelizado",
        timeout_sec: Optional[int] = 900
    ):
        """
        Inicializa el executor de scrapers.
        
        Args:
            scripts_dir: Directorio donde están los scripts de scraping
            timeout_sec: Timeout máximo para un scraper (None = sin límite)
        """
        self.scripts_dir = Path(scripts_dir)
        self.timeout_sec = timeout_sec
        self.logger = logging.getLogger("ScraperExecutor")
        
        #edittt - Diccionario de procesos activos
        # Estructura: {scraper_name: {process, start_time, script_filename}}
        self.active_processes: Dict[str, Dict[str, Any]] = {}
        
        #edittt - Diccionario de estadísticas de ejecución
        # Estructura: {scraper_name: {last_start, last_duration, last_exit_code, total_runs, ...}}
        self.execution_stats: Dict[str, Dict[str, Any]] = {}
        
        #edittt - Validar que el directorio de scripts existe
        if not self.scripts_dir.exists():
            self.logger.warning(
                f"⚠️  Directorio de scripts no existe: {self.scripts_dir}"
            )
        
        self.logger.info(f"✅ ScraperExecutor inicializado (scripts_dir={scripts_dir})")
    
    def is_running(self, scraper_name: str) -> bool:
        """
        Verifica si un scraper está actualmente en ejecución.
        
        Args:
            scraper_name: Nombre del scraper (ej: 'amber_chiapas')
        
        Returns:
            True si el scraper está corriendo, False si no
        """
        if scraper_name not in self.active_processes:
            return False
        
        process_info = self.active_processes[scraper_name]
        process = process_info['process']
        
        #edittt - Verificar si el proceso aún está vivo
        poll_result = process.poll()
        
        if poll_result is not None:
            # Proceso ya terminó, pero aún está en el diccionario
            # (cleanup_finished() lo removerá)
            return False
        
        return True
    
    def execute(
        self,
        scraper_name: str,
        script_filename: str,
        env_vars: Optional[Dict[str, str]] = None
    ) -> bool:
        """
        Ejecuta un scraper en background como subproceso.
        
        Args:
            scraper_name: Nombre identificador del scraper
            script_filename: Nombre del archivo Python a ejecutar
            env_vars: Variables de entorno adicionales (opcional)
        
        Returns:
            True si se ejecutó exitosamente, False si ya estaba corriendo
            o si hubo un error
        
        Raises:
            FileNotFoundError: Si el script no existe
        """
        #edittt - Verificar si ya está corriendo
        if self.is_running(scraper_name):
            self.logger.warning(
                f"⏭️  {scraper_name} ya está en ejecución, skip"
            )
            return False
        
        #edittt - Construir ruta completa al script
        script_path = self.scripts_dir / script_filename
        
        if not script_path.exists():
            self.logger.error(
                f"❌ Script no encontrado: {script_path}"
            )
            raise FileNotFoundError(f"Script no existe: {script_path}")
        
        #edittt - Preparar comando
        cmd = [sys.executable, str(script_path)]
        
        #edittt - Preparar variables de entorno
        import os
        process_env = os.environ.copy()
        if env_vars:
            process_env.update(env_vars)
        
        #edittt - Ejecutar proceso
        try:
            self.logger.info(f"🚀 Ejecutando {scraper_name}: {script_filename}")
            
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=str(self.scripts_dir.parent),  # /app/scripts
                env=process_env,
                text=True,
                bufsize=1  # Line buffered
            )
            
            #edittt - Registrar proceso activo
            start_time = time.time()
            self.active_processes[scraper_name] = {
                'process': process,
                'start_time': start_time,
                'start_datetime': datetime.now(),
                'script_filename': script_filename,
                'pid': process.pid
            }
            
            #edittt - Inicializar stats si es la primera ejecución
            if scraper_name not in self.execution_stats:
                self.execution_stats[scraper_name] = {
                    'total_runs': 0,
                    'successful_runs': 0,
                    'failed_runs': 0,
                    'total_duration_sec': 0.0,
                    'last_start': None,
                    'last_duration': None,
                    'last_exit_code': None,
                    'avg_duration': None
                }
            
            self.execution_stats[scraper_name]['total_runs'] += 1
            self.execution_stats[scraper_name]['last_start'] = datetime.now()
            
            self.logger.info(
                f"✅ {scraper_name} iniciado (PID={process.pid})"
            )
            
            return True
            
        except Exception as e:
            self.logger.error(
                f"❌ Error ejecutando {scraper_name}: {e}",
                exc_info=True
            )
            return False
    
    def cleanup_finished(self) -> int:
        """
        Limpia procesos terminados del registro de activos.
        Actualiza estadísticas de ejecución.
        
        Returns:
            Número de procesos limpiados
        """
        cleaned_count = 0
        scrapers_to_remove = []
        
        for scraper_name, process_info in self.active_processes.items():
            process = process_info['process']
            exit_code = process.poll()
            
            #edittt - Si poll() retorna None, el proceso aún corre
            if exit_code is not None:
                # Proceso terminó
                scrapers_to_remove.append(scraper_name)
                
                #edittt - Calcular duración
                duration = time.time() - process_info['start_time']
                
                #edittt - Actualizar estadísticas
                stats = self.execution_stats[scraper_name]
                stats['last_duration'] = duration
                stats['last_exit_code'] = exit_code
                stats['total_duration_sec'] += duration
                
                if exit_code == 0:
                    stats['successful_runs'] += 1
                    self.logger.info(
                        f"✅ {scraper_name} completado exitosamente "
                        f"(duración: {duration:.2f}s)"
                    )
                else:
                    stats['failed_runs'] += 1
                    self.logger.error(
                        f"❌ {scraper_name} falló con exit code {exit_code} "
                        f"(duración: {duration:.2f}s)"
                    )
                    
                    #edittt - Capturar stderr si hay error
                    try:
                        _, stderr = process.communicate(timeout=1)
                        if stderr:
                            self.logger.error(
                                f"📋 {scraper_name} stderr (últimas 500 chars):\n"
                                f"{stderr[-500:]}"
                            )
                    except subprocess.TimeoutExpired:
                        pass
                
                #edittt - Calcular promedio de duración
                if stats['total_runs'] > 0:
                    stats['avg_duration'] = (
                        stats['total_duration_sec'] / stats['total_runs']
                    )
                
                #edittt - Log adicional de CPU/memoria si psutil disponible
                if PSUTIL_AVAILABLE:
                    self._log_resource_usage(scraper_name)
                
                cleaned_count += 1
        
        #edittt - Remover procesos terminados del diccionario
        for scraper_name in scrapers_to_remove:
            del self.active_processes[scraper_name]
        
        if cleaned_count > 0:
            self.logger.debug(f"🧹 Limpiados {cleaned_count} procesos terminados")
        
        return cleaned_count
    
    def _log_resource_usage(self, scraper_name: str):
        """
        Logea uso de CPU y memoria (requiere psutil).
        
        Args:
            scraper_name: Nombre del scraper
        """
        if not PSUTIL_AVAILABLE:
            return
        
        try:
            # Obtener info del sistema
            cpu_percent = psutil.cpu_percent(interval=0.1)
            memory = psutil.virtual_memory()
            
            self.logger.info(
                f"📊 {scraper_name} recursos al terminar: "
                f"CPU={cpu_percent:.1f}%, "
                f"RAM={memory.percent:.1f}% ({memory.used / (1024**3):.2f}GB usado)"
            )
        except Exception as e:
            self.logger.debug(f"No se pudo obtener info de recursos: {e}")
    
    def get_stats(self, scraper_name: str) -> Optional[Dict[str, Any]]:
        """
        Obtiene estadísticas de ejecución de un scraper.
        
        Args:
            scraper_name: Nombre del scraper
        
        Returns:
            Diccionario con estadísticas o None si no hay datos
        """
        return self.execution_stats.get(scraper_name)
    
    def get_all_stats(self) -> Dict[str, Dict[str, Any]]:
        """
        Obtiene estadísticas de todos los scrapers.
        
        Returns:
            Diccionario con stats de cada scraper
        """
        return self.execution_stats.copy()
    
    def is_stuck(self, scraper_name: str) -> bool:
        """
        Verifica si un scraper parece estar atascado (timeout excedido).
        
        Args:
            scraper_name: Nombre del scraper
        
        Returns:
            True si el timeout fue excedido, False si no
        """
        if not self.is_running(scraper_name):
            return False
        
        if self.timeout_sec is None:
            return False  # Sin timeout configurado
        
        process_info = self.active_processes[scraper_name]
        elapsed = time.time() - process_info['start_time']
        
        return elapsed > self.timeout_sec
    
    def get_active_scrapers(self) -> list:
        """
        Retorna lista de nombres de scrapers actualmente en ejecución.
        
        Returns:
            Lista de strings con nombres de scrapers activos
        """
        #edittt - Filtrar solo los que realmente están corriendo
        active = []
        for scraper_name in list(self.active_processes.keys()):
            if self.is_running(scraper_name):
                active.append(scraper_name)
        return active
    
    def get_uptime(self, scraper_name: str) -> Optional[float]:
        """
        Obtiene el tiempo que lleva corriendo un scraper.
        
        Args:
            scraper_name: Nombre del scraper
        
        Returns:
            Tiempo en segundos o None si no está corriendo
        """
        if not self.is_running(scraper_name):
            return None
        
        process_info = self.active_processes[scraper_name]
        return time.time() - process_info['start_time']
    
    def kill_scraper(self, scraper_name: str, force: bool = False) -> bool:
        """
        Termina un scraper en ejecución.
        
        Args:
            scraper_name: Nombre del scraper
            force: Si True, usa SIGKILL (forzado), si False usa SIGTERM (graceful)
        
        Returns:
            True si se terminó, False si no estaba corriendo
        """
        if not self.is_running(scraper_name):
            self.logger.warning(f"⚠️  {scraper_name} no está corriendo")
            return False
        
        process_info = self.active_processes[scraper_name]
        process = process_info['process']
        
        try:
            if force:
                self.logger.warning(f"🔪 Matando {scraper_name} (SIGKILL)")
                process.kill()
            else:
                self.logger.info(f"⏹️  Terminando {scraper_name} (SIGTERM)")
                process.terminate()
            
            # Esperar hasta 5 segundos a que termine
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                if not force:
                    # Si terminate no funcionó, forzar kill
                    self.logger.warning(f"⚠️  {scraper_name} no respondió a SIGTERM, forzando SIGKILL")
                    process.kill()
                    process.wait(timeout=2)
            
            # Limpiar del registro
            self.cleanup_finished()
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error terminando {scraper_name}: {e}")
            return False
    
    def kill_all(self, force: bool = False) -> int:
        """
        Termina todos los scrapers en ejecución.
        
        Args:
            force: Si True, usa SIGKILL, si False usa SIGTERM
        
        Returns:
            Número de procesos terminados
        """
        active_scrapers = self.get_active_scrapers()
        count = 0
        
        for scraper_name in active_scrapers:
            if self.kill_scraper(scraper_name, force):
                count += 1
        
        return count
    
    def print_status(self):
        """
        Imprime el estado actual de todos los scrapers (para debugging).
        """
        print("\n" + "="*70)
        print("ESTADO DE SCRAPERS")
        print("="*70)
        
        #edittt - Scrapers activos
        active = self.get_active_scrapers()
        if active:
            print(f"\n🔄 Scrapers Activos ({len(active)}):")
            for scraper_name in active:
                uptime = self.get_uptime(scraper_name)
                process_info = self.active_processes[scraper_name]
                print(f"  • {scraper_name}")
                print(f"    - PID: {process_info['pid']}")
                print(f"    - Uptime: {uptime:.1f}s")
                print(f"    - Inicio: {process_info['start_datetime'].strftime('%H:%M:%S')}")
                
                if self.is_stuck(scraper_name):
                    print(f"    - ⚠️  POSIBLE TIMEOUT (>{self.timeout_sec}s)")
        else:
            print("\n✅ No hay scrapers en ejecución")
        
        #edittt - Estadísticas
        if self.execution_stats:
            print(f"\n📊 Estadísticas de Ejecución:")
            for scraper_name, stats in self.execution_stats.items():
                print(f"\n  {scraper_name}:")
                print(f"    - Total ejecuciones: {stats['total_runs']}")
                print(f"    - Exitosas: {stats['successful_runs']}")
                print(f"    - Fallidas: {stats['failed_runs']}")
                if stats['avg_duration']:
                    print(f"    - Duración promedio: {stats['avg_duration']:.2f}s")
                if stats['last_duration']:
                    print(f"    - Última duración: {stats['last_duration']:.2f}s")
                if stats['last_exit_code'] is not None:
                    print(f"    - Último exit code: {stats['last_exit_code']}")
        
        print("\n" + "="*70 + "\n")
    
    def __repr__(self) -> str:
        """Representación string para debugging."""
        active_count = len(self.get_active_scrapers())
        total_runs = sum(s['total_runs'] for s in self.execution_stats.values())
        return (
            f"ScraperExecutor(active={active_count}, "
            f"total_runs={total_runs}, "
            f"scripts_dir={self.scripts_dir})"
        )


#edittt - Función de testing
def test_executor():
    """
    Función de prueba para verificar el executor.
    Ejecuta un script de prueba simple.
    """
    import tempfile
    import os
    
    print("\n" + "="*70)
    print("TEST DE SCRAPER EXECUTOR")
    print("="*70)
    
    #edittt - Crear script de prueba temporal
    with tempfile.TemporaryDirectory() as tmpdir:
        test_script = Path(tmpdir) / "test_scraper.py"
        test_script.write_text("""
import time
import sys

print("🧪 Test scraper iniciado")
time.sleep(2)
print("✅ Test scraper completado")
sys.exit(0)
""")
        
        #edittt - Crear executor
        executor = ScraperExecutor(scripts_dir=tmpdir, timeout_sec=10)
        
        print("\n1️⃣  Ejecutando test_scraper...")
        success = executor.execute('test_scraper', 'test_scraper.py')
        print(f"   Resultado: {'✅ Ejecutado' if success else '❌ Falló'}")
        
        print("\n2️⃣  Verificando que está corriendo...")
        is_running = executor.is_running('test_scraper')
        print(f"   is_running: {is_running}")
        
        print("\n3️⃣  Intentando ejecutar duplicado...")
        duplicate = executor.execute('test_scraper', 'test_scraper.py')
        print(f"   Permitió duplicado: {duplicate} (debería ser False)")
        
        print("\n4️⃣  Esperando a que termine (2 segundos)...")
        time.sleep(3)
        
        print("\n5️⃣  Limpiando procesos terminados...")
        cleaned = executor.cleanup_finished()
        print(f"   Limpiados: {cleaned}")
        
        print("\n6️⃣  Estado final:")
        executor.print_status()
    
    print("="*70 + "\n")


if __name__ == '__main__':
    #edittt - Configurar logging para testing
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] [%(name)s] %(levelname)s: %(message)s'
    )
    
    test_executor()