#Gestión de configuración del scheduler
"""
Scheduler Configuration Manager

Este módulo maneja:
1. Carga de config.yaml con validación
2. Autodetección de capacidades de CPU
3. Cálculo de intervalos base adaptativos
4. Fallbacks a valores por defecto seguros
"""

import yaml
import logging
import multiprocessing
from pathlib import Path
from typing import Dict, Any, Optional

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    logging.warning("psutil no disponible, funcionalidad de CPU limitada")


class SchedulerConfig:
    """
    Gestor de configuración del scheduler con autodetección de hardware.
    
    Prioridad de configuración (mayor a menor):
    1. Valores override en config.yaml
    2. Autodetección de hardware
    3. Valores por defecto hardcoded
    """
    
    #Valores por defecto si no hay config.yaml o falla la carga
    DEFAULT_CONFIG = {
        'machine': {
            'cores_override': None,
            'threads_override': None,
            'performance_tier': 'auto',
            'interval_adjustment_factor': 1.0
        },
        'jitter': {
            'k': 2,
            'min_factor': 0.5,
            'max_factor': 2.0
        },
        'scrapers': {
            'amber_chiapas': {
                'enabled': True,
                'estimated_duration_sec': 60,
                'base_interval_min': None,
                'min_interval_min': 1.5,
                'max_interval_min': 30,
                'priority': 'medium',
                'script_filename': 'paralelo_amber_chiapas.py'
            },
            'havistoa_chiapas': {
                'enabled': True,
                'estimated_duration_sec': 60,
                'base_interval_min': None,
                'min_interval_min': 1.5,
                'max_interval_min': 30,
                'priority': 'medium',
                'script_filename': 'paralelo_havistoa_chiapas.py'
            },
            'amber_nacional': {
                'enabled': True,
                'estimated_duration_sec': 400,
                'base_interval_min': None,
                'min_interval_min': 10,
                'max_interval_min': 60,
                'priority': 'high',
                'script_filename': 'paralelo_amber_nacional.py'
            }
        },
        'logging': {
            'level': 'INFO',
            'log_execution_times': True,
            'log_cpu_usage': True,
            'timestamp_format': 'iso'
        },
        'advanced': {
            'allow_concurrent_same_scraper': False,
            'scraper_timeout_sec': 900,
            'dynamic_interval_recalculation': True,
            'cleanup_interval_sec': 60
        }
    }
    
    def __init__(self, config_path: str = "config.yaml"):
        """
        Inicializa el gestor de configuración.
        
        Args:
            config_path: Ruta al archivo config.yaml
        """
        self.logger = logging.getLogger("SchedulerConfig")
        self.config_path = Path(config_path)
        
        #edittt - Cargar configuración
        self.config = self._load_config()
        
        #edittt - Detectar capacidades de CPU
        self.cpu_info = self._detect_cpu_capacity()
        
        #edittt - Validar configuración
        self._validate_config()
        
        self.logger.info("✅ Configuración cargada exitosamente")
    
    def _load_config(self) -> Dict[str, Any]:
        """
        Carga el archivo config.yaml con fallback a defaults.
        
        Returns:
            Diccionario con la configuración
        """
        #edittt - Intentar cargar YAML
        if self.config_path.exists():
            try:
                with open(self.config_path, 'r', encoding='utf-8') as f:
                    yaml_config = yaml.safe_load(f)
                
                if yaml_config is None:
                    self.logger.warning(f"⚠️  {self.config_path} está vacío, usando defaults")
                    return self.DEFAULT_CONFIG.copy()
                
                self.logger.info(f"📄 Config cargado desde {self.config_path}")
                
                #edittt - Merge con defaults (para keys faltantes)
                return self._deep_merge(self.DEFAULT_CONFIG.copy(), yaml_config)
                
            except yaml.YAMLError as e:
                self.logger.error(f"❌ Error parseando YAML: {e}")
                self.logger.warning("⚠️  Usando configuración por defecto")
                return self.DEFAULT_CONFIG.copy()
            except Exception as e:
                self.logger.error(f"❌ Error leyendo config: {e}")
                return self.DEFAULT_CONFIG.copy()
        else:
            self.logger.warning(f"⚠️  No se encontró {self.config_path}, usando defaults")
            return self.DEFAULT_CONFIG.copy()
    
    def _deep_merge(self, base: Dict, override: Dict) -> Dict:
        """
        Merge recursivo de diccionarios (override tiene prioridad).
        
        Args:
            base: Diccionario base
            override: Diccionario con valores a sobrescribir
            
        Returns:
            Diccionario mergeado
        """
        result = base.copy()
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._deep_merge(result[key], value)
            else:
                result[key] = value
        return result
    
    def _detect_cpu_capacity(self) -> Dict[str, Any]:
        """
        Detecta capacidades de CPU de la máquina.
        
        Returns:
            Diccionario con información de CPU:
            - cores: Número de cores físicos
            - threads: Número de threads lógicos
            - performance_score: Puntuación estimada (1-100)
        """
        #edittt - Obtener cores override o detectar
        cores_override = self.config['machine'].get('cores_override')
        threads_override = self.config['machine'].get('threads_override')
        
        if cores_override is not None:
            cores = cores_override
            threads = threads_override if threads_override is not None else cores * 2
            self.logger.info(f"🔧 Usando cores override: {cores} cores, {threads} threads")
        else:
            #edittt - Autodetección
            threads = multiprocessing.cpu_count()
            
            if PSUTIL_AVAILABLE:
                cores = psutil.cpu_count(logical=False) or threads
            else:
                #edittt - Sin psutil, asumimos cores = threads/2 (heurística)
                cores = max(1, threads // 2)
            
            self.logger.info(f"💻 CPU detectado: {cores} cores, {threads} threads")
        
        #edittt - Calcular performance score (heurística simple)
        # Fórmula: cores * 10 + (threads - cores) * 2
        # Ejemplos:
        #   4 cores, 4 threads  → 40 + 0  = 40 (sin hyperthreading)
        #   4 cores, 8 threads  → 40 + 8  = 48 (con hyperthreading)
        #   8 cores, 16 threads → 80 + 16 = 96 (muy potente)
        performance_score = min(100, cores * 10 + (threads - cores) * 2)
        
        return {
            'cores': cores,
            'threads': threads,
            'performance_score': performance_score
        }
    
    def _validate_config(self):
        """
        Valida que la configuración tenga valores sensatos.
        Corrige valores inválidos automáticamente.
        """
        #edittt - Validar jitter
        jitter = self.config['jitter']
        if jitter['min_factor'] <= 0:
            self.logger.warning("⚠️  min_factor <= 0, corrigiendo a 0.1")
            jitter['min_factor'] = 0.1
        
        if jitter['max_factor'] <= jitter['min_factor']:
            self.logger.warning("⚠️  max_factor <= min_factor, corrigiendo")
            jitter['max_factor'] = jitter['min_factor'] * 2
        
        if jitter['k'] <= 0:
            self.logger.warning("⚠️  k <= 0, corrigiendo a 5")
            jitter['k'] = 5
        
        #CHECKPOINTTTTTTTTTTTT
        #EDITTT - Validar jitter específico por scraper (con fallback al global)
        for scraper_name, scraper_config in self.config['scrapers'].items():
            local_jitter = scraper_config.get('jitter')
            if local_jitter is not None:
                # Rellenar claves faltantes con valores globales
                if 'k' not in local_jitter:
                    local_jitter['k'] = jitter['k']
                if 'min_factor' not in local_jitter:
                    local_jitter['min_factor'] = jitter['min_factor']
                if 'max_factor' not in local_jitter:
                    local_jitter['max_factor'] = jitter['max_factor']
                
                # Validar valores locales
                if local_jitter['min_factor'] <= 0:
                    self.logger.warning(
                        f"⚠️  {scraper_name}.jitter.min_factor <= 0, corrigiendo a 0.1"
                    )
                    local_jitter['min_factor'] = 0.1
                
                if local_jitter['max_factor'] <= local_jitter['min_factor']:
                    self.logger.warning(
                        f"⚠️  {scraper_name}.jitter.max_factor <= min_factor, corrigiendo"
                    )
                    local_jitter['max_factor'] = local_jitter['min_factor'] * 2
                
                if local_jitter['k'] <= 0:
                    self.logger.warning(
                        f"⚠️  {scraper_name}.jitter.k <= 0, corrigiendo a 5"
                    )
                    local_jitter['k'] = 5



        #edittt - Validar scrapers
        for scraper_name, scraper_config in self.config['scrapers'].items():
            #edittt - Validar intervalos
            if scraper_config['min_interval_min'] <= 0:
                self.logger.warning(f"⚠️  {scraper_name}: min_interval_min inválido, usando 5")
                scraper_config['min_interval_min'] = 5
            
            if scraper_config['max_interval_min'] <= scraper_config['min_interval_min']:
                self.logger.warning(f"⚠️  {scraper_name}: max_interval_min inválido, corrigiendo")
                scraper_config['max_interval_min'] = scraper_config['min_interval_min'] * 3
            
            #edittt - Validar duración estimada
            duration_min = scraper_config['estimated_duration_sec'] / 60
            if scraper_config['min_interval_min'] < duration_min * 1.5:
                self.logger.warning(
                    f"⚠️  {scraper_name}: min_interval ({scraper_config['min_interval_min']}m) "
                    f"muy corto para duración estimada ({duration_min:.1f}m). "
                    f"Recomendado: >{duration_min * 1.5:.1f}m"
                )
    
    def get_machine_capacity(self) -> Dict[str, Any]:
        """
        Retorna información de capacidad de la máquina.
        
        Returns:
            Dict con cores, threads y performance_score
        """
        return self.cpu_info.copy()
    
    def get_scraper_config(self, scraper_name: str) -> Optional[Dict[str, Any]]:
        """
        Obtiene la configuración de un scraper específico.
        
        Args:
            scraper_name: Nombre del scraper (ej: 'amber_chiapas')
            
        Returns:
            Diccionario con configuración o None si no existe
        """
        return self.config['scrapers'].get(scraper_name)
    
    #EDITTT - Helper para obtener jitter efectivo (global + override por scraper)
    def get_jitter_config(self, scraper_name: Optional[str] = None) -> Dict[str, float]:
        """
        Obtiene la configuración de jitter efectiva.
        
        Si se pasa un nombre de scraper:
          - Usa jitter específico del scraper (si existe),
          - Completando claves faltantes con la config global.
        
        Si scraper_name es None:
          - Retorna la configuración global de jitter.
        
        Returns:
            Diccionario con claves: 'k', 'min_factor', 'max_factor'
        """
        global_jitter = self.config.get('jitter', {})
        # Valores por defecto de ultra-seguridad si algo falta
        default_k = global_jitter.get('k', 2.0)
        default_min = global_jitter.get('min_factor', 0.5)
        default_max = global_jitter.get('max_factor', 2.0)
        
        if scraper_name is None:
            return {
                'k': float(default_k),
                'min_factor': float(default_min),
                'max_factor': float(default_max),
            }
        
        scraper_cfg = self.get_scraper_config(scraper_name) or {}
        local_jitter = scraper_cfg.get('jitter', {})
        
        k = local_jitter.get('k', default_k)
        min_factor = local_jitter.get('min_factor', default_min)
        max_factor = local_jitter.get('max_factor', default_max)
        
        return {
            'k': float(k),
            'min_factor': float(min_factor),
            'max_factor': float(max_factor),
        }



    def calculate_base_interval(self, scraper_name: str) -> float:
        """
        Calcula el intervalo base para un scraper.
        
        Estrategia de cálculo:
        1. Si base_interval_min está especificado en config, usar ese
        2. Si no, calcular basándose en:
           - Duración estimada del scraper
           - Capacidad de CPU (performance_score)
           - interval_adjustment_factor
        3. Aplicar clamp entre min_interval_min y max_interval_min
        
        Args:
            scraper_name: Nombre del scraper
            
        Returns:
            Intervalo base en minutos
        """
        scraper_config = self.get_scraper_config(scraper_name)
        if not scraper_config:
            self.logger.error(f"❌ Scraper '{scraper_name}' no encontrado en config")
            return 30.0  # Fallback seguro
        
        #edittt - Caso 1: Intervalo manual especificado
        if scraper_config['base_interval_min'] is not None:
            interval = scraper_config['base_interval_min']
            self.logger.debug(f"📌 {scraper_name}: usando intervalo manual {interval} min")
            return self._clamp_interval(interval, scraper_config)
        
        #edittt - Caso 2: Cálculo automático
        duration_min = scraper_config['estimated_duration_sec'] / 60
        performance_score = self.cpu_info['performance_score']
        
        #edittt - Fórmula de cálculo adaptativo
        # Base: duración * safety_factor
        # safety_factor disminuye con mejor CPU (2.5 a 1.5)
        # Mapeo: score 20 → 2.5x, score 100 → 1.5x
        safety_factor = 2.5 - (performance_score / 100) * 1.0
        base_interval = duration_min * safety_factor
        
        #edittt - Aplicar performance tier si no es auto
        tier = self.config['machine'].get('performance_tier', 'auto')
        if tier != 'auto':
            tier_multipliers = {
                'low': 2.0,     # Máquina débil → intervalos más largos
                'medium': 1.5,
                'high': 1.0     # Máquina potente → intervalos más cortos
            }
            base_interval *= tier_multipliers.get(tier, 1.5)
            self.logger.debug(f"🎚️  {scraper_name}: aplicando tier '{tier}'")
        
        #edittt - Aplicar factor de ajuste manual
        adjustment = self.config['machine'].get('interval_adjustment_factor', 1.0)
        base_interval *= adjustment
        
        #edittt - Clamp entre límites configurados
        clamped_interval = self._clamp_interval(base_interval, scraper_config)
        
        self.logger.info(
            f"🧮 {scraper_name}: intervalo calculado = {base_interval:.2f} min "
            f"(clamped: {clamped_interval:.2f} min)"
        )
        
        return clamped_interval
    
    def _clamp_interval(self, interval: float, scraper_config: Dict) -> float:
        """
        Aplica límites min/max a un intervalo.
        
        Args:
            interval: Intervalo a limitar
            scraper_config: Config del scraper con límites
            
        Returns:
            Intervalo limitado
        """
        min_val = scraper_config['min_interval_min']
        max_val = scraper_config['max_interval_min']
        return max(min_val, min(max_val, interval))
    
    def get_enabled_scrapers(self) -> list:
        """
        Retorna lista de nombres de scrapers habilitados.
        
        Returns:
            Lista de strings con nombres de scrapers
        """
        return [
            name for name, config in self.config['scrapers'].items()
            if config.get('enabled', True)
        ]
    
    def get_logging_config(self) -> Dict[str, Any]:
        """
        Retorna configuración de logging.
        
        Returns:
            Diccionario con configuración de logs
        """
        return self.config['logging'].copy()
    
    def get_advanced_config(self) -> Dict[str, Any]:
        """
        Retorna configuración avanzada.
        
        Returns:
            Diccionario con configuración avanzada
        """
        return self.config['advanced'].copy()
    
    def should_allow_concurrent_execution(self) -> bool:
        """
        Determina si se permiten ejecuciones concurrentes del mismo scraper.
        
        Returns:
            True si se permite, False si no
        """
        return self.config['advanced'].get('allow_concurrent_same_scraper', False)
    
    def get_scraper_timeout(self) -> Optional[int]:
        """
        Obtiene el timeout configurado para scrapers.
        
        Returns:
            Timeout en segundos o None si no hay límite
        """
        return self.config['advanced'].get('scraper_timeout_sec')
    
    def should_recalculate_intervals(self) -> bool:
        """
        Determina si los intervalos deben recalcularse dinámicamente.
        
        Returns:
            True si debe recalcular, False si intervalo fijo
        """
        return self.config['advanced'].get('dynamic_interval_recalculation', True)
    
    def get_cleanup_interval(self) -> int:
        """
        Obtiene el intervalo de limpieza de procesos.
        
        Returns:
            Intervalo en segundos
        """
        return self.config['advanced'].get('cleanup_interval_sec', 60)
    
    def __repr__(self) -> str:
        """Representación string del config para debugging."""
        return (
            f"SchedulerConfig("
            f"cores={self.cpu_info['cores']}, "
            f"threads={self.cpu_info['threads']}, "
            f"score={self.cpu_info['performance_score']}, "
            f"scrapers_enabled={len(self.get_enabled_scrapers())}"
            f")"
        )


#edittt - Función helper para testing rápido
def test_config():
    """Función de prueba para verificar carga de configuración."""
    logging.basicConfig(level=logging.INFO)
    config = SchedulerConfig()
    
    print("\n" + "="*60)
    print("CONFIGURACIÓN DEL SCHEDULER")
    print("="*60)
    
    print(f"\n💻 CPU Info:")
    cpu = config.get_machine_capacity()
    print(f"   - Cores: {cpu['cores']}")
    print(f"   - Threads: {cpu['threads']}")
    print(f"   - Performance Score: {cpu['performance_score']}/100")
    
    print(f"\n📋 Scrapers Habilitados:")
    for scraper_name in config.get_enabled_scrapers():
        interval = config.calculate_base_interval(scraper_name)
        scraper_cfg = config.get_scraper_config(scraper_name)
        print(f"   - {scraper_name}:")
        print(f"     • Intervalo base: {interval:.2f} min")
        print(f"     • Duración estimada: {scraper_cfg['estimated_duration_sec']}s")
        print(f"     • Prioridad: {scraper_cfg['priority']}")

         #EDITTT - Mostrar jitter efectivo por scraper
        jitter_cfg = config.get_jitter_config(scraper_name)
        print(
            f"     • Jitter: k={jitter_cfg['k']}, "
            f"rango=[{jitter_cfg['min_factor']}x, {jitter_cfg['max_factor']}x]"
        )
    
    print(f"\n🎲 Jitter GLOBAL:")
    #EDITTT - Mostrar jitter global explícitamente
    global_jitter = config.get_jitter_config()
    print(f"   - k: {global_jitter['k']}")
    print(f"   - Rango: [{global_jitter['min_factor']}x, {global_jitter['max_factor']}x]")
    
    print(f"\n⚙️  Advanced:")
    adv = config.get_advanced_config()
    print(f"   - Concurrent same scraper: {adv['allow_concurrent_same_scraper']}")
    print(f"   - Dynamic recalc: {adv['dynamic_interval_recalculation']}")
    print(f"   - Timeout: {adv['scraper_timeout_sec']}s")
    
    print("\n" + "="*60 + "\n")


if __name__ == '__main__':
    #edittt - Permitir ejecutar directamente para testing
    test_config()