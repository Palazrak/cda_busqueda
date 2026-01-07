#edittt - Archivo nuevo: Cálculo de jitter log-normal para intervalos adaptativos
"""
Jitter Calculator - Distribución Log-Normal

Este módulo implementa aleatorización de intervalos usando distribución log-normal
con clamp para evitar valores extremos. La distribución log-normal es más "natural"
que la uniforme porque:
1. La mayoría de valores se concentran cerca de la media
2. Permite valores extremos ocasionales (más humano)
3. Dificulta la detección de patrones

Ejemplo de uso:
    base_interval = 15.0  # minutos
    interval = calcular_intervalo_con_jitter(
        base_interval,
        k=10,
        min_factor=0.5,
        max_factor=2.0
    )
    # Resultado: entre 7.5 y 30 minutos, la mayoría cerca de 15
"""

import random
import math
import logging
from typing import Optional


#edittt - Función principal de cálculo de jitter
def calcular_intervalo_con_jitter(
    intervalo_base_min: float,
    k: float = 10.0,
    min_factor: float = 0.5,
    max_factor: float = 2.0,
    logger: Optional[logging.Logger] = None
) -> float:
    """
    Calcula un intervalo con jitter log-normal aplicado.
    
    La distribución log-normal genera valores aleatorios donde:
    - La media es igual al intervalo base (μ = 0, σ ajustado)
    - Los valores se concentran cerca de la media
    - Valores extremos son posibles pero raros
    - Se aplica clamp para evitar valores fuera de rango aceptable
    
    Fórmula:
        sigma = k / intervalo_base_min
        mu = -(sigma^2) / 2  (para que E[X] = 1)
        multiplicador = lognormvariate(mu, sigma)
        multiplicador_clamped = clamp(multiplicador, min_factor, max_factor)
        intervalo_final = intervalo_base_min * multiplicador_clamped
    
    Args:
        intervalo_base_min: Intervalo base en minutos (ej: 15.0)
        k: Factor de variabilidad (mayor = más jitter)
           - k=5:  Poco jitter (~80% entre 0.7x-1.3x)
           - k=10: Medio jitter (~80% entre 0.5x-1.8x) [DEFAULT]
           - k=15: Alto jitter (~80% entre 0.3x-2.5x)
        min_factor: Multiplicador mínimo permitido (ej: 0.5 = 50% del base)
        max_factor: Multiplicador máximo permitido (ej: 2.0 = 200% del base)
        logger: Logger opcional para debugging
    
    Returns:
        Intervalo en minutos con jitter aplicado, garantizado entre
        [intervalo_base_min * min_factor, intervalo_base_min * max_factor]
    
    Raises:
        ValueError: Si los parámetros son inválidos
    
    Examples:
        >>> # Intervalo base de 20 minutos con jitter moderado
        >>> calcular_intervalo_con_jitter(20.0, k=10, min_factor=0.5, max_factor=2.0)
        18.3  # Varía cada llamada
        
        >>> # Jitter conservador (menos variación)
        >>> calcular_intervalo_con_jitter(30.0, k=5, min_factor=0.8, max_factor=1.2)
        31.2  # Entre 24 y 36 minutos
        
        >>> # Jitter agresivo (mucha variación)
        >>> calcular_intervalo_con_jitter(15.0, k=20, min_factor=0.3, max_factor=3.0)
        42.1  # Entre 4.5 y 45 minutos
    """
    #edittt - Validación de parámetros
    if intervalo_base_min <= 0:
        raise ValueError(f"intervalo_base_min debe ser > 0, recibido: {intervalo_base_min}")
    
    if k <= 0:
        raise ValueError(f"k debe ser > 0, recibido: {k}")
    
    if min_factor <= 0:
        raise ValueError(f"min_factor debe ser > 0, recibido: {min_factor}")
    
    if max_factor <= min_factor:
        raise ValueError(
            f"max_factor ({max_factor}) debe ser > min_factor ({min_factor})"
        )
    
    #edittt - Cálculo de parámetros de la distribución log-normal
    # sigma controla la dispersión de la distribución
    sigma = k / intervalo_base_min
    
    # mu se ajusta para que la media de la distribución sea 1
    # Esto garantiza que E[multiplicador] ≈ 1.0
    # Fórmula: para que E[exp(X)] = 1 donde X ~ N(mu, sigma^2)
    #          necesitamos mu = -(sigma^2) / 2
    mu = -(sigma ** 2) / 2
    
    #edittt - Generar multiplicador aleatorio con distribución log-normal
    # random.lognormvariate(mu, sigma) genera un valor de:
    #   X = exp(Y) donde Y ~ N(mu, sigma^2)
    # Es equivalente a np.random.lognormal(mu, sigma) pero sin numpy
    try:
        multiplicador = random.lognormvariate(mu, sigma)
    except (ValueError, OverflowError) as e:
        # En casos extremos (sigma muy grande), puede haber overflow
        # Fallback a un valor seguro cerca de 1.0
        if logger:
            logger.warning(f"⚠️  Overflow en lognormvariate (k={k}), usando fallback")
        multiplicador = random.uniform(min_factor, max_factor)
    
    #edittt - Aplicar clamp para evitar valores extremos
    # Sin clamp, lognormvariate puede dar valores como 0.001 o 100
    multiplicador_clamped = max(min_factor, min(max_factor, multiplicador))
    
    #edittt - Calcular intervalo final
    intervalo_final = intervalo_base_min * multiplicador_clamped
    
    #edittt - Logging opcional para debugging
    if logger:
        if multiplicador != multiplicador_clamped:
            logger.debug(
                f"🎲 Jitter: base={intervalo_base_min:.2f}m, "
                f"mult={multiplicador:.3f} (clamped→{multiplicador_clamped:.3f}), "
                f"final={intervalo_final:.2f}m"
            )
        else:
            logger.debug(
                f"🎲 Jitter: base={intervalo_base_min:.2f}m, "
                f"mult={multiplicador:.3f}, "
                f"final={intervalo_final:.2f}m"
            )
    
    return intervalo_final


#edittt - Función auxiliar para calcular estadísticas de la distribución
def calcular_estadisticas_jitter(
    intervalo_base_min: float,
    k: float = 10.0,
    min_factor: float = 0.5,
    max_factor: float = 2.0,
    muestras: int = 10000
) -> dict:
    """
    Genera estadísticas sobre la distribución de intervalos con jitter.
    
    Útil para:
    - Entender el comportamiento del jitter con diferentes parámetros
    - Validar que los rangos son los esperados
    - Ajustar k, min_factor, max_factor según necesidades
    
    Args:
        intervalo_base_min: Intervalo base en minutos
        k: Factor de variabilidad
        min_factor: Multiplicador mínimo
        max_factor: Multiplicador máximo
        muestras: Número de muestras a generar (más = más preciso)
    
    Returns:
        Diccionario con estadísticas:
        - mean: Media de los intervalos generados
        - median: Mediana
        - min: Mínimo observado
        - max: Máximo observado
        - std: Desviación estándar
        - percentiles: p10, p25, p75, p90
        - clamp_ratio: % de valores que fueron clamped
    
    Example:
        >>> stats = calcular_estadisticas_jitter(15.0, k=10, muestras=10000)
        >>> print(f"Rango típico: {stats['p10']:.1f} - {stats['p90']:.1f} min")
        Rango típico: 8.2 - 26.7 min
    """
    intervalos = []
    clamp_count = 0
    
    for _ in range(muestras):
        sigma = k / intervalo_base_min
        mu = -(sigma ** 2) / 2
        
        try:
            multiplicador = random.lognormvariate(mu, sigma)
            multiplicador_original = multiplicador
            multiplicador = max(min_factor, min(max_factor, multiplicador))
            
            if multiplicador != multiplicador_original:
                clamp_count += 1
            
            intervalo = intervalo_base_min * multiplicador
            intervalos.append(intervalo)
        except (ValueError, OverflowError):
            # Fallback en caso de overflow
            intervalo = intervalo_base_min * random.uniform(min_factor, max_factor)
            intervalos.append(intervalo)
            clamp_count += 1
    
    #edittt - Calcular estadísticas
    intervalos.sort()
    n = len(intervalos)
    
    mean = sum(intervalos) / n
    median = intervalos[n // 2]
    min_val = intervalos[0]
    max_val = intervalos[-1]
    
    # Desviación estándar
    variance = sum((x - mean) ** 2 for x in intervalos) / n
    std = math.sqrt(variance)
    
    # Percentiles
    p10 = intervalos[int(n * 0.10)]
    p25 = intervalos[int(n * 0.25)]
    p75 = intervalos[int(n * 0.75)]
    p90 = intervalos[int(n * 0.90)]
    
    clamp_ratio = clamp_count / muestras
    
    return {
        'mean': mean,
        'median': median,
        'min': min_val,
        'max': max_val,
        'std': std,
        'percentiles': {
            'p10': p10,
            'p25': p25,
            'p75': p75,
            'p90': p90
        },
        'clamp_ratio': clamp_ratio,
        'expected_range': (intervalo_base_min * min_factor, intervalo_base_min * max_factor)
    }


#edittt - Función helper para testing y visualización
def test_jitter_distribution():
    """
    Función de prueba para visualizar la distribución de jitter.
    Genera múltiples muestras y muestra estadísticas.
    """
    print("\n" + "="*70)
    print("ANÁLISIS DE DISTRIBUCIÓN LOG-NORMAL CON JITTER")
    print("="*70)
    
    # Configuraciones a probar
    configs = [
        {"base": 10.0, "k": 5, "min": 0.7, "max": 1.3, "desc": "Conservador"},
        {"base": 10.0, "k": 10, "min": 0.5, "max": 2.0, "desc": "Moderado [DEFAULT]"},
        {"base": 10.0, "k": 15, "min": 0.3, "max": 2.5, "desc": "Agresivo"},
        {"base": 10.0, "k": 3, "min": 0.5, "max": 2, "desc": "CONFIGURADO Nacional"},
        {"base": 3, "k": 1, "min": 0.5, "max": 2.1, "desc": "CONFIGURADO Chiapas"},
    ]
    
    for config in configs:
        print(f"\n{'─'*70}")
        print(f"Configuración: {config['desc']}")
        print(f"  Base: {config['base']} min, k={config['k']}, "
              f"rango=[{config['min']}x, {config['max']}x]")
        print(f"{'─'*70}")
        
        stats = calcular_estadisticas_jitter(
            config['base'],
            k=config['k'],
            min_factor=config['min'],
            max_factor=config['max'],
            muestras=10000
        )
        
        print(f"\n📊 Estadísticas (10,000 muestras):")
        print(f"  Media:    {stats['mean']:.2f} min")
        print(f"  Mediana:  {stats['median']:.2f} min")
        print(f"  Std Dev:  {stats['std']:.2f} min")
        print(f"  Rango:    [{stats['min']:.2f}, {stats['max']:.2f}] min")
        print(f"\n📈 Percentiles:")
        print(f"  P10: {stats['percentiles']['p10']:.2f} min")
        print(f"  P25: {stats['percentiles']['p25']:.2f} min")
        print(f"  P75: {stats['percentiles']['p75']:.2f} min")
        print(f"  P90: {stats['percentiles']['p90']:.2f} min")
        print(f"\n🔒 Clamp ratio: {stats['clamp_ratio']*100:.1f}% de valores fueron clamped")
        print(f"  Rango esperado: [{stats['expected_range'][0]:.2f}, "
              f"{stats['expected_range'][1]:.2f}] min")
        
        # Visualización ASCII de histograma simplificado
        print(f"\n📉 Distribución (histograma simplificado):")
        print_histogram(config['base'], config['k'], config['min'], config['max'])
    
    print("\n" + "="*70 + "\n")


def print_histogram(base: float, k: float, min_f: float, max_f: float, bins: int = 20):
    """
    Imprime un histograma ASCII de la distribución.
    
    Args:
        base: Intervalo base
        k: Factor k
        min_f: min_factor
        max_f: max_factor
        bins: Número de barras en el histograma
    """
    # Generar muestras
    samples = []
    for _ in range(1000):
        try:
            sigma = k / base
            mu = -(sigma ** 2) / 2
            mult = random.lognormvariate(mu, sigma)
            mult = max(min_f, min(max_f, mult))
            samples.append(base * mult)
        except (ValueError, OverflowError):
            samples.append(base * random.uniform(min_f, max_f))
    
    # Crear bins
    min_val = base * min_f
    max_val = base * max_f
    bin_width = (max_val - min_val) / bins
    
    counts = [0] * bins
    for sample in samples:
        bin_idx = int((sample - min_val) / bin_width)
        bin_idx = max(0, min(bins - 1, bin_idx))
        counts[bin_idx] += 1
    
    # Normalizar para display
    max_count = max(counts)
    bar_width = 40
    
    for i in range(bins):
        bin_start = min_val + i * bin_width
        bin_end = bin_start + bin_width
        bar_len = int((counts[i] / max_count) * bar_width)
        bar = "█" * bar_len
        print(f"  [{bin_start:5.1f}-{bin_end:5.1f}] {bar} {counts[i]}")


#edittt - Función auxiliar para validar configuración de jitter
def validar_config_jitter(k: float, min_factor: float, max_factor: float) -> tuple:
    """
    Valida que los parámetros de jitter sean sensatos.
    
    Args:
        k: Factor de variabilidad
        min_factor: Multiplicador mínimo
        max_factor: Multiplicador máximo
    
    Returns:
        Tuple (es_valido: bool, mensaje: str)
    
    Example:
        >>> es_valido, msg = validar_config_jitter(10, 0.5, 2.0)
        >>> print(es_valido)
        True
    """
    if k <= 0:
        return False, f"k debe ser > 0 (recibido: {k})"
    
    if k > 30:
        return False, f"k muy alto ({k}), recomendado < 30 para estabilidad"
    
    if min_factor <= 0:
        return False, f"min_factor debe ser > 0 (recibido: {min_factor})"
    
    if min_factor > 1.0:
        return False, f"min_factor > 1.0 ({min_factor}) no permite intervalos más cortos"
    
    if max_factor <= min_factor:
        return False, f"max_factor ({max_factor}) debe ser > min_factor ({min_factor})"
    
    if max_factor > 5.0:
        return False, f"max_factor muy alto ({max_factor}), recomendado < 5.0"
    
    if max_factor / min_factor > 10:
        return False, (
            f"Rango muy amplio (min={min_factor}, max={max_factor}), "
            f"ratio={max_factor/min_factor:.1f}x. Recomendado < 10x"
        )
    
    return True, "Configuración válida"


if __name__ == '__main__':
    #edittt - Ejecutar tests si se corre directamente
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == 'test':
        # Modo test completo con estadísticas
        test_jitter_distribution()
    else:
        # Modo demo rápido
        print("\n🎲 Demo de Jitter Calculator\n")
        print("Generando 20 intervalos con jitter a partir de base=10 min:\n")
        
        for i in range(20):
            intervalo = calcular_intervalo_con_jitter(
                10.0,
                k=2,
                min_factor=0.5,
                max_factor=2.0
            )
            print(f"  {i+1:2d}. {intervalo:5.2f} min")
        
        print(f"\n💡 Para análisis detallado ejecuta: python {sys.argv[0]} test\n")