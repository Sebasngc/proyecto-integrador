"""
schema.py — Contrato de datos compartido por los pipelines RDD y DataFrame.

Que ambos pipelines importen el MISMO esquema y las MISMAS constantes es lo que
hace que la comparación sea justa: la única variable entre ellos es la API de
Spark, no el parsing ni los umbrales.

Dataset: telemetría de sensores industriales ya normalizada por el módulo GPU.
    sensor_id : str    identificador del sensor
    region    : str    zona geográfica (clave de agregación, ~20 valores)
    ts        : long   epoch en segundos
    value     : double valor original
    value_norm: double valor normalizado (z-score) por el kernel CUDA
    quality   : int    0..100, indicador de calidad de la lectura
"""

from __future__ import annotations

from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType, IntegerType,
)

COLUMNS = ["sensor_id", "region", "ts", "value", "value_norm", "quality"]

SCHEMA = StructType([
    StructField("sensor_id", StringType(), nullable=False),
    StructField("region", StringType(), nullable=False),
    StructField("ts", LongType(), nullable=False),
    StructField("value", DoubleType(), nullable=True),
    StructField("value_norm", DoubleType(), nullable=True),
    StructField("quality", IntegerType(), nullable=True),
])

# --- Parámetros de negocio (idénticos en ambos pipelines) ---
QUALITY_THRESHOLD = 60      # se descartan lecturas por debajo
ANOMALY_SIGMA = 3.0         # |z| > 3 se considera anomalía
TOP_K = 20                  # sensores más anómalos a devolver

# Tabla de dimensión pequeña: se difunde (broadcast) en ambos pipelines para que
# el join no introduzca un shuffle que sesgue la comparación.
REGION_DIM = {
    "eu-west":   {"continent": "EU", "sla_ms": 250,  "tier": "gold"},
    "eu-north":  {"continent": "EU", "sla_ms": 300,  "tier": "silver"},
    "us-east":   {"continent": "NA", "sla_ms": 200,  "tier": "gold"},
    "us-west":   {"continent": "NA", "sla_ms": 220,  "tier": "gold"},
    "sa-east":   {"continent": "SA", "sla_ms": 400,  "tier": "bronze"},
    "ap-south":  {"continent": "AS", "sla_ms": 350,  "tier": "silver"},
    "ap-north":  {"continent": "AS", "sla_ms": 320,  "tier": "silver"},
    "af-south":  {"continent": "AF", "sla_ms": 500,  "tier": "bronze"},
}
DEFAULT_DIM = {"continent": "??", "sla_ms": 999, "tier": "unknown"}


def spark_session(app_name: str, extra: dict | None = None):
    """Sesión con configuración idéntica para los dos pipelines.

    AQE se DESACTIVA a propósito: reparticiona dinámicamente sólo en el camino
    DataFrame, así que dejarlo activo mediría "DataFrame + AQE" contra "RDD sin
    AQE" y exageraría el speedup. Se mide con AQE off (comparación limpia) y se
    documenta aparte la ganancia adicional que aporta.
    """
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.parquet.compression.codec", "snappy")
    )
    for k, v in (extra or {}).items():
        builder = builder.config(k, v)
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("WARN")
    return session
