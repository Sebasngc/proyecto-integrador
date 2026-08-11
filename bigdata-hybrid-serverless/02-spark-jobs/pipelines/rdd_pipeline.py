"""
rdd_pipeline.py — Pipeline analítico implementado con la API RDD (baja nivel).

Semántica (idéntica al pipeline DataFrame):
  1. leer el dataset preprocesado
  2. descartar lecturas con quality < QUALITY_THRESHOLD
  3. agregar por (region, sensor_id): n, media, desviación, min, max, anomalías
  4. enriquecer con la tabla de dimensión de regiones (broadcast)
  5. quedarse con el TOP_K por ratio de anomalías
  6. escribir el resultado

Qué se paga por usar RDD:
  * Spark trata la lambda de Python como una caja negra: no hay Catalyst, no hay
    poda de columnas ni pushdown de predicados.
  * Cada fila cruza la frontera JVM <-> Python y se serializa con pickle
    (el famoso coste del "Python UDF"): dos copias y una deserialización por fila.
  * Los datos viven como objetos Java/Python en el heap, no en el formato
    columnar off-heap de Tungsten -> más presión de GC.
  * reduceByKey sí combina en el lado del mapa (a diferencia de groupByKey), así
    que la implementación es la versión *buena* del camino RDD: el objetivo es
    comparar contra un rival digno, no contra un hombre de paja.

Uso:
    spark-submit --master local[*] pipelines/rdd_pipeline.py \
        --input data/sensors.parquet --output out/rdd --runs 3
"""

from __future__ import annotations

import argparse
import json
import math
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.schema import (  # noqa: E402
    ANOMALY_SIGMA, DEFAULT_DIM, QUALITY_THRESHOLD, REGION_DIM, TOP_K, spark_session,
)

ZERO = (0, 0.0, 0.0, float("inf"), float("-inf"), 0)  # n, sum, sumsq, min, max, anomalías


def _parse_row(row) -> tuple:
    """Convierte una Row de Spark en una tupla plana de Python."""
    return (row["sensor_id"], row["region"], row["value_norm"], row["quality"])


def _seq_op(acc: tuple, value_norm: float) -> tuple:
    """Combina un acumulador con un valor nuevo (fase map-side)."""
    n, s, sq, mn, mx, anom = acc
    return (
        n + 1,
        s + value_norm,
        sq + value_norm * value_norm,
        value_norm if value_norm < mn else mn,
        value_norm if value_norm > mx else mx,
        anom + (1 if abs(value_norm) > ANOMALY_SIGMA else 0),
    )


def _comb_op(a: tuple, b: tuple) -> tuple:
    """Fusiona dos acumuladores (fase reduce, tras el shuffle)."""
    return (a[0] + b[0], a[1] + b[1], a[2] + b[2],
            min(a[3], b[3]), max(a[4], b[4]), a[5] + b[5])


def _finalize(key: tuple, acc: tuple, dim_bc) -> dict:
    region, sensor_id = key
    n, s, sq, mn, mx, anom = acc
    mean = s / n
    var = max(0.0, sq / n - mean * mean)
    dim = dim_bc.value.get(region, DEFAULT_DIM)
    return {
        "region": region,
        "sensor_id": sensor_id,
        "readings": n,
        "mean_norm": mean,
        "stddev_norm": math.sqrt(var),
        "min_norm": mn,
        "max_norm": mx,
        "anomalies": anom,
        "anomaly_ratio": anom / n,
        "continent": dim["continent"],
        "tier": dim["tier"],
        "sla_ms": dim["sla_ms"],
    }


def run(spark, input_path: str, output_path: str | None) -> dict:
    sc = spark.sparkContext
    dim_bc = sc.broadcast(REGION_DIM)

    # Se lee con el lector de Spark y se baja a RDD inmediatamente: así ambos
    # pipelines parten del mismo fichero y del mismo coste de E/S.
    if input_path.endswith(".parquet") or Path(input_path).is_dir():
        base = spark.read.parquet(input_path)
    else:
        from common.schema import SCHEMA
        base = spark.read.schema(SCHEMA).option("header", "true").csv(input_path)

    rdd = base.rdd.map(_parse_row)

    filtered = rdd.filter(lambda r: r[3] is not None and r[3] >= QUALITY_THRESHOLD)
    keyed = filtered.map(lambda r: ((r[1], r[0]), r[2] if r[2] is not None else 0.0))

    # aggregateByKey = combina en el mapa antes del shuffle (como reduceByKey pero
    # permitiendo que el acumulador tenga un tipo distinto al valor).
    aggregated = keyed.aggregateByKey(ZERO, _seq_op, _comb_op)

    enriched = aggregated.map(lambda kv: _finalize(kv[0], kv[1], dim_bc))

    # takeOrdered hace la selección top-k con una cola de prioridad por partición:
    # evita ordenar globalmente los millones de claves.
    top = enriched.takeOrdered(TOP_K, key=lambda d: (-d["anomaly_ratio"], -d["readings"]))

    total_groups = enriched.count()
    total_anomalies = enriched.map(lambda d: d["anomalies"]).sum()

    if output_path:
        spark.createDataFrame(enriched).write.mode("overwrite").parquet(output_path)

    return {"groups": total_groups, "anomalies": int(total_anomalies), "top": top}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", default=None)
    ap.add_argument("--runs", type=int, default=1)
    ap.add_argument("--metrics", default=None, help="fichero JSON de salida con los tiempos")
    args = ap.parse_args()

    spark = spark_session("rdd-pipeline")
    times, result = [], None
    for i in range(args.runs):
        t0 = time.perf_counter()
        result = run(spark, args.input, args.output if i == args.runs - 1 else None)
        elapsed = time.perf_counter() - t0
        times.append(elapsed)
        print(f"[rdd] ejecución {i + 1}/{args.runs}: {elapsed:.2f} s", flush=True)

    payload = {
        "pipeline": "rdd",
        "input": args.input,
        "runs": args.runs,
        "times_s": times,
        "median_s": sorted(times)[len(times) // 2],
        "min_s": min(times),
        "groups": result["groups"],
        "anomalies": result["anomalies"],
        "top": result["top"][:5],
        "spark_version": spark.version,
    }
    print(json.dumps(payload, indent=2, default=str))
    if args.metrics:
        Path(args.metrics).write_text(json.dumps(payload, indent=2, default=str))
    spark.stop()


if __name__ == "__main__":
    main()
