"""
dataframe_pipeline.py — El mismo análisis con la API DataFrame / Spark SQL.

Por qué debería ganar (y qué hay que enseñar en el informe con `explain()`):

  1. Catalyst reordena el plan lógico: empuja el filtro de `quality` hasta la
     lectura (predicate pushdown) y sólo materializa las 4 columnas que se usan
     (column pruning). Sobre Parquet esto se traduce en leer menos bytes del
     disco/objeto, no sólo en procesar menos filas.
  2. Tungsten genera bytecode para la etapa completa (whole-stage codegen): el
     filtro, la proyección y el agregado parcial se compilan en un único bucle
     sobre memoria off-heap, sin objetos intermedios ni virtual dispatch.
  3. Nada cruza la frontera JVM<->Python: no hay pickle por fila. La ganancia
     frente a RDD en PySpark viene sobre todo de aquí.
  4. `broadcast()` explícito fija la estrategia de join y evita un shuffle.

Uso:
    spark-submit --master local[*] pipelines/dataframe_pipeline.py \
        --input data/sensors.parquet --output out/df --runs 3 --explain
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.functions import broadcast  # noqa: E402

from common.schema import (  # noqa: E402
    ANOMALY_SIGMA, QUALITY_THRESHOLD, REGION_DIM, SCHEMA, TOP_K, spark_session,
)


def _dim_dataframe(spark):
    rows = [(k, v["continent"], v["sla_ms"], v["tier"]) for k, v in REGION_DIM.items()]
    return spark.createDataFrame(rows, ["region", "continent", "sla_ms", "tier"])


def run(spark, input_path: str, output_path: str | None, explain: bool = False):
    if input_path.endswith(".parquet") or Path(input_path).is_dir():
        df = spark.read.parquet(input_path)
    else:
        df = spark.read.schema(SCHEMA).option("header", "true").csv(input_path)

    agg = (
        df.filter(F.col("quality") >= QUALITY_THRESHOLD)
          .groupBy("region", "sensor_id")
          .agg(
              F.count("*").alias("readings"),
              F.avg("value_norm").alias("mean_norm"),
              # stddev_pop para que coincida exactamente con la varianza
              # poblacional que calcula el pipeline RDD (n, no n-1).
              F.stddev_pop("value_norm").alias("stddev_norm"),
              F.min("value_norm").alias("min_norm"),
              F.max("value_norm").alias("max_norm"),
              F.sum(F.when(F.abs(F.col("value_norm")) > ANOMALY_SIGMA, 1).otherwise(0))
               .alias("anomalies"),
          )
          .withColumn("anomaly_ratio", F.col("anomalies") / F.col("readings"))
    )

    enriched = agg.join(broadcast(_dim_dataframe(spark)), on="region", how="left")

    # cache: el DataFrame se consume tres veces (count, sum, top-k). Sin cache
    # Spark recalcularía el shuffle en cada acción y la comparación con RDD
    # (que también materializa una vez) dejaría de ser justa.
    enriched.cache()

    if explain:
        print("\n===== PLAN FÍSICO (DataFrame) =====")
        enriched.explain(mode="formatted")

    total_groups = enriched.count()
    total_anomalies = enriched.agg(F.sum("anomalies")).collect()[0][0] or 0

    top = (
        enriched.orderBy(F.col("anomaly_ratio").desc(), F.col("readings").desc())
                .limit(TOP_K)
                .collect()
    )

    if output_path:
        enriched.write.mode("overwrite").parquet(output_path)

    enriched.unpersist()
    return {
        "groups": total_groups,
        "anomalies": int(total_anomalies),
        "top": [r.asDict() for r in top],
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", default=None)
    ap.add_argument("--runs", type=int, default=1)
    ap.add_argument("--explain", action="store_true")
    ap.add_argument("--metrics", default=None)
    args = ap.parse_args()

    spark = spark_session("dataframe-pipeline")
    times, result = [], None
    for i in range(args.runs):
        t0 = time.perf_counter()
        result = run(spark, args.input,
                     args.output if i == args.runs - 1 else None,
                     explain=args.explain and i == 0)
        elapsed = time.perf_counter() - t0
        times.append(elapsed)
        print(f"[dataframe] ejecución {i + 1}/{args.runs}: {elapsed:.2f} s", flush=True)

    payload = {
        "pipeline": "dataframe",
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
