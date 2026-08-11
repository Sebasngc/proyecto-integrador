"""
benchmark_runner.py — Compara RDD vs DataFrame de forma reproducible.

Protocolo de medición:
  * Una sesión Spark NUEVA por pipeline: si se comparten, el segundo se
    beneficia del pool de JVMs ya calientes y del caché del sistema de ficheros.
  * `--warmup` ejecuciones descartadas antes de medir (JIT de la JVM: el
    bytecode generado por Tungsten se compila a nativo tras unos miles de
    iteraciones; medir la primera vuelta mide al intérprete).
  * Se reporta la MEDIANA de N ejecuciones, no la media: los tiempos de Spark
    tienen cola derecha larga por pausas de GC y reintentos de tareas.
  * Se verifica que ambos pipelines produzcan los MISMOS resultados antes de
    comparar tiempos. Un speedup sobre resultados distintos no significa nada.

Uso:
    python bench/benchmark_runner.py --input data/sensors.parquet \
        --runs 5 --warmup 1 --out ../results/spark_bench.json
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.schema import spark_session  # noqa: E402
from pipelines import dataframe_pipeline, rdd_pipeline  # noqa: E402

TOLERANCE = 1e-6


def _time_pipeline(module, app_name: str, input_path: str, runs: int, warmup: int) -> dict:
    spark = spark_session(app_name)
    try:
        for _ in range(warmup):
            module.run(spark, input_path, None)
        times, result = [], None
        for i in range(runs):
            t0 = time.perf_counter()
            result = module.run(spark, input_path, None)
            dt = time.perf_counter() - t0
            times.append(dt)
            print(f"  [{app_name}] {i + 1}/{runs}: {dt:.2f} s", flush=True)
        return {
            "times_s": times,
            "median_s": statistics.median(times),
            "mean_s": statistics.fmean(times),
            "stdev_s": statistics.stdev(times) if len(times) > 1 else 0.0,
            "min_s": min(times),
            "max_s": max(times),
            "cv_pct": (statistics.stdev(times) / statistics.fmean(times) * 100)
                      if len(times) > 1 else 0.0,
            "result": result,
        }
    finally:
        spark.stop()


def _equivalent(a: dict, b: dict) -> tuple[bool, list[str]]:
    """Los dos pipelines deben coincidir en cardinalidad, anomalías y top-k."""
    problems = []
    if a["groups"] != b["groups"]:
        problems.append(f"nº de grupos distinto: rdd={a['groups']} df={b['groups']}")
    if a["anomalies"] != b["anomalies"]:
        problems.append(f"nº de anomalías distinto: rdd={a['anomalies']} df={b['anomalies']}")

    for i, (ra, rb) in enumerate(zip(a["top"][:5], b["top"][:5])):
        if ra["sensor_id"] != rb["sensor_id"]:
            problems.append(f"top[{i}] difiere: {ra['sensor_id']} vs {rb['sensor_id']}")
        elif abs(ra["anomaly_ratio"] - rb["anomaly_ratio"]) > TOLERANCE:
            problems.append(f"top[{i}] ratio difiere: "
                            f"{ra['anomaly_ratio']} vs {rb['anomaly_ratio']}")
    return (not problems), problems


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--runs", type=int, default=5)
    ap.add_argument("--warmup", type=int, default=1)
    ap.add_argument("--out", default="../results/spark_bench.json")
    ap.add_argument("--label", default="local", help="etiqueta del entorno (local, emr, k8s)")
    args = ap.parse_args()

    print("== Pipeline RDD ==")
    rdd = _time_pipeline(rdd_pipeline, "bench-rdd", args.input, args.runs, args.warmup)
    print("== Pipeline DataFrame ==")
    dfr = _time_pipeline(dataframe_pipeline, "bench-dataframe", args.input, args.runs, args.warmup)

    ok, problems = _equivalent(rdd["result"], dfr["result"])
    speedup = rdd["median_s"] / dfr["median_s"] if dfr["median_s"] else float("nan")
    # Reducción porcentual del tiempo: más intuitivo que el speedup para el informe
    reduction = (1 - dfr["median_s"] / rdd["median_s"]) * 100 if rdd["median_s"] else 0.0

    report = {
        "environment": args.label,
        "input": args.input,
        "runs": args.runs,
        "warmup": args.warmup,
        "equivalent_results": ok,
        "equivalence_issues": problems,
        "rdd": {k: v for k, v in rdd.items() if k != "result"},
        "dataframe": {k: v for k, v in dfr.items() if k != "result"},
        "speedup_df_over_rdd": speedup,
        "time_reduction_pct": reduction,
        "groups": rdd["result"]["groups"],
        "anomalies": rdd["result"]["anomalies"],
    }

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2, default=str))

    print("\n" + "=" * 62)
    print(f"  RDD        mediana {rdd['median_s']:7.2f} s  (CV {rdd['cv_pct']:.1f}%)")
    print(f"  DataFrame  mediana {dfr['median_s']:7.2f} s  (CV {dfr['cv_pct']:.1f}%)")
    print(f"  Speedup DataFrame/RDD : {speedup:.2f}x  ({reduction:.1f}% menos tiempo)")
    print(f"  Resultados equivalentes: {'sí' if ok else 'NO -> ' + '; '.join(problems)}")
    print(f"  Informe: {out}")
    print("=" * 62)

    if not ok:
        sys.exit(2)  # falla el build si los pipelines divergen


if __name__ == "__main__":
    main()
