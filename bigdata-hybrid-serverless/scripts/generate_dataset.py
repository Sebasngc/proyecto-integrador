"""
generate_dataset.py — Dataset sintético de telemetría para las pruebas.

Diseñado para que el benchmark sea representativo:
  * cardinalidad alta en la clave de agrupación (miles de sensores) -> el shuffle
    es real, no cabe en una sola partición;
  * sesgo (skew) controlado: unas pocas regiones concentran el grueso de las
    filas, como en producción, para que se note el desequilibrio entre tareas;
  * anomalías inyectadas con una tasa conocida, para poder validar que ambos
    pipelines cuentan lo mismo;
  * valores nulos y de baja calidad, que ejercitan el filtro.

Uso:
    python scripts/generate_dataset.py --rows 20000000 --out data/sensors.parquet
    python scripts/generate_dataset.py --rows 1000000 --out data/sensors.csv --format csv
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

import numpy as np

REGIONS = ["eu-west", "eu-north", "us-east", "us-west",
           "sa-east", "ap-south", "ap-north", "af-south"]
# Distribución sesgada: us-east y eu-west se llevan ~55% de las filas.
REGION_P = np.array([0.25, 0.08, 0.30, 0.12, 0.06, 0.10, 0.06, 0.03])


def build_chunk(n: int, n_sensors: int, rng: np.random.Generator,
                anomaly_rate: float, base_ts: int) -> dict:
    region_idx = rng.choice(len(REGIONS), size=n, p=REGION_P)
    sensor_num = rng.integers(0, n_sensors, size=n)

    value = rng.normal(100.0, 25.0, size=n)
    # inyectar anomalías: desplazamiento de 4-8 sigma
    n_anom = int(n * anomaly_rate)
    if n_anom:
        idx = rng.choice(n, size=n_anom, replace=False)
        value[idx] += rng.choice([-1, 1], size=n_anom) * rng.uniform(100, 200, size=n_anom)

    quality = np.clip(rng.normal(80, 18, size=n), 0, 100).astype(np.int32)

    return {
        "sensor_id": np.array([f"S{i:06d}" for i in sensor_num]),
        "region": np.array(REGIONS, dtype=object)[region_idx],
        "ts": base_ts + rng.integers(0, 86_400 * 30, size=n).astype(np.int64),
        "value": value,
        # value_norm simula la salida del kernel CUDA (z-score global aproximado)
        "value_norm": (value - 100.0) / 25.0,
        "quality": quality,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=5_000_000)
    ap.add_argument("--sensors", type=int, default=5_000)
    ap.add_argument("--anomaly-rate", type=float, default=0.004)
    ap.add_argument("--out", default="data/sensors.parquet")
    ap.add_argument("--format", choices=["parquet", "csv", "npy"], default="parquet")
    ap.add_argument("--chunk", type=int, default=2_000_000, help="filas por bloque en memoria")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(args.seed)
    base_ts = int(time.time()) - 86_400 * 30
    t0 = time.perf_counter()

    if args.format == "npy":
        # sólo la columna numérica: entrada directa del servicio GPU
        values = rng.normal(100.0, 25.0, size=args.rows).astype(np.float32)
        np.save(out, values)
        print(f"{args.rows:,} valores float32 -> {out} "
              f"({out.stat().st_size / 1e6:.1f} MB, {time.perf_counter() - t0:.1f}s)")
        return

    import pyarrow as pa
    import pyarrow.parquet as pq

    schema = pa.schema([
        ("sensor_id", pa.string()), ("region", pa.string()), ("ts", pa.int64()),
        ("value", pa.float64()), ("value_norm", pa.float64()), ("quality", pa.int32()),
    ])

    writer = None
    csv_handle = None
    try:
        if args.format == "parquet":
            writer = pq.ParquetWriter(out, schema, compression="snappy")
        else:
            csv_handle = out.open("w")
            csv_handle.write("sensor_id,region,ts,value,value_norm,quality\n")

        remaining = args.rows
        while remaining > 0:
            n = min(args.chunk, remaining)
            data = build_chunk(n, args.sensors, rng, args.anomaly_rate, base_ts)
            table = pa.Table.from_pydict(
                {k: pa.array(v) for k, v in data.items()}, schema=schema)
            if writer:
                writer.write_table(table)
            else:
                import pandas as pd
                pd.DataFrame(data).to_csv(csv_handle, header=False, index=False)
            remaining -= n
            print(f"  {args.rows - remaining:,}/{args.rows:,} filas", end="\r", flush=True)
    finally:
        if writer:
            writer.close()
        if csv_handle:
            csv_handle.close()

    size = sum(f.stat().st_size for f in ([out] if out.is_file() else out.rglob("*")))
    print(f"\n{args.rows:,} filas -> {out} ({size / 1e6:.1f} MB, "
          f"{time.perf_counter() - t0:.1f}s, {args.sensors:,} sensores, "
          f"tasa de anomalía {args.anomaly_rate:.2%})")


if __name__ == "__main__":
    main()
