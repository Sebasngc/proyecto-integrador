"""
build_report.py — Consolida las mediciones en tablas Markdown y gráficas.

Lee lo que hayan dejado los benchmarks:
    results/gpu_bench.csv      (make gpu-bench)
    results/spark_bench.json   (make spark-bench)
    results/e2e_*.json         (tiempos extremo a extremo de la demo)

y produce:
    results/tablas.md          -> pegar en el informe
    results/gpu_speedup.png
    results/spark_comparison.png

Si falta algún fichero, se avisa y se sigue con el resto: se puede regenerar la
parte del informe que corresponda sin tener que reejecutarlo todo.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path

RESULTS = Path(__file__).resolve().parents[1] / "results"


def _fmt(x: float, dec: int = 2) -> str:
    return f"{x:,.{dec}f}"


# --------------------------------------------------------------------- GPU ---
def gpu_table() -> tuple[str, dict]:
    path = RESULTS / "gpu_bench.csv"
    if not path.exists():
        return f"_(sin datos: ejecute `make gpu-bench` para generar {path.name})_\n", {}

    rows = list(csv.DictReader(path.open()))
    by_n: dict[int, dict[str, dict]] = {}
    for r in rows:
        n = int(r["n"])
        key = f"{r['impl']}_{r['threads']}"
        by_n.setdefault(n, {})[key] = r

    lines = [
        "| n (elementos) | CPU 1 hilo (ms) | CPU N hilos (ms) | GPU e2e (ms) | GPU kernel (ms) "
        "| Speedup GPU vs 1 hilo | Speedup GPU vs N hilos | % tiempo en PCIe |",
        "|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    series = {"n": [], "cpu1": [], "cpuN": [], "gpu": [], "kernel": []}

    for n in sorted(by_n):
        entries = by_n[n]
        cpu1 = next((v for k, v in entries.items() if k.startswith("cpu_omp_1")), None)
        cpun = next((v for k, v in entries.items()
                     if k.startswith("cpu_omp_") and not k.endswith("_1")), None)
        gpu = next((v for k, v in entries.items() if k.startswith("gpu_cuda")), None)
        if not cpu1:
            continue

        t1 = float(cpu1["ms_mediana"])
        tn = float(cpun["ms_mediana"]) if cpun else float("nan")
        tg = float(gpu["ms_mediana"]) if gpu else float("nan")
        tk = float(gpu["kernel_ms"]) if gpu else float("nan")
        pcie = ((float(gpu["h2d_ms"]) + float(gpu["d2h_ms"])) / tg * 100) if gpu and tg else float("nan")

        lines.append(
            f"| {n:,} | {_fmt(t1)} | {_fmt(tn)} | {_fmt(tg)} | {_fmt(tk)} "
            f"| {_fmt(t1 / tg) if tg == tg else '—'}x "
            f"| {_fmt(tn / tg) if tg == tg else '—'}x "
            f"| {_fmt(pcie, 1) if pcie == pcie else '—'}% |"
        )
        series["n"].append(n)
        series["cpu1"].append(t1)
        series["cpuN"].append(tn)
        series["gpu"].append(tg)
        series["kernel"].append(tk)

    return "\n".join(lines) + "\n", series


# ------------------------------------------------------------------- Spark ---
def spark_table() -> tuple[str, dict]:
    path = RESULTS / "spark_bench.json"
    if not path.exists():
        return f"_(sin datos: ejecute `make spark-bench` para generar {path.name})_\n", {}

    d = json.loads(path.read_text())
    rdd, df = d["rdd"], d["dataframe"]
    lines = [
        "| Métrica | RDD | DataFrame |",
        "|---|---:|---:|",
        f"| Mediana (s) | {_fmt(rdd['median_s'])} | {_fmt(df['median_s'])} |",
        f"| Media (s) | {_fmt(rdd['mean_s'])} | {_fmt(df['mean_s'])} |",
        f"| Desv. típica (s) | {_fmt(rdd['stdev_s'])} | {_fmt(df['stdev_s'])} |",
        f"| Mínimo (s) | {_fmt(rdd['min_s'])} | {_fmt(df['min_s'])} |",
        f"| Coef. de variación | {_fmt(rdd['cv_pct'], 1)}% | {_fmt(df['cv_pct'], 1)}% |",
        "",
        f"- **Speedup DataFrame/RDD: {_fmt(d['speedup_df_over_rdd'])}x** "
        f"({_fmt(d['time_reduction_pct'], 1)}% menos tiempo)",
        f"- Ejecuciones: {d['runs']} (+{d['warmup']} de calentamiento descartadas)",
        f"- Resultados equivalentes entre pipelines: "
        f"{'sí' if d['equivalent_results'] else 'NO — ' + '; '.join(d['equivalence_issues'])}",
        f"- Grupos agregados: {d.get('groups', 0):,} | anomalías: {d.get('anomalies', 0):,}",
    ]
    return "\n".join(lines) + "\n", d


# ---------------------------------------------------------------- gráficas ---
def charts(gpu: dict, spark: dict) -> list[str]:
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        return []

    made = []

    if gpu.get("n"):
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4.5))
        ax1.plot(gpu["n"], gpu["cpu1"], "o-", label="CPU 1 hilo")
        ax1.plot(gpu["n"], gpu["cpuN"], "s-", label="CPU N hilos (OpenMP)")
        ax1.plot(gpu["n"], gpu["gpu"], "^-", label="GPU extremo a extremo")
        ax1.plot(gpu["n"], gpu["kernel"], "v--", label="GPU sólo kernel")
        ax1.set(xscale="log", yscale="log", xlabel="elementos", ylabel="tiempo (ms)",
                title="Tiempo de normalización")
        ax1.grid(alpha=0.3, which="both"); ax1.legend()

        sp_e2e = [c / g if g else 0 for c, g in zip(gpu["cpu1"], gpu["gpu"])]
        sp_k = [c / k if k else 0 for c, k in zip(gpu["cpu1"], gpu["kernel"])]
        ax2.plot(gpu["n"], sp_e2e, "^-", label="speedup extremo a extremo")
        ax2.plot(gpu["n"], sp_k, "v--", label="speedup sólo kernel")
        ax2.axhline(1, color="gray", ls=":", label="paridad con CPU 1 hilo")
        ax2.set(xscale="log", xlabel="elementos", ylabel="speedup (x)",
                title="Speedup GPU frente a CPU secuencial")
        ax2.grid(alpha=0.3); ax2.legend()

        fig.tight_layout()
        out = RESULTS / "gpu_speedup.png"
        fig.savefig(out, dpi=150); plt.close(fig)
        made.append(str(out))

    if spark:
        fig, ax = plt.subplots(figsize=(7, 4.5))
        labels = ["RDD", "DataFrame"]
        medians = [spark["rdd"]["median_s"], spark["dataframe"]["median_s"]]
        errors = [spark["rdd"]["stdev_s"], spark["dataframe"]["stdev_s"]]
        bars = ax.bar(labels, medians, yerr=errors, capsize=6,
                      color=["#c44e52", "#4c72b0"], alpha=0.85)
        for b, m in zip(bars, medians):
            ax.text(b.get_x() + b.get_width() / 2, m, f"{m:.1f}s",
                    ha="center", va="bottom", fontweight="bold")
        ax.set(ylabel="tiempo mediano (s)",
               title=f"RDD vs DataFrame — speedup {spark['speedup_df_over_rdd']:.2f}x")
        ax.grid(axis="y", alpha=0.3)
        fig.tight_layout()
        out = RESULTS / "spark_comparison.png"
        fig.savefig(out, dpi=150); plt.close(fig)
        made.append(str(out))

    return made


def main() -> None:
    RESULTS.mkdir(exist_ok=True)
    gpu_md, gpu_data = gpu_table()
    spark_md, spark_data = spark_table()

    doc = "\n".join([
        "# Tablas de resultados (generado por scripts/build_report.py)",
        "",
        "## 1. GPU (CUDA) frente a CPU (OpenMP)", "", gpu_md, "",
        "## 2. Spark: RDD frente a DataFrame", "", spark_md, "",
    ])
    (RESULTS / "tablas.md").write_text(doc)
    print(f"escrito {RESULTS / 'tablas.md'}")
    for f in charts(gpu_data, spark_data):
        print(f"escrito {f}")


if __name__ == "__main__":
    main()
