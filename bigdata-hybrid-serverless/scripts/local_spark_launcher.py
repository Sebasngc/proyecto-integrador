"""
local_spark_launcher.py — Emula la API de EMR Serverless para la demo local.

Expone el MISMO contrato que la Lambda `spark_launcher` (/submit y /status), de
forma que el orquestador Akka no distingue entre local y nube: sólo cambia la
variable SPARK_LAUNCHER_ENDPOINT. Esto es lo que permite grabar el vídeo
demostrativo del flujo completo sin desplegar en AWS.

Ejecutar:
    python scripts/local_spark_launcher.py --port 8082 --data-dir ./data
"""

from __future__ import annotations

import argparse
import json
import logging
import subprocess
import threading
import uuid
from pathlib import Path

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("local-launcher")

ROOT = Path(__file__).resolve().parents[1]
PIPELINES = {
    "rdd": ROOT / "02-spark-jobs" / "pipelines" / "rdd_pipeline.py",
    "dataframe": ROOT / "02-spark-jobs" / "pipelines" / "dataframe_pipeline.py",
}

app = FastAPI(title="Local Spark Launcher", version="1.0.0")

# run_id -> estado. En memoria a propósito: es un doble de pruebas, no un
# servicio de producción. El equivalente real es la tabla de EMR.
RUNS: dict[str, dict] = {}
LOCK = threading.Lock()


class SubmitRequest(BaseModel):
    job_id: str
    pipeline: str = "dataframe"
    input_uri: str
    output_uri: str | None = None
    runs: int = 3


class StatusRequest(BaseModel):
    run_id: str


def _run_pipeline(run_id: str, req: SubmitRequest, metrics_path: Path) -> None:
    """Ejecuta spark-submit en un hilo aparte para no bloquear la respuesta HTTP."""
    script = PIPELINES[req.pipeline]
    cmd = [
        "spark-submit", "--master", "local[*]",
        "--driver-memory", "4g",
        "--conf", "spark.sql.adaptive.enabled=false",
        str(script),
        "--input", req.input_uri,
        "--runs", str(req.runs),
        "--metrics", str(metrics_path),
    ]
    if req.output_uri:
        cmd += ["--output", req.output_uri]

    log.info("run %s: %s", run_id, " ".join(cmd))
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)
        if proc.returncode != 0:
            tail = proc.stderr.strip().splitlines()[-15:]
            with LOCK:
                RUNS[run_id].update(state="FAILED", error="\n".join(tail))
            log.error("run %s falló (rc=%s)", run_id, proc.returncode)
            return

        metrics = json.loads(metrics_path.read_text()) if metrics_path.exists() else {}
        with LOCK:
            RUNS[run_id].update(
                state="SUCCESS",
                groups=metrics.get("groups", 0),
                anomalies=metrics.get("anomalies", 0),
                median_s=metrics.get("median_s", 0.0),
                times_s=metrics.get("times_s", []),
            )
        log.info("run %s completado en %.1fs (mediana)", run_id, metrics.get("median_s", 0.0))

    except subprocess.TimeoutExpired:
        with LOCK:
            RUNS[run_id].update(state="FAILED", error="timeout tras 3600 s")
    except Exception as exc:  # noqa: BLE001
        with LOCK:
            RUNS[run_id].update(state="FAILED", error=str(exc))
        log.exception("run %s reventó", run_id)


@app.post("/submit")
def submit(req: SubmitRequest) -> dict:
    if req.pipeline not in PIPELINES:
        raise HTTPException(400, f"pipeline desconocido: {req.pipeline}")

    run_id = f"local-{uuid.uuid4().hex[:12]}"
    metrics_dir = ROOT / "results" / "runs"
    metrics_dir.mkdir(parents=True, exist_ok=True)
    metrics_path = metrics_dir / f"{run_id}.json"

    with LOCK:
        RUNS[run_id] = {
            "run_id": run_id, "job_id": req.job_id, "pipeline": req.pipeline,
            "state": "RUNNING", "output_uri": req.output_uri,
        }

    threading.Thread(target=_run_pipeline, args=(run_id, req, metrics_path), daemon=True).start()
    return {"run_id": run_id, "job_id": req.job_id, "pipeline": req.pipeline, "state": "SUBMITTED"}


@app.post("/status")
def status(req: StatusRequest) -> dict:
    with LOCK:
        run = RUNS.get(req.run_id)
    if run is None:
        raise HTTPException(404, f"run desconocido: {req.run_id}")
    return run


@app.get("/health")
def health() -> dict:
    with LOCK:
        active = sum(1 for r in RUNS.values() if r["state"] == "RUNNING")
    return {"status": "ok", "runs": len(RUNS), "active": active}


if __name__ == "__main__":
    import uvicorn

    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8082)
    ap.add_argument("--host", default="0.0.0.0")
    args = ap.parse_args()
    uvicorn.run(app, host=args.host, port=args.port)
