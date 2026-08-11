"""
spark_launcher.py — Función serverless que lanza jobs de Spark.

Responde a dos rutas y nada más, siguiendo el principio de que una Lambda debe
terminar rápido y no esperar:

    POST /submit  {job_id, pipeline, input_uri, output_uri, runs}  -> {run_id}
    POST /status  {run_id}                                          -> {state, ...}

Por qué NO se espera dentro de la Lambda a que Spark termine:
  * el techo de ejecución de Lambda son 15 minutos y un job Spark puede durar
    más, así que sería una bomba de relojería;
  * se pagaría por milisegundos de espera ociosa, que es justo lo contrario de
    lo que hace atractivo el serverless;
  * el sondeo es responsabilidad del SparkJobActor, que ya lo hace sin bloquear
    hilos y con reintentos.
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

import boto3

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
log = logging.getLogger("spark-launcher")

emr = boto3.client("emr-serverless")
s3 = boto3.client("s3")

APPLICATION_ID = os.environ["EMR_APPLICATION_ID"]
EXECUTION_ROLE = os.environ["EMR_EXECUTION_ROLE"]
DATA_BUCKET = os.environ["DATA_BUCKET"]

PIPELINE_ENTRYPOINTS = {
    "rdd": f"s3://{DATA_BUCKET}/code/rdd_pipeline.py",
    "dataframe": f"s3://{DATA_BUCKET}/code/dataframe_pipeline.py",
}

# Configuración de Spark. Se fija explícitamente para que los dos pipelines
# corran en condiciones idénticas: cualquier diferencia de recursos entre ellos
# invalidaría la comparación de rendimiento.
SPARK_SUBMIT_PARAMS = " ".join([
    "--conf spark.executor.cores=4",
    "--conf spark.executor.memory=14g",
    "--conf spark.executor.instances=4",
    "--conf spark.driver.cores=2",
    "--conf spark.driver.memory=6g",
    "--conf spark.sql.adaptive.enabled=false",
    "--conf spark.dynamicAllocation.enabled=false",  # nº de executors fijo: medición estable
    f"--py-files s3://{DATA_BUCKET}/code/common.zip",
])


def handler(event, context=None):  # noqa: ANN001
    path = (event.get("rawPath") or event.get("path") or "").rstrip("/")
    body = event.get("body", event)
    if isinstance(body, str):
        body = json.loads(body)

    try:
        if path.endswith("/submit"):
            return _ok(submit(body))
        if path.endswith("/status"):
            return _ok(status(body))
        return _err(404, "ruta desconocida", path)
    except KeyError as exc:
        return _err(400, "falta un campo obligatorio", str(exc))
    except Exception as exc:  # noqa: BLE001
        log.exception("fallo en el lanzador")
        return _err(500, "error del lanzador", str(exc))


def submit(body: dict) -> dict:
    job_id = body["job_id"]
    pipeline = body.get("pipeline", "dataframe")
    if pipeline not in PIPELINE_ENTRYPOINTS:
        raise ValueError(f"pipeline desconocido: {pipeline}")

    input_uri = body["input_uri"]
    output_uri = body.get("output_uri") or f"s3://{DATA_BUCKET}/results/{job_id}/{pipeline}/"
    runs = int(body.get("runs", 3))
    metrics_uri = f"s3://{DATA_BUCKET}/metrics/{job_id}/{pipeline}.json"

    response = emr.start_job_run(
        applicationId=APPLICATION_ID,
        executionRoleArn=EXECUTION_ROLE,
        name=f"{job_id[:8]}-{pipeline}",
        jobDriver={
            "sparkSubmit": {
                "entryPoint": PIPELINE_ENTRYPOINTS[pipeline],
                "entryPointArguments": [
                    "--input", input_uri,
                    "--output", output_uri,
                    "--runs", str(runs),
                    "--metrics", metrics_uri,
                ],
                "sparkSubmitParameters": SPARK_SUBMIT_PARAMS,
            }
        },
        configurationOverrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {"logUri": f"s3://{DATA_BUCKET}/logs/{job_id}/"}
            }
        },
        # 40 min de techo: si un job se cuelga, EMR lo mata y deja de facturar
        executionTimeoutMinutes=40,
        tags={"job_id": job_id, "pipeline": pipeline},
    )

    run_id = response["jobRunId"]
    log.info("job %s: lanzado %s como run %s", job_id, pipeline, run_id)
    return {
        "run_id": run_id,
        "job_id": job_id,
        "pipeline": pipeline,
        "state": "SUBMITTED",
        "output_uri": output_uri,
        "metrics_uri": metrics_uri,
    }


# EMR usa muchos estados; el orquestador sólo entiende tres familias.
_TERMINAL_OK = {"SUCCESS"}
_TERMINAL_KO = {"FAILED", "CANCELLED", "CANCELLING"}


def status(body: dict) -> dict:
    run_id = body["run_id"]
    run = emr.get_job_run(applicationId=APPLICATION_ID, jobRunId=run_id)["jobRun"]
    state = run["state"]
    result: dict[str, Any] = {
        "run_id": run_id,
        "state": state,
        "state_details": run.get("stateDetails", ""),
        "billed_vcpu_hour": run.get("billedResourceUtilization", {}).get("vCPUHour"),
        "billed_memory_gb_hour": run.get("billedResourceUtilization", {}).get("memoryGBHour"),
    }

    if state in _TERMINAL_KO:
        result["error"] = run.get("stateDetails", "el job terminó sin éxito")
        return result

    if state in _TERMINAL_OK:
        tags = run.get("tags", {})
        metrics_key = f"metrics/{tags.get('job_id', '')}/{tags.get('pipeline', '')}.json"
        result.update(_read_metrics(metrics_key))

    return result


def _read_metrics(key: str) -> dict:
    """El pipeline deja sus tiempos en S3; aquí sólo se recogen.

    Si el fichero no existe (job que terminó bien pero no escribió métricas), se
    devuelve un dict vacío en lugar de fallar: perder las métricas no debe
    invalidar un job que sí produjo resultados.
    """
    try:
        obj = s3.get_object(Bucket=DATA_BUCKET, Key=key)
        data = json.loads(obj["Body"].read())
        return {
            "groups": data.get("groups", 0),
            "anomalies": data.get("anomalies", 0),
            "median_s": data.get("median_s", 0.0),
            "times_s": data.get("times_s", []),
            "output_uri": data.get("output", ""),
        }
    except s3.exceptions.NoSuchKey:
        log.warning("métricas no encontradas en %s", key)
        return {}
    except Exception as exc:  # noqa: BLE001
        log.warning("no se pudieron leer las métricas de %s: %s", key, exc)
        return {}


def _ok(payload: dict) -> dict:
    return {"statusCode": 200, "headers": {"Content-Type": "application/json"},
            "body": json.dumps(payload, default=str)}


def _err(code: int, error: str, detail: str = "") -> dict:
    return {"statusCode": code, "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": error, "detail": detail})}
