"""
service.py — Microservicio serverless de preprocesamiento en GPU.

Un mismo módulo sirve para los tres destinos de despliegue:

  * Google Cloud Run con GPU (NVIDIA L4) / Azure Container Apps serverless GPU:
    se ejecuta como app ASGI (`uvicorn service:app`). Escala a cero, se factura
    por segundo y SÍ expone GPU. Es el destino recomendado para el camino CUDA.

  * AWS Lambda (imagen de contenedor): `handler` es el punto de entrada. Ojo:
    Lambda NO ofrece GPU, así que ahí se ejecuta el camino OpenMP. Para CUDA en
    AWS el orquestador delega en AWS Batch (cola con instancias G5) — ver
    04-serverless/aws/. Esta limitación está documentada en el informe.

  * Local / docker-compose para la demo.

Contrato HTTP
-------------
POST /preprocess
  { "data": [1.0, 2.0, ...], "mode": "zscore" }                 # array en línea
  { "input_uri": "s3://bucket/raw.npy", "output_uri": "s3://bucket/norm.parquet",
    "mode": "zscore", "column": "value" }                        # por referencia
GET  /health   -> estado + backend disponible
GET  /info     -> versión de la librería, dispositivo, hilos
"""

from __future__ import annotations

import base64
import io
import json
import logging
import os
import time
import uuid
from typing import Any

import numpy as np

from gpu_normalize import Normalizer, MODES

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
log = logging.getLogger("gpu-service")

# Límite para respuestas en línea. Por encima, se obliga a usar S3/GCS:
# API Gateway corta a 10 MB de payload y serializar 10^8 floats a JSON cuesta
# más que el propio kernel.
MAX_INLINE_ELEMENTS = int(os.environ.get("MAX_INLINE_ELEMENTS", 1_000_000))


# --------------------------------------------------------------------------
# Núcleo compartido por todos los transportes
# --------------------------------------------------------------------------
def _load_array(payload: dict) -> tuple[np.ndarray, str | None]:
    """Devuelve (array, uri_origen). Acepta datos en línea, base64 u objeto remoto."""
    if "data" in payload:
        arr = np.asarray(payload["data"], dtype=np.float32)
        if arr.size > MAX_INLINE_ELEMENTS:
            raise ValueError(
                f"array en línea demasiado grande ({arr.size} elementos > "
                f"{MAX_INLINE_ELEMENTS}); use input_uri")
        return arr, None

    if "data_b64" in payload:  # float32 crudo en base64: 4 bytes/elem en vez de ~12
        raw = base64.b64decode(payload["data_b64"])
        return np.frombuffer(raw, dtype=np.float32).copy(), None

    if uri := payload.get("input_uri"):
        return _read_uri(uri, payload.get("column")), uri

    raise ValueError("payload sin 'data', 'data_b64' ni 'input_uri'")


def _read_uri(uri: str, column: str | None = None) -> np.ndarray:
    """Lee .npy / .csv / .parquet desde s3://, gs:// o ruta local."""
    if uri.startswith(("s3://", "gs://")):
        import fsspec  # se resuelve a s3fs o gcsfs según el esquema
        opener = fsspec.open(uri, "rb")
    else:
        opener = open(uri, "rb")  # noqa: SIM115 — se cierra en el with

    with opener as fh:
        buf = io.BytesIO(fh.read())

    if uri.endswith(".npy"):
        return np.load(buf).astype(np.float32, copy=False).ravel()
    if uri.endswith(".parquet"):
        import pyarrow.parquet as pq
        table = pq.read_table(buf)
        col = column or table.column_names[0]
        return table.column(col).to_numpy(zero_copy_only=False).astype(np.float32)
    # csv por defecto
    import pandas as pd
    df = pd.read_csv(buf)
    col = column or df.select_dtypes("number").columns[0]
    return df[col].to_numpy(dtype=np.float32)


def _write_uri(uri: str, arr: np.ndarray) -> None:
    buf = io.BytesIO()
    np.save(buf, arr, allow_pickle=False)
    buf.seek(0)
    if uri.startswith(("s3://", "gs://")):
        import fsspec
        with fsspec.open(uri, "wb") as fh:
            fh.write(buf.read())
    else:
        with open(uri, "wb") as fh:
            fh.write(buf.read())


def preprocess(payload: dict) -> dict:
    """Lógica de negocio pura: entra dict, sale dict. Testeable sin HTTP."""
    started = time.perf_counter()
    request_id = payload.get("request_id") or str(uuid.uuid4())
    mode = payload.get("mode", "zscore")
    if mode not in MODES:
        raise ValueError(f"modo inválido {mode!r}; use uno de {list(MODES)}")

    arr, source = _load_array(payload)
    norm = Normalizer.get()
    result = norm.normalize(
        arr,
        mode=mode,
        force_cpu=bool(payload.get("force_cpu", False)),
        streams=int(payload.get("streams", 4)),
    )

    response: dict[str, Any] = {
        "request_id": request_id,
        "mode": mode,
        "elements": int(arr.size),
        "backend": result.backend,
        "device": result.device,
        "stats": result.stats,
        "timing_ms": result.timing,
        "source_uri": source,
    }

    if out_uri := payload.get("output_uri"):
        _write_uri(out_uri, result.data)
        response["output_uri"] = out_uri
    elif arr.size <= MAX_INLINE_ELEMENTS:
        response["data"] = result.data.tolist()
    else:
        response["warning"] = "resultado omitido: array grande sin output_uri"

    response["wall_ms"] = round((time.perf_counter() - started) * 1000, 3)
    log.info("preprocess ok id=%s n=%s backend=%s wall=%.1fms",
             request_id, arr.size, result.backend, response["wall_ms"])
    return response


# --------------------------------------------------------------------------
# Transporte 1: AWS Lambda
# --------------------------------------------------------------------------
def handler(event, context=None):  # noqa: ANN001
    """Punto de entrada Lambda. Acepta invocación directa o vía API Gateway."""
    try:
        body = event.get("body", event) if isinstance(event, dict) else event
        if isinstance(body, str):
            body = json.loads(body)
        if event.get("isBase64Encoded"):
            body = json.loads(base64.b64decode(event["body"]))

        path = (event.get("rawPath") or event.get("path") or "/preprocess")
        if path.endswith("/health") or path.endswith("/info"):
            return _lambda_response(200, Normalizer.get().info())

        return _lambda_response(200, preprocess(body))
    except ValueError as exc:                      # error del cliente
        log.warning("bad request: %s", exc)
        return _lambda_response(400, {"error": "bad_request", "detail": str(exc)})
    except Exception as exc:                       # noqa: BLE001 — frontera del proceso
        log.exception("fallo no controlado")
        return _lambda_response(500, {"error": "internal_error", "detail": str(exc)})


def _lambda_response(status: int, body: dict) -> dict:
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body, ensure_ascii=False),
    }


# --------------------------------------------------------------------------
# Transporte 2: FastAPI (Cloud Run GPU / Container Apps / local)
# --------------------------------------------------------------------------
try:
    from fastapi import FastAPI, HTTPException
    from pydantic import BaseModel, Field

    class PreprocessRequest(BaseModel):
        data: list[float] | None = None
        data_b64: str | None = None
        input_uri: str | None = None
        output_uri: str | None = None
        column: str | None = None
        mode: str = Field(default="zscore", pattern="^(minmax|zscore|robust)$")
        force_cpu: bool = False
        streams: int = 4
        request_id: str | None = None

    app = FastAPI(title="GPU Preprocessing Service", version="1.0.0")

    @app.get("/health")
    def health() -> dict:
        return {"status": "ok", **Normalizer.get().info()}

    @app.get("/info")
    def info() -> dict:
        return Normalizer.get().info()

    @app.post("/preprocess")
    def do_preprocess(req: PreprocessRequest) -> dict:
        try:
            return preprocess(req.model_dump(exclude_none=True))
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:  # noqa: BLE001
            log.exception("fallo no controlado")
            raise HTTPException(status_code=500, detail=str(exc)) from exc

except ImportError:  # FastAPI no está instalado en la imagen mínima de Lambda
    app = None
    log.info("FastAPI no disponible: sólo modo Lambda handler")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
