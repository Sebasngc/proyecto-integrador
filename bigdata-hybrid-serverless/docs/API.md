# API HTTP del orquestador

Base: `http://localhost:8081` (local) · `https://<host>/` (desplegado)

## POST /api/v1/jobs

Lanza un job. Devuelve inmediatamente; el trabajo continúa en segundo plano.

```json
{
  "datasetUri": "s3://bucket/sensors.parquet",
  "mode": "zscore",
  "pipeline": "both",
  "outputUri": "s3://bucket/salida/",
  "callbackUrl": "https://mi-servicio/webhook",
  "forceCpu": false,
  "column": "value"
}
```

| Campo | Tipo | Obligatorio | Valores |
|---|---|:---:|---|
| `datasetUri` | string | ✓ | esquemas `s3://`, `s3a://`, `gs://`, `hdfs://`, `file://`, `abfss://` |
| `mode` | string | | `minmax` \| `zscore` (def.) \| `robust` |
| `pipeline` | string | | `rdd` \| `dataframe` \| `both` (def.) |
| `outputUri` | string | | Si se omite, se deriva de `datasetUri` |
| `callbackUrl` | string | | Webhook con el resultado; alternativa al sondeo |
| `forceCpu` | bool | | Fuerza el camino OpenMP (útil para comparar) |
| `column` | string | | Columna numérica a normalizar (def. `value`) |

**Respuestas:** `202` job aceptado (+ cabecera `Location`) · `400` cuerpo inválido ·
`409` job ya existente · `503` registro no disponible.

## GET /api/v1/jobs/{jobId}

Estado actual. `stage` ∈ `received`, `validating`, `gpu`, `spark`, `analyzing`,
`responding`, `completed`, `failed`. `attempts` lleva el contador de reintentos por etapa.

## GET /api/v1/jobs/{jobId}/result

- `200` — terminado; incluye `gpu`, `spark[]` y `analysis`
- `409` + `Retry-After: 5` — todavía en curso
- `422` — el job falló; el detalle está en `error`
- `404` — jobId desconocido

```json
{
  "jobId": "3f2a...",
  "stage": "completed",
  "gpu": {"backend": "cuda", "device": "NVIDIA L4 (sm_89, 58 SM, 22.3 GB)",
          "elements": 20000000, "kernelMs": 4.1, "wallMs": 118.7},
  "spark": [
    {"pipeline": "rdd", "groups": 40000, "anomalies": 1600, "medianSeconds": 42.3},
    {"pipeline": "dataframe", "groups": 40000, "anomalies": 1600, "medianSeconds": 14.1}
  ],
  "analysis": {"sparkSpeedup": 3.0, "anomalyRate": 0.04, "gpuBackend": "cuda",
               "totalSeconds": 71.2, "verdict": "DataFrame es 3.00x más rápido que RDD",
               "warnings": []}
}
```

> Los valores del ejemplo son ilustrativos del **formato**, no mediciones reales.

## Otros

| Método | Ruta | Descripción |
|---|---|---|
| `GET` | `/api/v1/jobs` | Últimos 100 jobs |
| `DELETE` | `/api/v1/jobs/{id}` | Cancelación (`202`) |
| `GET` | `/health` | Sonda de vida |
| `GET` | `/ready` | Sonda de disponibilidad (comprueba el registro) |

## Servicio GPU (interno)

`POST /preprocess` — acepta `data` (array en línea, ≤ 10⁶ elementos), `data_b64`
(float32 crudo) o `input_uri`. `GET /health`, `GET /info` devuelven backend y dispositivo.

## Lanzador de Spark (interno)

`POST /submit` → `{run_id}` · `POST /status` → `{state, groups, anomalies, median_s}`.
Estados: `SUBMITTED`, `RUNNING`, `SUCCESS`, `FAILED`, `CANCELLED`.
