# Aplicación híbrida de procesamiento Big-Data en entorno serverless

Proyecto integrador que combina **GPU (CUDA + OpenMP)**, **Apache Spark (RDD vs DataFrame)**,
**modelo de actores (Akka)** y **funciones serverless** en un único flujo extremo a extremo.


## 1. Estructura del repositorio

| Ruta | Contenido |
|---|---|
| `01-gpu-preprocessing/` | Kernel CUDA, versión CPU con OpenMP, wrapper Python, servicio HTTP y Dockerfile multi-etapa |
| `02-spark-jobs/` | Pipelines RDD y DataFrame con semántica idéntica + arnés de benchmark |
| `03-akka-orchestrator/` | Sistema de actores (Scala/Akka Typed), API HTTP, persistencia y tests |
| `04-serverless/` | Plantilla AWS SAM, Cloud Run con GPU (GCP), Container Apps (Azure) |
| `scripts/` | Generación de datos, lanzador Spark local, demo y generación de informes |
| `informe/` | Informe técnico y guion del vídeo |
| `results/` | Salida de los benchmarks (CSV, JSON, gráficas) |

---

## 2. Requisitos

**Mínimos (todo funciona sin GPU: se usa el camino OpenMP)**

- Python 3.10+, Java 17+, Docker + Docker Compose
- `curl` y `jq` para el script de demostración

**Para el camino CUDA**

- GPU NVIDIA con capacidad de cómputo ≥ 7.0, driver ≥ 535, CUDA Toolkit 12.x
- `nvidia-container-toolkit` si se usa Docker

**Para desarrollar el orquestador**

- `sbt` 1.10+ y JDK 17/21

> **Versiones**: las de `build.sbt`, `requirements.txt` y los Dockerfile están fijadas
> a las que se usaron durante el desarrollo. Antes de reproducir el proyecto conviene
> comprobar que siguen disponibles, sobre todo `emr-7.x` y las imágenes base de CUDA.

---

## 3. Puesta en marcha rápida (5 minutos, sin GPU)

```bash
git clone <url-del-repo> && cd bigdata-hybrid-serverless
make setup                                  # dependencias de Python
make data ROWS=2000000                      # dataset sintético (~50 MB)
make up                                     # levanta MinIO, GPU-service, Spark, orquestador
make demo                                   # flujo completo con salida comentada
```

Al terminar:

```bash
curl -s localhost:8081/api/v1/jobs | jq '.[0].analysis'
make down
```

Con GPU real: `make up-gpu` en lugar de `make up`.

---

## 4. Ejecución módulo a módulo

### 4.1 Preprocesamiento GPU

```bash
make gpu-build                              # compila libnormalize.so (usa nvcc si existe)
make gpu-bench                              # -> results/gpu_bench.csv

# prueba directa del servicio
cd 01-gpu-preprocessing/python && python service.py &
curl -s -X POST localhost:8080/preprocess \
  -H 'Content-Type: application/json' \
  -d '{"data":[10,20,30,40,50],"mode":"zscore"}' | jq
```

`make gpu-build` detecta si hay `nvcc`. Si no lo hay compila sólo el camino OpenMP con
stubs de la API CUDA, de modo que **el mismo código Python funciona en ambos casos** y
`gpu_available()` devuelve `false`.

Modos disponibles: `minmax` → [0,1], `zscore` → media 0 / σ 1, `robust` → centrado y escalado por el rango.

### 4.2 Spark: RDD vs DataFrame

```bash
make data ROWS=20000000                     # ~500 MB en Parquet
make spark-bench                            # -> results/spark_bench.json
make explain                                # plan físico de Catalyst (para el informe)
```

El arnés lanza **una sesión Spark nueva por pipeline**, descarta ejecuciones de
calentamiento, reporta la mediana y **verifica que ambos pipelines devuelven el mismo
resultado** antes de comparar tiempos (si divergen, sale con código 2).

### 4.3 Orquestador de actores

```bash
cd 03-akka-orchestrator
sbt test                                    # tests del FSM, reintentos e idempotencia
sbt run                                     # API en http://localhost:8081
```

| Método | Ruta | Descripción |
|---|---|---|
| `POST` | `/api/v1/jobs` | Lanza un job → `202 Accepted` + `jobId` |
| `GET` | `/api/v1/jobs/{id}` | Estado y etapa actual |
| `GET` | `/api/v1/jobs/{id}/result` | Resultado (`409` + `Retry-After` si sigue en curso) |
| `GET` | `/api/v1/jobs` | Últimos jobs |
| `DELETE` | `/api/v1/jobs/{id}` | Cancelación |
| `GET` | `/health`, `/ready` | Sondas |

```bash
curl -X POST localhost:8081/api/v1/jobs -H 'Content-Type: application/json' -d '{
  "datasetUri": "s3://bigdata-data/sensors.parquet",
  "mode": "zscore",
  "pipeline": "both",
  "callbackUrl": "https://mi-endpoint/notificaciones"
}'
```

### 4.4 Despliegue serverless

```bash
# AWS (Lambda + EMR Serverless + DynamoDB + S3)
cd 04-serverless/aws && sam build && sam deploy --guided

# GCP: el único camino "serverless + CUDA" en un solo salto
gcloud run services replace 04-serverless/gcp/cloudrun-gpu.yaml --region us-central1

# Azure Container Apps con GPU serverless
az deployment group create -g <rg> --template-file 04-serverless/azure/containerapp.bicep
```

---

## 5. Una advertencia importante sobre "GPU serverless"

**AWS Lambda no ofrece GPU.** Es la restricción que más condiciona esta arquitectura y
conviene decirlo claro en lugar de disimularla. Las tres salidas implementadas:

| Opción | GPU | Escala a cero | Cuándo usarla |
|---|:---:|:---:|---|
| AWS Lambda (imagen de contenedor) | ✗ | ✓ | Arrays pequeños; camino OpenMP. Arranque ~1 s |
| **Google Cloud Run + NVIDIA L4** | ✓ | ✓ | **Opción recomendada**: cumple el enunciado literalmente |
| Azure Container Apps (perfil GPU) | ✓ | ✓ | Equivalente a Cloud Run en el ecosistema Azure |
| AWS Batch sobre instancias G5 | ✓ | ✓ (desde 0 nodos) | Lotes grandes en AWS; arranque de 2-4 min |

El orquestador consulta `/info` del servicio y registra en el resultado qué backend se usó
(`gpu.backend` = `cuda` o `openmp`), de modo que **ninguna medición se atribuye por error a
la GPU**. El análisis lo señala como aviso explícito.

---

## 6. Reproducir el análisis de rendimiento

```bash
make gpu-bench        # GPU vs CPU, varios tamaños
make spark-bench      # RDD vs DataFrame
make report           # -> results/tablas.md + gráficas PNG
```

Las tablas de `results/tablas.md` se pegan directamente en las secciones 6 y 7 del informe.

---

## 7. Entregables

| Entregable | Ubicación |
|---|---|
| Código CUDA/OpenMP | `01-gpu-preprocessing/src/` |
| Scripts Spark | `02-spark-jobs/pipelines/` |
| Sistema de actores | `03-akka-orchestrator/src/main/scala/` |
| Funciones serverless | `01-gpu-preprocessing/python/service.py`, `04-serverless/aws/spark_launcher.py` |
| Informe técnico | `informe/INFORME.md` |
| Guion del vídeo | `informe/guion-video.md` |
| Instrucciones | este archivo |

---

## 8. Resolución de problemas

| Síntoma | Causa y solución |
|---|---|
| `No se encontró libnormalize.so` | Ejecute `make gpu-build`, o exporte `NORMALIZE_LIB=/ruta/libnormalize.so` |
| `gpu_available` devuelve `false` con GPU instalada | El contenedor no ve el dispositivo: instale `nvidia-container-toolkit` y use `make up-gpu` |
| `nvcc: unrecognized option '-arch=sm_89'` | CUDA Toolkit antiguo: use `make gpu-build ARCH=sm_75` |
| El pipeline RDD tarda muchísimo | Normal en PySpark: cada fila cruza la frontera JVM↔Python. Es justo lo que mide el experimento |
| `OutOfMemoryError` en Spark local | Baje `ROWS` o suba `--driver-memory` en `scripts/local_spark_launcher.py` |
| El job se queda en `spark` | Revise los logs del lanzador; con `maxPolls=180` el techo son 30 min |
| Puerto 8080/8081/8082 ocupado | `docker compose down -v` o cambie los puertos en `docker-compose.yml` |

---

## 9. Licencia y créditos

Código del proyecto bajo licencia MIT.

**Akka** se distribuye bajo Business Source License 1.1 desde la versión 2.7 (uso gratuito por
debajo de 25 M USD de facturación anual). Para un uso comercial sin restricciones, `build.sbt`
incluye el bloque equivalente con **Apache Pekko** (Apache 2.0); la migración consiste en
sustituir `akka.` por `org.apache.pekko.` en los imports.
