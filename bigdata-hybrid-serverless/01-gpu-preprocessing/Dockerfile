# =============================================================================
#  Imagen del servicio de preprocesamiento GPU.
#  Multi-stage: el toolchain de CUDA (≈3 GB) se queda en la etapa de build;
#  la imagen final sólo lleva el runtime (≈1.2 GB), lo que reduce
#  notablemente el arranque en frío al descargar capas.
#
#  Build GPU  : docker build -t gpu-preprocess:cuda .
#  Build CPU  : docker build -t gpu-preprocess:cpu --target cpu-runtime .
# =============================================================================

# ---------- etapa 1: compilar la librería nativa ----------
FROM nvidia/cuda:12.4.1-devel-ubuntu22.04 AS builder
ARG CUDA_ARCH=sm_89
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential libgomp1 && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY include/ include/
COPY src/ src/
COPY Makefile .
RUN make ARCH=${CUDA_ARCH} libnormalize.so && ls -lh libnormalize.so

# ---------- etapa 2: runtime con GPU (Cloud Run GPU / Container Apps / Batch) ----------
FROM nvidia/cuda:12.4.1-runtime-ubuntu22.04 AS gpu-runtime
RUN apt-get update && apt-get install -y --no-install-recommends \
        python3 python3-pip libgomp1 && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /build/libnormalize.so /opt/lib/libnormalize.so
COPY python/ /app/
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt
ENV NORMALIZE_LIB=/opt/lib/libnormalize.so \
    PYTHONUNBUFFERED=1 \
    OMP_NUM_THREADS=4 \
    PORT=8080
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=3s CMD python3 -c "import urllib.request;urllib.request.urlopen('http://localhost:8080/health')"
CMD ["python3", "-m", "uvicorn", "service:app", "--host", "0.0.0.0", "--port", "8080"]

# ---------- etapa 3: servicio HTTP normal sin GPU (demo local) ----------
#  IMPORTANTE: esta es la etapa que usa docker-compose.
#  La etapa `cpu-runtime` de abajo es la imagen de AWS Lambda, y el Runtime
#  Interface Emulator SOLO expone /2015-03-31/functions/function/invocations.
#  No sirve /health ni /preprocess, así que el orquestador recibía un 404.
#  Aquí se sirve el mismo `service.py` con uvicorn, con las rutas reales.
FROM python:3.12-slim AS cpu-service
RUN apt-get update && apt-get install -y --no-install-recommends \
        g++ libgomp1 && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY include/ /tmp/include/
COPY src/normalize_cpu.cpp src/cuda_stubs.cpp /tmp/src/
RUN mkdir -p /opt/lib && g++ -O3 -fopenmp -fPIC -std=c++17 -I/tmp/include --shared \
        /tmp/src/normalize_cpu.cpp /tmp/src/cuda_stubs.cpp \
        -o /opt/lib/libnormalize.so -lgomp -lm && rm -rf /tmp/src /tmp/include
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY python/ /app/
ENV NORMALIZE_LIB=/opt/lib/libnormalize.so PYTHONUNBUFFERED=1 OMP_NUM_THREADS=4
EXPOSE 8080
CMD ["python3", "-m", "uvicorn", "service:app", "--host", "0.0.0.0", "--port", "8080"]

# ---------- etapa 4: runtime CPU para AWS Lambda ----------
FROM public.ecr.aws/lambda/python:3.12 AS cpu-runtime
RUN dnf install -y gcc-c++ make libgomp && dnf clean all
COPY include/ /tmp/include/
COPY src/normalize_cpu.cpp src/cuda_stubs.cpp /tmp/src/
RUN g++ -O3 -fopenmp -fPIC -std=c++17 -I/tmp/include --shared \
        /tmp/src/normalize_cpu.cpp /tmp/src/cuda_stubs.cpp \
        -o /opt/lib/libnormalize.so -lgomp -lm && rm -rf /tmp/src /tmp/include
COPY python/ ${LAMBDA_TASK_ROOT}/
COPY requirements-lambda.txt .
RUN pip install --no-cache-dir -r requirements-lambda.txt -t ${LAMBDA_TASK_ROOT}
ENV NORMALIZE_LIB=/opt/lib/libnormalize.so OMP_NUM_THREADS=6
CMD ["service.handler"]
