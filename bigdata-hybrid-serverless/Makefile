# ============================================================
#  Proyecto integrador: GPU + Spark + actores + serverless
#  `make help` para ver los objetivos disponibles.
# ============================================================
.DEFAULT_GOAL := help
ROWS  ?= 5000000
DATA  ?= data/sensors.parquet
PY    ?= python3

help:  ## muestra esta ayuda
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	 awk 'BEGIN {FS=":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

setup:  ## instala dependencias de Python
	$(PY) -m pip install -r requirements.txt

gpu-build:  ## compila la librería CUDA/OpenMP
	$(MAKE) -C 01-gpu-preprocessing all

gpu-bench: gpu-build  ## benchmark GPU vs CPU -> results/gpu_bench.csv
	$(MAKE) -C 01-gpu-preprocessing bench

data:  ## genera el dataset sintético
	$(PY) scripts/generate_dataset.py --rows $(ROWS) --out $(DATA)
	$(PY) scripts/generate_dataset.py --rows $(ROWS) --out data/values.npy --format npy

spark-bench: ## benchmark RDD vs DataFrame -> results/spark_bench.json
	cd 02-spark-jobs && $(PY) bench/benchmark_runner.py \
	  --input ../$(DATA) --runs 5 --warmup 1 --out ../results/spark_bench.json

explain:  ## imprime el plan físico de Catalyst (para el informe)
	cd 02-spark-jobs && spark-submit --master 'local[*]' \
	  pipelines/dataframe_pipeline.py --input ../$(DATA) --runs 1 --explain

orchestrator:  ## arranca el sistema de actores
	cd 03-akka-orchestrator && sbt run

up:  ## levanta el entorno local completo (docker compose)
	docker compose up -d --build

up-gpu:  ## idem, con el servicio CUDA real
	docker compose --profile gpu up -d --build

down:  ## para el entorno local
	docker compose down -v

demo:  ## ejecuta el flujo extremo a extremo
	bash scripts/run_demo.sh

report:  ## regenera las tablas y gráficas del informe
	$(PY) scripts/build_report.py

deploy-aws:  ## despliega la pila serverless en AWS
	cd 04-serverless/aws && sam build && sam deploy --guided

clean:  ## limpia artefactos
	$(MAKE) -C 01-gpu-preprocessing clean
	rm -rf data/*.parquet data/*.npy results/runs out/ .pytest_cache

.PHONY: help setup gpu-build gpu-bench data spark-bench explain orchestrator up up-gpu down demo report deploy-aws clean
