# =============================================================================
#  Imagen del lanzador de Spark para la demo local.
#
#  Dos restricciones que chocan entre sí:
#
#    · `pip install pyspark` es inviable: PyPI sólo publica el paquete como
#      código fuente, así que pip descarga 317 MB, compila una rueda de otros
#      317 MB y la instala — más de 1 GB simultáneo, suficiente para agotar el
#      disco virtual de Docker Desktop.
#
#    · La imagen oficial `apache/spark:3.5.1` ya trae Spark compilado, pero va
#      sobre Ubuntu 20.04 con Python 3.8, y tanto este servicio como los
#      pipelines usan sintaxis de Python 3.10+ (`str | None`, PEP 604).
#
#  Se resuelve con una copia multi-etapa: se toman Spark y la JVM ya compilados
#  de la imagen oficial y se injertan sobre una base con Python 3.11. Sin
#  compilar nada y sin descargar la distribución por segunda vez.
# =============================================================================
FROM apache/spark:3.5.1 AS spark

FROM python:3.11-slim

# La JVM de la imagen oficial es Temurin 11, exactamente la que Spark 3.5
# espera. Copiarla evita instalar un JRE por apt (~180 MB de descarga extra).
COPY --from=spark /opt/java/openjdk /opt/java/openjdk
COPY --from=spark /opt/spark        /opt/spark

# Versiones fijadas a propósito: PySpark 3.5 se rompe con numpy 2.x y pandas
# 3.x en las conversiones vía Arrow. Sin fijarlas, pip instala las últimas y
# los pipelines fallan más tarde con errores difíciles de interpretar.
RUN pip install --no-cache-dir \
        "numpy<2" \
        "pandas==2.2.2" \
        "pyarrow==15.0.2" \
        "fastapi>=0.115" \
        "uvicorn>=0.30" \
        "pydantic>=2.7"

# Comprobación en tiempo de build: más vale fallar aquí que con un
# ModuleNotFoundError dentro de spark-submit, donde el mensaje se pierde.
ENV JAVA_HOME=/opt/java/openjdk \
    SPARK_HOME=/opt/spark \
    PATH=/opt/java/openjdk/bin:/opt/spark/bin:/usr/local/bin:/usr/bin:/bin \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3 \
    SPARK_LOCAL_IP=127.0.0.1 \
    PYTHONUNBUFFERED=1 \
    HOME=/tmp \
    PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip:/app/02-spark-jobs

RUN java -version && python3 -c "import pyspark, pandas, numpy, fastapi; print('pyspark', pyspark.__version__)"

WORKDIR /app
EXPOSE 8082
CMD ["python3", "scripts/local_spark_launcher.py", "--port", "8082"]
