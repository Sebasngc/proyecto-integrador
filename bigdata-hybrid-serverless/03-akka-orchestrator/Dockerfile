FROM eclipse-temurin:11-jdk AS builder

# Instalamos SBT 1.10.0 (coincide con el proyecto)
ENV SBT_VERSION=1.10.0
RUN apt-get update && apt-get install -y curl tar && \
    curl -L -o sbt-$SBT_VERSION.tgz https://github.com/sbt/sbt/releases/download/v$SBT_VERSION/sbt-$SBT_VERSION.tgz && \
    tar -xzf sbt-$SBT_VERSION.tgz -C /usr/local && \
    ln -s /usr/local/sbt/bin/sbt /usr/local/bin/sbt && \
    rm sbt-$SBT_VERSION.tgz

WORKDIR /app
COPY . .
RUN sbt compile stage

FROM eclipse-temurin:11-jre
WORKDIR /app
# Copiamos la aplicación empaquetada por sbt-native-packager
COPY --from=builder /app/target/universal/stage /app

EXPOSE 8081
CMD ["/app/bin/bigdata-orchestrator"]