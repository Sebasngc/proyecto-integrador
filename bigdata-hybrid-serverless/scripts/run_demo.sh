#!/usr/bin/env bash
# =============================================================================
#  run_demo.sh — Demostración extremo a extremo. Es el guion literal del vídeo.
#
#    bash scripts/run_demo.sh              flujo completo
#    bash scripts/run_demo.sh --fallos     añade la demo de reintentos
#
#  Requisitos: los servicios levantados (`docker compose up -d`), curl y jq.
# =============================================================================
set -uo pipefail   # sin -e: preferimos diagnosticar el fallo a morir en seco

ORCHESTRATOR="${ORCHESTRATOR:-http://localhost:8081}"
GPU_SERVICE="${GPU_SERVICE:-http://localhost:8080}"
LAUNCHER="${LAUNCHER:-http://localhost:8082}"
DATASET="${DATASET:-file:///app/data/sensors.parquet}"
CON_FALLOS=0
[ "${1:-}" = "--fallos" ] && CON_FALLOS=1

bold() { printf "\n\033[1;36m== %s ==\033[0m\n" "$1"; }
ok()   { printf "\033[0;32m  ✓ %s\033[0m\n" "$1"; }
warn() { printf "\033[0;33m  ! %s\033[0m\n" "$1"; }
fail() { printf "\033[0;31m  ✗ %s\033[0m\n" "$1"; exit 1; }

esperar() {  # esperar <url> <nombre> <segundos>
  local url="$1" nombre="$2" limite="${3:-60}" i=0
  while [ "$i" -lt "$limite" ]; do
    curl -sf "$url" >/dev/null 2>&1 && { ok "$nombre"; return 0; }
    i=$((i + 1)); sleep 1
  done
  fail "$nombre no responde en $url tras ${limite}s"
}

# ---------------------------------------------------------------- paso 0 ----
bold "0. Comprobando los tres servicios"
esperar "$ORCHESTRATOR/health"  "Orquestador Akka   ($ORCHESTRATOR)"  30
esperar "$GPU_SERVICE/health"   "Servicio GPU       ($GPU_SERVICE)"   30
esperar "$LAUNCHER/health"      "Lanzador de Spark  ($LAUNCHER)"      90

BACKEND=$(curl -s "$GPU_SERVICE/info" | jq -r '.backend // "desconocido"')
if [ "$BACKEND" = "cuda" ]; then ok "backend CUDA detectado"
else warn "backend '$BACKEND': sin GPU, se usa el camino OpenMP (esperado en portátil)"; fi

# ---------------------------------------------------------------- paso 1 ----
bold "1. Normalización directa contra el servicio (array pequeño)"
curl -s -X POST "$GPU_SERVICE/preprocess" \
  -H 'Content-Type: application/json' \
  -d '{"data":[10,20,30,40,50,60,70,80,90,100],"mode":"zscore"}' \
  | jq '{backend, device, media: .stats.mean, desviacion: .stats.stddev,
         salida: (.data | map(. * 10000 | round / 10000)), wall_ms}'
ok "media 0 y desviación 1: la normalización es correcta"

# ---------------------------------------------------------------- paso 2 ----
bold "2. Lanzando el job completo por el orquestador de actores"
RESP=$(curl -s -X POST "$ORCHESTRATOR/api/v1/jobs" \
  -H 'Content-Type: application/json' \
  -d "{\"datasetUri\":\"$DATASET\",\"mode\":\"zscore\",\"pipeline\":\"both\"}")
echo "$RESP" | jq .

JOB_ID=$(echo "$RESP" | jq -r '.jobId // empty')
[ -z "$JOB_ID" ] && fail "el orquestador no devolvió jobId (respuesta: $RESP)"
ok "job aceptado: $JOB_ID"

# ---------------------------------------------------------------- paso 3 ----
bold "3. Avance por etapas: validación → GPU → Spark → análisis"
ETAPA_PREV=""
for _ in $(seq 1 240); do
  ESTADO=$(curl -s "$ORCHESTRATOR/api/v1/jobs/$JOB_ID")
  ETAPA=$(echo "$ESTADO" | jq -r '.stage // "?"')
  if [ "$ETAPA" != "$ETAPA_PREV" ]; then
    printf "  %s  →  %s\n" "$(date +%H:%M:%S)" "$ETAPA"
    ETAPA_PREV="$ETAPA"
  fi
  case "$ETAPA" in
    completed) ok "pipeline completado"; break ;;
    failed)    echo "$ESTADO" | jq '.error'; fail "el job falló" ;;
  esac
  sleep 2
done
[ "$ETAPA_PREV" = "completed" ] || fail "tiempo agotado en la etapa '$ETAPA_PREV'"

# ---------------------------------------------------------------- paso 4 ----
bold "4. Resultado: GPU vs CPU y RDD vs DataFrame"
curl -s "$ORCHESTRATOR/api/v1/jobs/$JOB_ID/result" | jq '{
  backend: .gpu.backend, dispositivo: .gpu.device, elementos: .gpu.elements,
  spark: [.spark[] | {pipeline, segundos: .medianSeconds, grupos: .groups}],
  speedup: .analysis.sparkSpeedup, veredicto: .analysis.verdict,
  total_s: .analysis.totalSeconds, avisos: .analysis.warnings }'

# ---------------------------------------------------------------- paso 5 ----
bold "5. Entrada inválida: se rechaza sin gastar GPU ni reintentos"
curl -s -X POST "$ORCHESTRATOR/api/v1/jobs" \
  -H 'Content-Type: application/json' \
  -d '{"datasetUri":"ftp://esquema-no-soportado/x","mode":"zscore"}' | jq .
ok "rechazado en validación: los errores 4xx no se reintentan"

# ---------------------------------------------------------------- paso 6 ----
if [ "$CON_FALLOS" -eq 1 ]; then
  bold "6. Tolerancia a fallos: tirando el servicio GPU a mitad de job"
  R=$(curl -s -X POST "$ORCHESTRATOR/api/v1/jobs" -H 'Content-Type: application/json' \
      -d "{\"datasetUri\":\"$DATASET\",\"mode\":\"minmax\",\"pipeline\":\"dataframe\"}")
  J=$(echo "$R" | jq -r '.jobId // empty')
  ok "job de prueba: $J"
  docker compose stop gpu-preprocess >/dev/null 2>&1
  warn "servicio GPU detenido — observa los reintentos en los logs"
  sleep 12
  docker compose start gpu-preprocess >/dev/null 2>&1
  ok "servicio GPU restaurado; el job continúa donde estaba"
  for _ in $(seq 1 120); do
    E=$(curl -s "$ORCHESTRATOR/api/v1/jobs/$J" | jq -r '.stage // "?"')
    printf "\r  etapa: %-12s" "$E"
    [ "$E" = "completed" ] && { echo; ok "recuperado sin intervención manual"; break; }
    [ "$E" = "failed" ]    && { echo; warn "agotó los 3 intentos (también es un resultado válido)"; break; }
    sleep 2
  done
fi

bold "Demostración terminada"
