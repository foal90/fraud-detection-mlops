#!/usr/bin/env bash
#
# run_bronze.sh — despliega y ejecuta la capa Bronze de punta a punta.
#
# Hace: sube el script de Glue a S3, sincroniza el raw, dispara el job,
# y espera (polling) hasta que termine, reportando el resultado.
#
# Uso:
#   ./run_bronze.sh                 # usa los outputs de Terraform en ./infra
#   REGION=us-west-2 ./run_bronze.sh
#
set -euo pipefail   # -e: corta si algo falla | -u: error si usas var no definida | -o pipefail: falla en pipes

# ---------- configuracion ----------
REGION="${REGION:-us-east-1}"
INFRA_DIR="${INFRA_DIR:-infra}"
RAW_DIR="${RAW_DIR:-output/raw}"
SCRIPT_LOCAL="${SCRIPT_LOCAL:-glue_jobs/bronze_ingest.py}"
POLL_SECONDS="${POLL_SECONDS:-15}"

export AWS_DEFAULT_REGION="$REGION"

# ---------- helpers de log ----------
info()  { printf '\033[0;34m[info]\033[0m  %s\n' "$*"; }
ok()    { printf '\033[0;32m[ ok ]\033[0m  %s\n' "$*"; }
err()   { printf '\033[0;31m[fail]\033[0m  %s\n' "$*" >&2; }

# ---------- pre-checks ----------
command -v aws >/dev/null       || { err "aws CLI no encontrado"; exit 1; }
command -v terraform >/dev/null || { err "terraform no encontrado"; exit 1; }
[ -d "$INFRA_DIR" ]             || { err "no existe el directorio '$INFRA_DIR'"; exit 1; }
[ -f "$SCRIPT_LOCAL" ]          || { err "no existe el script '$SCRIPT_LOCAL'"; exit 1; }
[ -d "$RAW_DIR" ]               || { err "no existe el raw '$RAW_DIR' (¿corriste generate.py?)"; exit 1; }

# ---------- leer outputs de Terraform ----------
info "Leyendo outputs de Terraform en '$INFRA_DIR'…"
BUCKET="$(terraform -chdir="$INFRA_DIR" output -raw bucket)"
JOB="$(terraform -chdir="$INFRA_DIR" output -raw glue_job)"
[ -n "$BUCKET" ] && [ -n "$JOB" ] || { err "no pude leer bucket/glue_job (¿hiciste terraform apply?)"; exit 1; }
ok "bucket=$BUCKET  job=$JOB  region=$REGION"

# ---------- subir script de Glue ----------
info "Subiendo $SCRIPT_LOCAL a S3…"
aws s3 cp "$SCRIPT_LOCAL" "s3://$BUCKET/scripts/bronze_ingest.py" --only-show-errors
ok "script actualizado en s3://$BUCKET/scripts/"

# ---------- sincronizar raw ----------
info "Sincronizando raw a S3…"
aws s3 sync "$RAW_DIR" "s3://$BUCKET/raw" --only-show-errors
ok "raw sincronizado"

# ---------- disparar el job ----------
info "Disparando el job de Glue…"
RUN_ID="$(aws glue start-job-run --job-name "$JOB" --query JobRunId --output text)"
ok "run iniciado: $RUN_ID"

# ---------- polling hasta que termine ----------
info "Esperando a que termine (poll cada ${POLL_SECONDS}s)…"
while true; do
  STATE="$(aws glue get-job-run --job-name "$JOB" --run-id "$RUN_ID" \
            --query 'JobRun.JobRunState' --output text)"
  case "$STATE" in
    SUCCEEDED)
      SECS="$(aws glue get-job-run --job-name "$JOB" --run-id "$RUN_ID" \
               --query 'JobRun.ExecutionTime' --output text)"
      ok "job SUCCEEDED en ${SECS}s"
      break ;;
    FAILED|STOPPED|TIMEOUT|ERROR)
      MSG="$(aws glue get-job-run --job-name "$JOB" --run-id "$RUN_ID" \
              --query 'JobRun.ErrorMessage' --output text)"
      err "job $STATE — $MSG"
      exit 1 ;;
    *)
      printf '  … %s\n' "$STATE"
      sleep "$POLL_SECONDS" ;;
  esac
done

ok "Bronze listo. Valida en Athena la base 'bronze'."