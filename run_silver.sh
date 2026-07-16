#!/usr/bin/env bash
#
# run_silver.sh — despliega y ejecuta la capa Silver de punta a punta.
#
# Hace: sube el script de Glue y las reglas de calidad a S3, dispara el job,
# y espera (polling) hasta que termine, reportando el resultado.
#
# A diferencia de Bronze, este job puede fallar A PROPOSITO: si un chequeo de
# calidad critico no pasa, Silver no se publica. Ese fallo es el sistema
# funcionando, no un bug — el detalle queda en silver.quality_checks.
#
# Uso:
#   ./run_silver.sh
#   REGION=us-west-2 ./run_silver.sh
#
set -euo pipefail

# ---------- configuracion ----------
REGION="${REGION:-us-east-1}"
INFRA_DIR="${INFRA_DIR:-infra}"
SCRIPT_LOCAL="${SCRIPT_LOCAL:-glue_jobs/silver_clean.py}"
CONFIG_LOCAL="${CONFIG_LOCAL:-config/quality_rules.yaml}"
POLL_SECONDS="${POLL_SECONDS:-15}"

export AWS_DEFAULT_REGION="${REGION}"

# ---------- helpers de log ----------
info() { printf '\033[0;34m[info]\033[0m  %s\n' "$*"; }
ok()   { printf '\033[0;32m[ ok ]\033[0m  %s\n' "$*"; }
warn() { printf '\033[0;33m[warn]\033[0m  %s\n' "$*"; }
err()  { printf '\033[0;31m[fail]\033[0m  %s\n' "$*" >&2; }

# ---------- pre-checks ----------
command -v aws >/dev/null       || { err "aws CLI no encontrado"; exit 1; }
command -v terraform >/dev/null || { err "terraform no encontrado"; exit 1; }
[ -d "${INFRA_DIR}" ]             || { err "no existe el directorio '${INFRA_DIR}'"; exit 1; }
[ -f "${SCRIPT_LOCAL}" ]          || { err "no existe el script '${SCRIPT_LOCAL}'"; exit 1; }
[ -f "${CONFIG_LOCAL}" ]          || { err "no existen las reglas '${CONFIG_LOCAL}'"; exit 1; }

# ---------- leer outputs de Terraform ----------
info "Leyendo outputs de Terraform en '${INFRA_DIR}'..."
BUCKET="$(terraform -chdir="${INFRA_DIR}" output -raw bucket)"
JOB="$(terraform -chdir="${INFRA_DIR}" output -raw glue_job_silver)"
[ -n "${BUCKET}" ] && [ -n "${JOB}" ] || { err "no pude leer bucket/glue_job_silver (¿hiciste terraform apply?)"; exit 1; }
ok "bucket=$BUCKET  job=$JOB  region=$REGION"

# ---------- publicar script y reglas ----------
info "Subiendo ${SCRIPT_LOCAL}..."
aws s3 cp "${SCRIPT_LOCAL}" "s3://$BUCKET/scripts/silver_clean.py" --only-show-errors
info "Subiendo ${CONFIG_LOCAL}..."
aws s3 cp "${CONFIG_LOCAL}" "s3://$BUCKET/config/quality_rules.yaml" --only-show-errors
ok "script y reglas publicados"

# ---------- disparar el job ----------
info "Disparando el job de Glue..."
RUN_ID="$(aws glue start-job-run --job-name "${JOB}" --query JobRunId --output text)"
ok "run iniciado: $RUN_ID"

# ---------- polling hasta que termine ----------
info "Esperando a que termine (poll cada ${POLL_SECONDS}s)..."
while true; do
  STATE="$(aws glue get-job-run --job-name "${JOB}" --run-id "${RUN_ID}" \
            --query 'JobRun.JobRunState' --output text)"
  case "${STATE}" in
    SUCCEEDED)
      SECS="$(aws glue get-job-run --job-name "${JOB}" --run-id "${RUN_ID}" \
               --query 'JobRun.ExecutionTime' --output text)"
      ok "job SUCCEEDED en ${SECS}s"
      break ;;
    FAILED|STOPPED|TIMEOUT|ERROR)
      MSG="$(aws glue get-job-run --job-name "${JOB}" --run-id "${RUN_ID}" \
              --query 'JobRun.ErrorMessage' --output text)"
      err "job ${STATE} - ${MSG}"
      # un fallo por chequeo critico es el sistema haciendo su trabajo:
      # se distingue del bug para no mandar a depurar en falso
      case "${MSG}" in
        *critico*|*critical*)
          warn "Parece un chequeo de calidad CRITICO, no un error del pipeline."
          warn "Silver no se publico a proposito. Revisa el detalle en Athena:"
          warn "  SELECT * FROM silver.quality_checks WHERE NOT passed ORDER BY checked_at DESC;"
          ;;
      esac
      exit 1 ;;
    *)
      printf '  ... %s\n' "${STATE}"
      sleep "${POLL_SECONDS}" ;;
  esac
done

ok "Silver listo. Concilia la cuarentena en Athena:"
cat <<'SQL'

  -- ¿que se rechazo y por que? (debe cuadrar con el manifiesto de generate.py)
  SELECT reject_reason, COUNT(*) AS n
  FROM silver.quarantine GROUP BY reject_reason ORDER BY n DESC;

  -- invariante: validas + cuarentena == bronze
  SELECT (SELECT COUNT(*) FROM silver.transactions) AS validas,
         (SELECT COUNT(*) FROM silver.quarantine)   AS cuarentena,
         (SELECT COUNT(*) FROM bronze.transactions) AS bronze;

  -- veredicto de los chequeos por lote
  SELECT check_name, severity, observed_value, passed
  FROM silver.quality_checks ORDER BY checked_at DESC, severity;

SQL