#!/usr/bin/env bash
#
# run_gold.sh — empaqueta, despliega y ejecuta la capa Gold (features).
#
# La diferencia con run_bronze/run_silver: aqui hay un PASO DE BUILD. Las
# definiciones de features son un paquete Python que entrega ciencia de datos,
# y un job de Glue es un script suelto, no un proyecto. Para que el job pueda
# hacer `import features`, hay que comprimir el paquete y pasarlo con
# --extra-py-files. Ese empaquetado ES el handoff: el codigo de DS se despliega
# sin que nadie edite el motor.
#
# El zip lo construye este script y no Terraform a proposito: comprimir es un
# paso de build, no infraestructura. Ademas asi DS puede agregar archivos a
# features/ sin tocar el .tf.
#
# Uso:
#   ./run_gold.sh
#   AS_OF=2026-03-10 ./run_gold.sh     # materializa lo que se sabia ese dia
#
set -euo pipefail

# ---------- configuracion ----------
REGION="${REGION:-us-east-1}"
INFRA_DIR="${INFRA_DIR:-infra}"
SCRIPT_LOCAL="${SCRIPT_LOCAL:-glue_jobs/gold_features.py}"
FEATURES_DIR="${FEATURES_DIR:-features}"
BUILD_DIR="${BUILD_DIR:-.build}"
AS_OF="${AS_OF:-now}"
POLL_SECONDS="${POLL_SECONDS:-15}"

export AWS_DEFAULT_REGION="${REGION}"

info() { printf '\033[0;34m[info]\033[0m  %s\n' "$*"; }
ok()   { printf '\033[0;32m[ ok ]\033[0m  %s\n' "$*"; }
err()  { printf '\033[0;31m[fail]\033[0m  %s\n' "$*" >&2; }

# ---------- pre-checks ----------
command -v aws >/dev/null       || { err "aws CLI no encontrado"; exit 1; }
command -v terraform >/dev/null || { err "terraform no encontrado"; exit 1; }
command -v zip >/dev/null       || { err "zip no encontrado"; exit 1; }
[ -d "${INFRA_DIR}" ]           || { err "no existe '${INFRA_DIR}'"; exit 1; }
[ -f "${SCRIPT_LOCAL}" ]        || { err "no existe '${SCRIPT_LOCAL}'"; exit 1; }
[ -f "${FEATURES_DIR}/__init__.py" ] || { err "'${FEATURES_DIR}' no es un paquete Python"; exit 1; }

# ---------- outputs de Terraform ----------
info "Leyendo outputs de Terraform en '${INFRA_DIR}'..."
BUCKET="$(terraform -chdir="${INFRA_DIR}" output -raw bucket)"
JOB="$(terraform -chdir="${INFRA_DIR}" output -raw glue_job_gold)"
[ -n "${BUCKET}" ] && [ -n "${JOB}" ] || { err "no pude leer bucket/glue_job_gold (¿terraform apply?)"; exit 1; }
ok "bucket=${BUCKET}  job=${JOB}  region=${REGION}  as_of=${AS_OF}"

# ---------- build: empaquetar el codigo de DS ----------
# El zip debe contener la CARPETA features/, no sus archivos sueltos: Glue
# agrega el zip a sys.path, asi que `import features` solo resuelve si dentro
# hay features/__init__.py.
info "Empaquetando ${FEATURES_DIR}/ ..."
rm -rf "${BUILD_DIR}"
mkdir -p "${BUILD_DIR}"
zip -r -q "${BUILD_DIR}/features.zip" "${FEATURES_DIR}" \
    -x '*__pycache__*' -x '*.pyc'
ok "features.zip: $(unzip -l "${BUILD_DIR}/features.zip" | tail -1 | awk '{print $2}') archivos"

# ---------- publicar ----------
info "Subiendo script y paquete de features..."
aws s3 cp "${SCRIPT_LOCAL}" "s3://${BUCKET}/scripts/gold_features.py" --only-show-errors
aws s3 cp "${BUILD_DIR}/features.zip" "s3://${BUCKET}/scripts/features.zip" --only-show-errors
ok "publicados"

# ---------- ejecutar ----------
info "Disparando el job de Glue..."
RUN_ID="$(aws glue start-job-run --job-name "${JOB}" \
          --arguments "{\"--as_of\":\"${AS_OF}\"}" \
          --query JobRunId --output text)"
ok "run iniciado: ${RUN_ID}"

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
      exit 1 ;;
    *)
      printf '  ... %s\n' "${STATE}"
      sleep "${POLL_SECONDS}" ;;
  esac
done

ok "Gold listo. Inspecciona en Athena:"
cat <<'SQL'

  -- el catalogo de features: que hay, de quien, con que ventana
  SELECT feature_name, tier, owner, window_seconds, description
  FROM gold.feature_registry ORDER BY tier, feature_name;

  -- cuantas filas son entrenables (etiqueta ya conocida en el corte)
  SELECT is_trainable, COUNT(*) AS n, SUM(CAST(is_fraud AS INT)) AS fraude
  FROM gold.fraud_features GROUP BY is_trainable;

  -- la tabla que consume el modelo
  SELECT * FROM gold.fraud_features LIMIT 20;

SQL