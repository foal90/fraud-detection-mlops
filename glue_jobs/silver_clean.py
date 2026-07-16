"""
silver_clean.py — AWS Glue 5.0 (Spark 3.5 + Iceberg) — capa Silver.

Lee las tablas Bronze y produce dato CONFIABLE. Filosofia Silver: lo que
sobrevive esta garantizado; lo que no, queda auditable.

Las reglas NO viven aqui: se leen de un YAML en S3 (config/quality_rules.yaml).
Este job es la MECANICA; el YAML es la POLITICA. Cambiar un umbral no requiere
tocar este archivo ni redesplegar.

Tablas producidas (Iceberg, namespace = --database):
  transactions    filas validas, deduplicadas, con etiqueta de fraude unida
  quarantine      filas rechazadas, cada una con su reject_reason
  data_profile    perfilado por columna, con historial por corrida
  quality_checks  veredicto de cada chequeo por lote, con historial

Invariante que debe cumplirse siempre:
  count(transactions) + count(quarantine) == count(bronze.transactions)

Orden de evaluacion (importa):
  1. validaciones por fila   -> gana la PRIMERA regla que falla
  2. integridad referencial  -> solo sobre filas que aun no fueron rechazadas
  3. deduplicacion           -> solo sobre las que sobrevivieron 1 y 2

Job args:
  --config_path  s3://<bucket>/config/quality_rules.yaml
  --bronze_db    bronze
  --database     silver
Requiere: --datalake-formats iceberg  y  --additional-python-modules pyyaml

===============================================================================
LIMITACION CONOCIDA — este job es FULL REFRESH, no incremental
===============================================================================
Cada corrida lee Bronze completo y reescribe Silver completo. Es una decision
deliberada: a la escala de este proyecto (~140k filas / ~6 MB) el costo es de
segundos, y el codigo se lee sin la complejidad del control incremental.

A escala de TB por dia este diseño NO sobrevive. Donde revienta, en orden:

  1. spark.table(bronze) sin filtro relee toda la historia cada corrida,
     aunque solo hayan llegado unos GB nuevos. Este es el techo real.
  2. createOrReplace() sobre Silver bota y reescribe todo el dataset.
  3. El cache() no cabe en memoria: se derrama a disco y hace thrashing.
  4. La ventana del dedup (partitionBy sobre la llave) baraja el dataset
     completo en cada corrida.

Como se resolveria (procesamiento incremental):

  - Leer solo lo nuevo con la incremental read de Iceberg: los snapshots
    posteriores al ultimo procesado. Aqui el time travel deja de ser
    curiosidad y se vuelve el mecanismo central.
  - Mantener una tabla watermark con "hasta que snapshot ya procese", para
    saber donde retomar y que la reejecucion siga siendo idempotente.
  - Publicar Silver con MERGE en vez de createOrReplace: solo se tocan las
    particiones afectadas.
  - Dedup en dos etapas: dentro del incremento (shuffle chico) + anti-join
    contra las llaves ya publicadas.
  - Perfilar y chequear sobre el incremento, no sobre la historia completa.

El efecto: el costo pasa de ser proporcional a la HISTORIA a ser proporcional
a lo NUEVO, que es la unica forma de que un pipeline diario sobreviva años.
===============================================================================
"""
import sys
from datetime import datetime, timezone

import boto3
import yaml
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

CATALOG = "glue_catalog"


# ==============================================================================
# Config
# ==============================================================================

def load_config(s3_path: str) -> dict:
    """Lee el YAML de reglas desde S3 en tiempo de ejecucion."""
    without_scheme = s3_path.replace("s3://", "", 1)
    bucket, key = without_scheme.split("/", 1)
    body = boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read()
    return yaml.safe_load(body)


# ==============================================================================
# 1-2. Validaciones por fila + integridad referencial
# ==============================================================================

def build_reject_reason(df: DataFrame, cfg: dict, dims: dict) -> DataFrame:
    """
    Etiqueta cada fila con el motivo de rechazo de la PRIMERA regla que falla,
    o NULL si pasa todo.

    Se construye como un coalesce sobre una lista de `when`: cada `when` devuelve
    su reject_reason si la regla falla, o NULL si pasa. coalesce toma el primero
    no-nulo, que es exactamente "gana la primera regla que falla". Es una sola
    expresion plana, evaluada en una pasada.
    """
    candidates = []

    # --- validaciones por fila (expresiones SQL declaradas en el YAML) ---
    for rule in cfg.get("row_validations", []):
        expr = F.expr(rule["expr"])
        # una regla que evalua a NULL (p.ej. amount >= 0 con amount nulo) cuenta
        # como fallo; por eso el YAML pone los chequeos de nulos ANTES
        invalid = ~F.coalesce(expr, F.lit(False))
        candidates.append(F.when(invalid, F.lit(rule["reject_reason"])))

    # --- integridad referencial: la llave foranea debe existir en la dimension ---
    # se resuelve con left join contra la dim; si el lado derecho quedo nulo pero
    # la llave de la transaccion no lo era, la fila es huerfana
    for chk in cfg.get("referential_checks", []):
        dim_col = f"_ref_{chk['name']}"
        orphan = F.col(chk["column"]).isNotNull() & F.col(dim_col).isNull()
        candidates.append(F.when(orphan, F.lit(chk["reject_reason"])))

    return df.withColumn("reject_reason", F.coalesce(*candidates))


def attach_dimension_keys(spark, df: DataFrame, cfg: dict, bronze_db: str) -> DataFrame:
    """Left join contra cada dimension para poder detectar llaves huerfanas."""
    for chk in cfg.get("referential_checks", []):
        dim = (spark.table(f"{CATALOG}.{bronze_db}.{chk['ref_table']}")
               .select(F.col(chk["ref_column"]).alias(f"_ref_{chk['name']}"))
               .distinct())
        df = df.join(
            F.broadcast(dim),
            df[chk["column"]] == F.col(f"_ref_{chk['name']}"),
            "left",
        )
    return df


# ==============================================================================
# 3. Deduplicacion
# ==============================================================================

def split_duplicates(df: DataFrame, cfg: dict):
    """
    Sobre las filas que sobrevivieron validaciones, conserva una sola version
    por llave: la mas reciente por `order_by`. Las versiones antiguas NO se
    tiran, se devuelven para ir a quarantine.
    """
    dd = cfg["deduplication"]
    order = F.col(dd["order_by"]).desc() if dd["keep"] == "latest" else F.col(dd["order_by"]).asc()
    w = Window.partitionBy(dd["key"]).orderBy(order)

    ranked = df.withColumn("_rn", F.row_number().over(w))
    unique = ranked.filter(F.col("_rn") == 1).drop("_rn")
    dupes = (ranked.filter(F.col("_rn") > 1).drop("_rn")
             .withColumn("reject_reason", F.lit(dd["reject_reason"])))
    return unique, dupes


# ==============================================================================
# 4. Perfilado
# ==============================================================================

def profile(df: DataFrame, run_ts) -> DataFrame:
    """
    Radiografia por columna: conteo, nulos, distintos y estadisticos de las
    numericas. Descriptivo, no rechaza nada. Se acumula por corrida para poder
    ver como evoluciona la calidad en el tiempo.

    PERFORMANCE — una sola pasada:
    Un `.collect()` por columna dispararia un job de Spark por columna, o sea
    N lecturas completas del dataset. Aqui se construyen TODAS las expresiones
    de TODAS las columnas y se ejecuta un unico agg(): 1 job, 1 lectura.

    El collect() no es riesgo de memoria: agg() reduce a UNA fila en el cluster
    antes de traer nada al driver, sin importar el volumen de entrada. El costo
    de hacerlo por columna es I/O repetido, no RAM.
    """
    numeric = ("int", "bigint", "double", "float", "decimal")
    cols = [(n, t) for n, t in df.dtypes if not n.startswith("_")]

    # --- se ARMAN todas las expresiones (barato: solo construye el plan) ---
    aggs = [F.count(F.lit(1)).alias("_total")]
    for name, dtype in cols:
        aggs.append(F.count(F.col(name)).alias(f"{name}__non_null"))
        aggs.append(F.approx_count_distinct(F.col(name)).alias(f"{name}__distinct"))
        if dtype.startswith(numeric):
            aggs.append(F.min(name).alias(f"{name}__min"))
            aggs.append(F.max(name).alias(f"{name}__max"))
            aggs.append(F.avg(name).alias(f"{name}__avg"))

    # --- se EJECUTA una sola vez (la unica accion de la funcion) ---
    r = df.agg(*aggs).collect()[0]
    total = r["_total"]

    rows = []
    for name, dtype in cols:
        is_num = dtype.startswith(numeric)
        nulls = total - r[f"{name}__non_null"]
        rows.append({
            "profiled_at": run_ts,
            "column_name": name,
            "data_type": dtype,
            "row_count": total,
            "null_count": nulls,
            "null_rate": round(nulls / total, 6) if total else None,
            "distinct_count": int(r[f"{name}__distinct"]),
            "min_value": str(r[f"{name}__min"]) if is_num and r[f"{name}__min"] is not None else None,
            "max_value": str(r[f"{name}__max"]) if is_num and r[f"{name}__max"] is not None else None,
            "mean_value": float(r[f"{name}__avg"]) if is_num and r[f"{name}__avg"] is not None else None,
        })

    return df.sparkSession.createDataFrame(rows)


# ==============================================================================
# 5. Chequeos por lote
# ==============================================================================

def compute_metric(chk: dict, ctx: dict):
    """Calcula el valor observado de la metrica que pide el chequeo."""
    m = chk["metric"]

    if m == "row_count":
        return float(ctx["bronze_count"])

    if m == "quarantine_rate":
        return ctx["quarantine_count"] / ctx["bronze_count"] if ctx["bronze_count"] else 0.0

    if m == "fraud_rate":
        return ctx["fraud_count"] / ctx["valid_count"] if ctx["valid_count"] else 0.0

    if m == "max_null_rate":
        # la peor tasa de nulos entre las columnas vigiladas.
        # un agg por columna serian N lecturas del dataset: se cuentan todas
        # en una sola pasada
        df, total = ctx["bronze_df"], ctx["bronze_count"]
        if not total:
            return 0.0
        r = df.agg(*[F.count(F.col(c)).alias(c) for c in chk["columns"]]).collect()[0]
        return max((total - r[c]) / total for c in chk["columns"])

    if m == "max_category_share":
        # que tanto domina la categoria mas frecuente de una columna
        df, total = ctx["valid_df"], ctx["valid_count"]
        if not total:
            return 0.0
        top = (df.groupBy(chk["column"]).count()
               .orderBy(F.desc("count")).limit(1).collect())
        return top[0]["count"] / total if top else 0.0

    if m == "column_mean":
        v = ctx["valid_df"].agg(F.avg(chk["column"])).collect()[0][0]
        return float(v) if v is not None else 0.0

    raise ValueError(f"metrica desconocida en el config: {m}")


def run_batch_checks(spark, cfg: dict, ctx: dict, run_ts):
    """
    Evalua cada chequeo por lote y devuelve (dataframe de resultados,
    lista de criticos fallados).

    severity critical -> el pipeline o el origen esta roto: aborta el job.
    severity warn     -> el dato cambio: se registra y se continua.
    """
    results, failed_critical = [], []

    for chk in cfg.get("batch_checks", []):
        observed = compute_metric(chk, ctx)
        lo, hi = chk.get("min"), chk.get("max")

        passed = True
        if lo is not None and observed < lo:
            passed = False
        if hi is not None and observed > hi:
            passed = False

        results.append({
            "checked_at": run_ts,
            "check_name": chk["name"],
            "severity": chk["severity"],
            "metric": chk["metric"],
            "observed_value": float(observed),
            "min_expected": float(lo) if lo is not None else None,
            "max_expected": float(hi) if hi is not None else None,
            "passed": passed,
            "description": chk.get("description"),
        })

        status = "PASS" if passed else "FAIL"
        print(f"  [{status}] {chk['name']:<32} {chk['severity']:<8} "
              f"observado={observed:.6g} esperado=[{lo}, {hi}]")

        if not passed and chk["severity"] == "critical":
            failed_critical.append(f"{chk['name']}: observado={observed:.6g} "
                                   f"esperado=[{lo}, {hi}] — {chk.get('description', '')}")

    return spark.createDataFrame(results), failed_critical


# ==============================================================================
# Escritura
# ==============================================================================

def write_table(df: DataFrame, table: str, partition_col: str = None):
    w = df.writeTo(f"{CATALOG}.{table}").tableProperty("format-version", "2")
    if partition_col:
        w = w.partitionedBy(F.col(partition_col))
    w.createOrReplace()


def append_table(spark, df: DataFrame, table: str):
    """Historial acumulativo (perfilado y chequeos): append, no reemplazo."""
    full = f"{CATALOG}.{table}"
    if spark.catalog.tableExists(full):
        df.writeTo(full).append()
    else:
        df.writeTo(full).tableProperty("format-version", "2").createOrReplace()


# ==============================================================================
# Main
# ==============================================================================

def main():
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "config_path", "bronze_db", "database"])
    bronze_db, db = args["bronze_db"], args["database"]
    run_ts = datetime.now(timezone.utc)

    sc = SparkContext.getOrCreate()
    glue = GlueContext(sc)
    spark = glue.spark_session
    job = Job(glue)
    job.init(args["JOB_NAME"], args)

    print(f"Cargando reglas desde {args['config_path']}")
    cfg = load_config(args["config_path"])
    print(f"  {len(cfg.get('row_validations', []))} validaciones por fila")
    print(f"  {len(cfg.get('referential_checks', []))} chequeos referenciales")
    print(f"  {len(cfg.get('batch_checks', []))} chequeos por lote")

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{db}")

    bronze = spark.table(f"{CATALOG}.{bronze_db}.transactions").cache()
    bronze_count = bronze.count()
    print(f"\nbronze.transactions: {bronze_count} filas")

    # --- 1-2. validaciones + referencial ---
    tagged = build_reject_reason(
        attach_dimension_keys(spark, bronze, cfg, bronze_db), cfg, {}
    )
    ref_cols = [f"_ref_{c['name']}" for c in cfg.get("referential_checks", [])]
    tagged = tagged.drop(*ref_cols).cache()

    rejected_rules = tagged.filter(F.col("reject_reason").isNotNull())
    survivors = tagged.filter(F.col("reject_reason").isNull())

    # --- 3. dedup sobre los sobrevivientes ---
    unique, dupes = split_duplicates(survivors, cfg)

    quarantine = (rejected_rules.unionByName(dupes)
                  .withColumn("quarantined_at", F.lit(run_ts))
                  .cache())
    quarantine_count = quarantine.count()

    valid = unique.drop("reject_reason").cache()
    valid_count = valid.count()

    print(f"validas={valid_count}  cuarentena={quarantine_count}")

    # --- invariante: nada se pierde en silencio ---
    assert valid_count + quarantine_count == bronze_count, (
        f"INVARIANTE ROTA: {valid_count} + {quarantine_count} != {bronze_count}. "
        "Alguna fila se perdio sin quedar registrada."
    )
    print("invariante OK: validas + cuarentena == bronze")

    # --- etiquetas de fraude: left join, respetando que pueden no existir aun ---
    # una transaccion sin etiqueta NO es "no fraude": es "aun no se sabe".
    # se conserva label_timestamp para poder filtrar point-in-time rio abajo.
    labels = spark.table(f"{CATALOG}.{bronze_db}.fraud_labels").select(
        "transaction_id", "is_fraud", "fraud_type", "label_timestamp"
    )
    enriched = valid.join(labels, "transaction_id", "left")

    fraud_count = enriched.filter(F.col("is_fraud") == 1).count()

    # --- 5. chequeos por lote ---
    print("\n--- chequeos de calidad por lote ---")
    ctx = {
        "bronze_df": bronze, "bronze_count": bronze_count,
        "valid_df": valid, "valid_count": valid_count,
        "quarantine_count": quarantine_count, "fraud_count": fraud_count,
    }
    checks_df, failed_critical = run_batch_checks(spark, cfg, ctx, run_ts)

    # la evidencia se persiste SIEMPRE, incluso si vamos a abortar: sin ella
    # nadie puede saber por que fallo la corrida
    append_table(spark, checks_df, f"{db}.quality_checks")

    if failed_critical:
        print("\nCHEQUEOS CRITICOS FALLADOS — no se publica Silver:")
        for f in failed_critical:
            print(f"  - {f}")
        raise RuntimeError(
            f"{len(failed_critical)} chequeo(s) critico(s) fallaron. "
            "Silver no se publico; revisa silver.quality_checks."
        )

    # --- publicacion ---
    write_table(enriched, f"{db}.transactions", partition_col="event_date")
    write_table(quarantine, f"{db}.quarantine")

    # --- 4. perfilado del dato publicado ---
    append_table(spark, profile(enriched, run_ts), f"{db}.data_profile")

    print(f"\nsilver.transactions: {valid_count} filas ({fraud_count} fraude)")
    print(f"silver.quarantine:   {quarantine_count} filas")
    print("\n--- motivos de rechazo ---")
    for r in (quarantine.groupBy("reject_reason").count()
              .orderBy(F.desc("count")).collect()):
        print(f"  {r['reject_reason']:<32} {r['count']:>5}")

    job.commit()


if __name__ == "__main__":
    main()