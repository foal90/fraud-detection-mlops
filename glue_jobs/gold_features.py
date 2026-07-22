"""
gold_features.py — AWS Glue 5.0 (Spark 3.5 + Iceberg) — capa Gold (features).

EL MOTOR. Ejecuta las definiciones de features que entrega ciencia de datos
(el paquete features/, desplegado via --extra-py-files) y las materializa.

Este job NO decide que features existen ni opina sobre sus formulas. Su
trabajo es:
  - descubrir las features registradas y ejecutarlas
  - proveerles el frame de ventana point-in-time (lo hace features/__init__)
  - materializar los dos tiers
  - aparear cada fila con la etiqueta que YA SE CONOCIA en el corte
  - publicar el registro con dueño, tier y linaje

Tablas producidas (namespace = --database):
  customer_features   tier "shared": hechos de entidad, reutilizables
  <owner>_features    una por modelo (fraud_features, churn_features...):
                      sus features propias + las compartidas via join. Es lo
                      que ese modelo consume. Esa indireccion permite promover
                      una feature de "model" a "shared" sin romper consumidores.
                      Los dueños se descubren del registro, no se cablean.
  feature_registry    catalogo de lo declarado, con historial por corrida
                      (append). Registra DECLARACION, no consumo: el linaje
                      de quien lee que feature sale del entrenamiento.

===============================================================================
EL APAREO DE ETIQUETAS  (correctitud, no modelado)
===============================================================================
Una transaccion no se sabe fraudulenta al momento del swipe: el chargeback
llega dias despues. Entrenar con etiquetas que en ese momento no existian es
enseñarle al modelo a usar informacion del futuro.

Por eso cada fila trae:
  label_known   la etiqueta ya existia en el corte (--as_of)
  is_trainable  la fila es apta para entrenar

Una fila con label_known=false NO se rellena con is_fraud=0: eso seria
inventar negativos. Se marca como no entrenable y el set de entrenamiento la
excluye. Rellenar con 0 le enseñaria al modelo que el fraude reciente es
legitimo, solo porque su chargeback aun no llego.

Nota: aqui el apareo es EXACTO porque el feed de etiquetas trae
label_timestamp (cuando se supo). En un sistema donde solo el fraude genera
evento y no hay confirmacion de lo legitimo, se usaria en su lugar una
VENTANA DE MADUREZ: "una transaccion con mas de N dias y sin chargeback
cuenta como legitima", con N = el plazo maximo de reclamo.

===============================================================================
LIMITACION CONOCIDA — full refresh, igual que Silver
===============================================================================
Recalcula todas las features sobre toda la historia en cada corrida. A esta
escala son segundos. A escala de TB no sobrevive: las ventanas
(partitionBy + orderBy) barajan el dataset completo.

Como se resolveria: materializacion incremental por entidad, calculando solo
las ventanas de las entidades tocadas por el incremento y trayendo el estado
previo de la tabla ya materializada. El costo pasa de ser proporcional a la
historia a serlo a lo nuevo.

Job args:
  --silver_db  silver
  --database   gold
  --as_of      corte de conocimiento de etiquetas (ISO8601). Default: ahora.
Requiere: --datalake-formats iceberg  y  --extra-py-files <s3>/features.zip
"""
import sys
from datetime import datetime, timezone

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# El paquete de DS, desplegado como zip. Importar los modulos POBLA el
# REGISTRY via el decorador @feature: por eso se importan aunque no se usen
# por nombre.
import features
from features import Ctx, registered
import features.shared   # noqa: F401
import features.fraud    # noqa: F401

CATALOG = "glue_catalog"
ORDER_COL = features._ORDER_COL


# ==============================================================================
# Base
# ==============================================================================

def build_base(spark, silver_db: str) -> DataFrame:
    """
    silver.transactions enriquecida con los atributos de dimension que las
    features puedan necesitar (home_country, signup_date, risk_band...).

    Silver ya garantizo integridad referencial, asi que estos joins no pierden
    filas: lo huerfano quedo en cuarentena.
    """
    txn = spark.table(f"{CATALOG}.{silver_db}.transactions")
    cust = (spark.table(f"{CATALOG}.{silver_db}.customers")
            .select("customer_id", "home_country", "signup_date",
                    "account_tier", "age_band")) \
        if spark.catalog.tableExists(f"{CATALOG}.{silver_db}.customers") \
        else spark.table(f"{CATALOG}.bronze.customers").select(
            "customer_id", "home_country", "signup_date", "account_tier", "age_band")
    merch = spark.table(f"{CATALOG}.bronze.merchants").select(
        "merchant_id", F.col("category").alias("merchant_category"), "risk_band")

    return (txn
            .join(F.broadcast(cust), "customer_id", "left")
            .join(F.broadcast(merch), "merchant_id", "left")
            # columna interna: las ventanas por rango de tiempo necesitan un
            # valor numerico para acotar (rangeBetween no admite timestamps)
            .withColumn(ORDER_COL, F.col("event_timestamp").cast("long")))


# ==============================================================================
# Ejecucion de las features
# ==============================================================================

def apply_features(df: DataFrame, defs) -> tuple[DataFrame, list[str]]:
    """
    Ejecuta cada definicion y la agrega como columna.

    El bucle solo CONSTRUYE expresiones — barato. Spark las evalua todas en la
    misma pasada; features que comparten (entidad, ventana) reutilizan el mismo
    shuffle. Ejecutar por feature seria una pasada por feature.
    """
    names = []
    for d in defs:
        ctx = Ctx(entity=d.entity, window_seconds=d.window_seconds)
        df = df.withColumn(d.name, d.fn(ctx))
        names.append(d.name)
        print(f"  [{d.tier:<6}] {d.name:<28} entity={d.entity} "
              f"window={d.window_seconds or 'historial'}")
    return df, names


# ==============================================================================
# Etiquetas
# ==============================================================================

def attach_labels(df: DataFrame, as_of: datetime) -> DataFrame:
    """
    Marca que se sabia en el corte. NO rellena lo desconocido.

    Silver ya trajo is_fraud y label_timestamp con un LEFT join, asi que una
    transaccion sin etiqueta llega con nulos: eso significa "aun no se sabe",
    no "no fue fraude".
    """
    known = F.col("label_timestamp").isNotNull() & (F.col("label_timestamp") <= F.lit(as_of))
    return (df
            .withColumn("label_known", known)
            # is_fraud solo se expone cuando ya se conocia; si no, queda nulo
            # a proposito, para que sea imposible entrenar con ella por error
            .withColumn("is_fraud", F.when(known, F.col("is_fraud")))
            .withColumn("is_trainable", known))


# ==============================================================================
# Escritura
# ==============================================================================

def write_table(df: DataFrame, table: str, partition_col: str | None = None):
    w = df.writeTo(f"{CATALOG}.{table}").tableProperty("format-version", "2")
    if partition_col:
        w = w.partitionedBy(F.col(partition_col))
    w.createOrReplace()


def append_table(spark, df: DataFrame, table: str):
    """Historial acumulativo: append, no reemplazo."""
    full = f"{CATALOG}.{table}"
    if spark.catalog.tableExists(full):
        df.writeTo(full).append()
    else:
        df.writeTo(full).tableProperty("format-version", "2").createOrReplace()


def build_registry(spark, run_ts):
    """
    Catalogo de lo DECLARADO en los decoradores, con una foto por corrida.

    Se escribe con append, no reemplazo: asi queda la historia del catalogo
    (cuando aparecio una feature, cuando cambio su descripcion, cuando dejo
    de declararse). Con createOrReplace cada corrida pisaba a la anterior y
    no habia forma de ver la evolucion.

    OJO — esto registra DECLARACION, no CONSUMO. El campo `owner` dice quien
    la definio, no quien la lee. Saber que modelo consume que feature no lo
    puede saber este job: el motor materializa y no ve quien lee despues. Ese
    linaje sale del lado del entrenamiento (Parte 4), cuando cada corrida
    loguea la lista de features que uso. Cruzando ambos se responde "¿cuales
    se compartieron de verdad?", que es la pregunta que justifica el tier.
    """
    rows = [{
        "feature_name": d.name,
        "tier": d.tier,
        "owner": d.owner,
        "entity": d.entity,
        "window_seconds": d.window_seconds,
        "description": d.description,
        "materialized_at": run_ts,
    } for d in registered()]
    return spark.createDataFrame(rows)


# ==============================================================================
# Main
# ==============================================================================

def main():
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "silver_db", "database", "as_of"])
    silver_db, db = args["silver_db"], args["database"]
    run_ts = datetime.now(timezone.utc)
    as_of = (run_ts if args["as_of"] in ("now", "", None)
             else datetime.fromisoformat(args["as_of"]).replace(tzinfo=timezone.utc))

    sc = SparkContext.getOrCreate()
    glue = GlueContext(sc)
    spark = glue.spark_session
    job = Job(glue)
    job.init(args["JOB_NAME"], args)

    shared_defs = registered(tier="shared")
    owners = sorted({d.owner for d in registered(tier="model") if d.owner})
    print(f"Registro: {len(shared_defs)} features shared")
    for o in owners:
        print(f"          {len(registered(tier='model', owner=o)):>2} features de '{o}'")
    print(f"Corte de etiquetas (--as_of): {as_of.isoformat()}")

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{db}")

    base = build_base(spark, silver_db).cache()
    print(f"\nsilver.transactions: {base.count()} filas")

    # --- tier 1: features de entidad, compartidas ---
    print("\n--- tier shared -> customer_features ---")
    with_shared, shared_names = apply_features(base, shared_defs)
    customer_features = with_shared.select(
        "transaction_id", "customer_id", "event_timestamp", "event_date", *shared_names
    ).cache()
    write_table(customer_features, f"{db}.customer_features", partition_col="event_date")

    # --- tier 2: una tabla por modelo, con sus features + las compartidas ---
    # Los dueños salen del REGISTRO, no de una lista cableada aqui: un modelo
    # nuevo agrega su features/<owner>.py y el motor le materializa su tabla
    # sin que nadie toque este archivo.
    #
    # Cada modelo consume SU tabla, nunca customer_features directo. Asi, si una
    # feature pasa de "model" a "shared", se mueve su definicion y la tabla del
    # modelo la sigue exponiendo via el join: el consumidor no cambia nada.
    shared_cols = customer_features.drop("customer_id", "event_timestamp", "event_date")

    for model_owner in owners:
        defs = registered(tier="model", owner=model_owner)
        print(f"\n--- tier model ({model_owner}) -> {model_owner}_features ---")
        with_model, model_names = apply_features(base, defs)
        t = (with_model
             .select("transaction_id", "customer_id", "merchant_id", "event_timestamp",
                     "event_date", "amount", "channel", "country",
                     "is_fraud", "fraud_type", "label_timestamp", *model_names)
             .join(shared_cols, "transaction_id", "inner"))
        t = attach_labels(t, as_of).cache()
        write_table(t, f"{db}.{model_owner}_features", partition_col="event_date")

        total = t.count()
        trainable = t.filter(F.col("is_trainable")).count()
        positives = t.filter(F.col("is_fraud") == 1).count()
        print(f"  gold.{model_owner}_features: {total} filas, "
              f"{len(shared_names) + len(model_names)} features "
              f"({len(shared_names)} compartidas + {len(model_names)} propias)")
        print(f"    entrenables (etiqueta conocida al corte): {trainable} "
              f"({100 * trainable / max(total, 1):.1f}%)")
        print(f"    no entrenables (etiqueta aun no llegaba): {total - trainable}")
        print(f"    positivas entre las entrenables: {positives}")
        t.unpersist()

    # --- registro ---
    append_table(spark, build_registry(spark, run_ts), f"{db}.feature_registry")
    print(f"\ngold.customer_features: {len(shared_names)} features compartidas")
    print(f"gold.feature_registry:  {len(registered())} features catalogadas")

    job.commit()


if __name__ == "__main__":
    main()