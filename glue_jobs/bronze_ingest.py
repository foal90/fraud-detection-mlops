"""
bronze_ingest.py — AWS Glue 5.0 (Spark 3.5 + Iceberg) — capa Bronze.

Lee el raw landing JSONL desde S3 y lo materializa como tablas Iceberg en el
Glue Data Catalog. Filosofia Bronze: transformacion minima (parsear, tipar,
linaje) + IDEMPOTENCIA via MERGE sobre la llave natural. La limpieza es Silver.

Job args:  --raw_path s3://<bucket>/raw   --database bronze
"""
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

CATALOG = "glue_catalog"

TXN_SCHEMA = T.StructType([
    T.StructField("transaction_id", T.StringType()),
    T.StructField("event_timestamp", T.TimestampType()),
    T.StructField("customer_id", T.StringType()),
    T.StructField("card_id", T.StringType()),
    T.StructField("merchant_id", T.StringType()),
    T.StructField("amount", T.DoubleType()),
    T.StructField("currency", T.StringType()),
    T.StructField("channel", T.StringType()),
    T.StructField("country", T.StringType()),
    T.StructField("device_id", T.StringType()),
    T.StructField("ingestion_timestamp", T.TimestampType()),
])
LABEL_SCHEMA = T.StructType([
    T.StructField("transaction_id", T.StringType()),
    T.StructField("is_fraud", T.IntegerType()),
    T.StructField("fraud_type", T.StringType()),
    T.StructField("label_timestamp", T.TimestampType()),
])
CUSTOMER_SCHEMA = T.StructType([
    T.StructField("customer_id", T.StringType()),
    T.StructField("signup_date", T.DateType()),
    T.StructField("home_country", T.StringType()),
    T.StructField("age_band", T.StringType()),
    T.StructField("account_tier", T.StringType()),
])
CARD_SCHEMA = T.StructType([
    T.StructField("card_id", T.StringType()),
    T.StructField("customer_id", T.StringType()),
    T.StructField("issue_date", T.DateType()),
    T.StructField("card_type", T.StringType()),
    T.StructField("network", T.StringType()),
])
MERCHANT_SCHEMA = T.StructType([
    T.StructField("merchant_id", T.StringType()),
    T.StructField("name", T.StringType()),
    T.StructField("category", T.StringType()),
    T.StructField("country", T.StringType()),
    T.StructField("risk_band", T.StringType()),
])


def read_jsonl(spark, path: str, schema: T.StructType) -> DataFrame:
    """JSONL crudo con esquema impuesto + linaje. recursiveFileLookup lee todas
    las particiones dt=* sin que Spark las infiera como columnas."""
    return (
        spark.read
        .option("recursiveFileLookup", "true")
        .schema(schema)
        .json(path)
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_bronze_ingested_at", F.current_timestamp())
    )


def merge_incremental(spark, df: DataFrame, table: str, key: str,
                      partition_col: str, update: bool = False):
    """Carga idempotente: crea si no existe, si no MERGE sobre la llave natural."""
    full = f"{CATALOG}.{table}"
    if not spark.catalog.tableExists(full):
        (df.writeTo(full).partitionedBy(partition_col)
           .tableProperty("format-version", "2").createOrReplace())
        return

    # El MERGE escanea el source varias veces, asi que DEBE ser determinista.
    # current_timestamp() e input_file_name() (columnas de linaje) no lo son.
    # Solucion: materializar el staging en una tabla temporal; al persistirlo,
    # esos valores quedan congelados como datos y leerlos de vuelta es determinista.
    staging = f"{CATALOG}.{table}_staging"
    df.writeTo(staging).createOrReplace()

    cols = df.columns
    matched = ""
    if update:
        set_clause = ", ".join(f"t.{c} = s.{c}" for c in cols if c != key)
        matched = f"WHEN MATCHED THEN UPDATE SET {set_clause}\n"
    spark.sql(f"""
        MERGE INTO {full} t USING {staging} s ON t.{key} = s.{key}
        {matched}WHEN NOT MATCHED THEN INSERT ({", ".join(cols)})
        VALUES ({", ".join(f"s.{c}" for c in cols)})
    """)
    spark.sql(f"DROP TABLE IF EXISTS {staging} PURGE")


def overwrite_snapshot(spark, df: DataFrame, table: str):
    (df.writeTo(f"{CATALOG}.{table}")
       .tableProperty("format-version", "2").createOrReplace())


def main():
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "raw_path", "database"])
    raw, db = args["raw_path"].rstrip("/"), args["database"]

    sc = SparkContext.getOrCreate()
    glue = GlueContext(sc)
    spark = glue.spark_session
    job = Job(glue)
    job.init(args["JOB_NAME"], args)

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{db}")

    txn = (read_jsonl(spark, f"{raw}/transactions/", TXN_SCHEMA)
           .withColumn("event_date", F.to_date("event_timestamp")))
    merge_incremental(spark, txn, f"{db}.transactions",
                      key="transaction_id", partition_col="event_date", update=False)

    lab = (read_jsonl(spark, f"{raw}/fraud_labels/", LABEL_SCHEMA)
           .withColumn("label_date", F.to_date("label_timestamp")))
    merge_incremental(spark, lab, f"{db}.fraud_labels",
                      key="transaction_id", partition_col="label_date", update=True)

    overwrite_snapshot(spark, read_jsonl(spark, f"{raw}/dimensions/customers.jsonl", CUSTOMER_SCHEMA), f"{db}.customers")
    overwrite_snapshot(spark, read_jsonl(spark, f"{raw}/dimensions/cards.jsonl", CARD_SCHEMA), f"{db}.cards")
    overwrite_snapshot(spark, read_jsonl(spark, f"{raw}/dimensions/merchants.jsonl", MERCHANT_SCHEMA), f"{db}.merchants")

    for t in ["transactions", "fraud_labels", "customers", "cards", "merchants"]:
        print(f"bronze.{t}: {spark.table(f'{CATALOG}.{db}.{t}').count()} filas")

    job.commit()


if __name__ == "__main__":
    main()