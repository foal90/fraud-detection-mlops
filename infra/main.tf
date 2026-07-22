terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.0" }
  }
}

provider "aws" {
  region = var.region
}

variable "region" {
  default = "us-east-1"
}

variable "project" {
  default = "fraud-mlops"
}

data "aws_caller_identity" "me" {}

locals {
  # nombre globalmente unico: proyecto + id de cuenta
  bucket = "${var.project}-lake-${data.aws_caller_identity.me.account_id}"

  # Config de Iceberg + catalogo Glue, compartida por TODOS los jobs.
  # Debe fijarse al arrancar la sesion Spark, no dentro del codigo.
  # El alias "glue_catalog" tiene que coincidir con la constante CATALOG
  # de los scripts de PySpark.
  iceberg_conf = join(" --conf ", [
    "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.glue_catalog.warehouse=s3://${local.bucket}/warehouse/",
    "spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog",
    "spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO",
    "spark.sql.defaultCatalog=glue_catalog",
  ])
}

# ---------- Data lake (raw, warehouse, scripts, config, temp por prefijo) ----------
resource "aws_s3_bucket" "lake" {
  bucket        = local.bucket
  force_destroy = true # permite destroy aunque tenga objetos (cleanup limpio)
}

resource "aws_s3_object" "bronze_script" {
  bucket = aws_s3_bucket.lake.id
  key    = "scripts/bronze_ingest.py"
  source = "${path.module}/../glue_jobs/bronze_ingest.py"
  etag   = filemd5("${path.module}/../glue_jobs/bronze_ingest.py")
}

resource "aws_s3_object" "silver_script" {
  bucket = aws_s3_bucket.lake.id
  key    = "scripts/silver_clean.py"
  source = "${path.module}/../glue_jobs/silver_clean.py"
  etag   = filemd5("${path.module}/../glue_jobs/silver_clean.py")
}

resource "aws_s3_object" "gold_script" {
  bucket = aws_s3_bucket.lake.id
  key    = "scripts/gold_features.py"
  source = "${path.module}/../glue_jobs/gold_features.py"
  etag   = filemd5("${path.module}/../glue_jobs/gold_features.py")
}

# OJO: features.zip NO se gestiona aqui. Comprimir el paquete de features es un
# paso de BUILD, no infraestructura, y vive en run_gold.sh. Asi ciencia de datos
# puede agregar archivos a features/ sin tocar este .tf. El job de abajo apunta
# a la ruta en S3; run_gold.sh la publica antes de disparar.

# Las reglas de calidad viven versionadas en el repo; aqui se publican a S3
# para que el job las lea en tiempo de ejecucion.
resource "aws_s3_object" "quality_rules" {
  bucket = aws_s3_bucket.lake.id
  key    = "config/quality_rules.yaml"
  source = "${path.module}/../config/quality_rules.yaml"
  etag   = filemd5("${path.module}/../config/quality_rules.yaml")
}

# ---------- Rol IAM para Glue ----------
data "aws_iam_policy_document" "glue_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue" {
  name               = "${var.project}-glue-role"
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_s3" {
  name = "${var.project}-glue-s3"
  role = aws_iam_role.glue.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
      Resource = [aws_s3_bucket.lake.arn, "${aws_s3_bucket.lake.arn}/*"]
    }]
  })
}

# ---------- Glue Data Catalog ----------
resource "aws_glue_catalog_database" "bronze" {
  name = "bronze"
}

resource "aws_glue_catalog_database" "silver" {
  name = "silver"
}

resource "aws_glue_catalog_database" "gold" {
  name = "gold"
}

# ---------- Glue Jobs ----------
resource "aws_glue_job" "bronze_ingest" {
  name              = "${var.project}-bronze-ingest"
  role_arn          = aws_iam_role.glue.arn
  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = 2

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.lake.id}/scripts/bronze_ingest.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"     = "python"
    "--datalake-formats" = "iceberg"
    "--raw_path"         = "s3://${aws_s3_bucket.lake.id}/raw"
    "--database"         = aws_glue_catalog_database.bronze.name
    "--TempDir"          = "s3://${aws_s3_bucket.lake.id}/temp/"
    "--enable-metrics"   = "true"
    "--conf"             = local.iceberg_conf
  }
}

resource "aws_glue_job" "silver_clean" {
  name              = "${var.project}-silver-clean"
  role_arn          = aws_iam_role.glue.arn
  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = 2

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.lake.id}/scripts/silver_clean.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"     = "python"
    "--datalake-formats" = "iceberg"
    # PyYAML no viene en el runtime de Glue; el job lo necesita para leer
    # las reglas de calidad desde el YAML
    "--additional-python-modules" = "pyyaml"
    "--config_path"               = "s3://${aws_s3_bucket.lake.id}/config/quality_rules.yaml"
    "--bronze_db"                 = aws_glue_catalog_database.bronze.name
    "--database"                  = aws_glue_catalog_database.silver.name
    "--TempDir"                   = "s3://${aws_s3_bucket.lake.id}/temp/"
    "--enable-metrics"            = "true"
    "--conf"                      = local.iceberg_conf
  }
}

resource "aws_glue_job" "gold_features" {
  name              = "${var.project}-gold-features"
  role_arn          = aws_iam_role.glue.arn
  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = 2

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.lake.id}/scripts/gold_features.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"     = "python"
    "--datalake-formats" = "iceberg"
    # El paquete de features que entrega ciencia de datos. Glue lo agrega a
    # sys.path, y el motor hace `import features`. run_gold.sh lo publica.
    "--extra-py-files" = "s3://${aws_s3_bucket.lake.id}/scripts/features.zip"
    "--silver_db"      = aws_glue_catalog_database.silver.name
    "--database"       = aws_glue_catalog_database.gold.name
    # corte de conocimiento de etiquetas; run_gold.sh lo sobrescribe por corrida
    "--as_of"          = "now"
    "--TempDir"        = "s3://${aws_s3_bucket.lake.id}/temp/"
    "--enable-metrics" = "true"
    "--conf"           = local.iceberg_conf
  }
}

output "bucket" {
  value = aws_s3_bucket.lake.id
}
output "glue_job" {
  value = aws_glue_job.bronze_ingest.name
}
output "glue_job_silver" {
  value = aws_glue_job.silver_clean.name
}
output "glue_job_gold" {
  value = aws_glue_job.gold_features.name
}