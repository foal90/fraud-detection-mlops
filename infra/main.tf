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
}

# ---------- Data lake (raw, warehouse, scripts, temp viven aqui por prefijo) ----------
resource "aws_s3_bucket" "lake" {
  bucket        = local.bucket
  force_destroy = true # permite destroy aunque tenga objetos (cleanup limpio)
}

# Sube el script del job al bucket
resource "aws_s3_object" "bronze_script" {
  bucket = aws_s3_bucket.lake.id
  key    = "scripts/bronze_ingest.py"
  source = "${path.module}/../glue_jobs/bronze_ingest.py"
  etag   = filemd5("${path.module}/../glue_jobs/bronze_ingest.py")
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

# Permisos base de Glue (catalogo, logs)
resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# Acceso al bucket del lake
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

# ---------- Glue Job ----------
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
    # Iceberg + catalogo Glue, fijado al arrancar la sesion Spark
    "--conf" = join(" --conf ", [
      "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
      "spark.sql.catalog.${"glue_catalog"}=org.apache.iceberg.spark.SparkCatalog",
      "spark.sql.catalog.glue_catalog.warehouse=s3://${aws_s3_bucket.lake.id}/warehouse/",
      "spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog",
      "spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO",
      "spark.sql.defaultCatalog=glue_catalog",
    ])
  }
}

output "bucket" {
  value = aws_s3_bucket.lake.id
}
output "glue_job" {
  value = aws_glue_job.bronze_ingest.name
}
