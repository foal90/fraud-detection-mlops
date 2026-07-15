# Fraud Detection MLOps Platform

> A build-in-public series: an end-to-end fraud-detection platform on AWS, built one layer at a time — from raw ingestion to model serving and drift monitoring.

**This repository is not about the model.** A fraud classifier is a handful of lines of XGBoost. The hard, valuable part of putting machine learning into production is everything *around* the model: reliable ingestion, trustworthy data, consistent features, reproducible training, safe serving, and drift monitoring. That plumbing is what this project builds.

## Series

| Part | Layer | Status |
|------|-------|--------|
| **1** | [Batch ingestion — Bronze on Iceberg](docs/part-1-bronze.md) | ✅ Complete |
| **2** | Silver — data quality, quarantine & profiling | 🔨 In progress |
| **3** | Feature pipeline & feature store | Planned |
| **4** | Training, experiment tracking & model registry | Planned |
| **5** | Serving & drift monitoring | Planned |

Each part is tagged in Git (`part-1-bronze`, …), so you can browse the repository exactly as it stood when that part shipped.

## Architecture

```
generate.py  ──►  raw (S3, JSONL)
                     │
                     ▼
             Bronze  (Glue PySpark → Apache Iceberg)     ✅ Part 1
                     │        typed, idempotent, lineage-tracked
                     ▼
             Silver  (validation, quarantine, profiling)  🔨 Part 2
                     │        trustworthy data + auditable rejects
                     ▼
         Feature pipeline → Feature store                 Part 3
                     │        point-in-time correct, train/serve consistent
                     ▼
         Training → MLflow → Model registry               Part 4
                     │
                     ▼
         Serving  +  Drift monitoring                     Part 5

   Terraform (IaC) + orchestration span the whole flow
```

## Stack

| Concern | Choice | Why |
|---|---|---|
| Storage | Amazon S3 | Cheap, decoupled from compute |
| Table format | **Apache Iceberg** | ACID, time travel, schema evolution — open, no vendor lock-in |
| Compute | AWS Glue 5.0 (Spark 3.5) | Serverless Spark: no cluster to run, zero cost at rest |
| Catalog | AWS Glue Data Catalog | Hive-Metastore-compatible |
| Query | Amazon Athena (Trino) | SQL straight over S3, pay per TB scanned |
| IaC | Terraform | Reproducible builds, clean teardown |
| Data generation | Python (Faker, NumPy) | Realistic transactions + delayed fraud labels |

Everything is serverless and pay-per-use. Nothing runs 24/7 — a full pipeline run costs cents, and `terraform destroy` returns the account to zero.

## Repository structure

```
fraud-detection-mlops/
├── README.md                     # you are here
├── docs/
│   └── part-1-bronze.md          # design decisions & deep dive per part
├── requirements.txt
├── run_bronze.sh                 # deploy + run the Bronze job end to end
├── config/
│   └── quality_rules.yaml        # declarative data quality rules (Part 2)
├── data_generator/
│   └── generate.py               # synthetic transactions + delayed labels + dimensions
├── glue_jobs/
│   └── bronze_ingest.py          # raw JSONL → idempotent Iceberg tables
└── infra/
    └── main.tf                   # S3, IAM, Glue Catalog DB, Glue jobs (Terraform)
```

## Quick start

**Prerequisites:** an AWS account, the AWS CLI configured (`aws configure`), and Terraform built for your CPU architecture (on Apple Silicon, verify with `file $(which terraform)` — a `x86_64` binary will hang under Rosetta).

All commands run from the repository root.

```bash
# Environment + dataset
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python data_generator/generate.py --customers 2000 --days 60 --out ./output --seed 42

# Infrastructure
terraform -chdir=infra init
terraform -chdir=infra apply

# Run the pipeline
./run_bronze.sh

# Tear down when finished
terraform -chdir=infra destroy
```

See [Part 1](docs/part-1-bronze.md) for what this builds, the design decisions behind it, and how to validate the results in Athena.

## Cost

All services are serverless and billed per use: a Glue run is a couple of DPU-minutes (cents), S3 storage for this dataset is fractions of a dollar per month, and the Glue Data Catalog stays inside the free tier. Running `terraform destroy` after each session keeps standing cost at zero. A budget alarm in AWS Budgets is recommended as a safety net.