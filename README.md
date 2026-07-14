# Fraud Detection MLOps Platform

> A build-in-public series where I learn MLOps and data engineering on AWS by building a fraud-detection platform end to end — from raw ingestion to model serving and monitoring.

This repository is **not about the model**. A fraud classifier is a handful of lines of XGBoost. The hard, valuable part of putting machine learning into production is everything *around* the model: reliable ingestion, consistent features, reproducible training, safe serving, and drift monitoring. That plumbing is what this project builds, one layer at a time.

**Series index**
- **Part 1 — Batch ingestion layer (this post)** ✅
- Part 2 — Silver: cleaning & conforming *(next)*
- Part 3 — Feature pipeline & feature store
- Part 4 — Training, experiment tracking & model registry
- Part 5 — Serving & drift monitoring

---

## Part 1 — What this covers

The offline / batch foundation: generating realistic data and landing it in a serverless lakehouse, provisioned entirely as code.

```
data_generator.py  ──►  raw (S3, JSONL)
                     │
                     ▼
             Bronze  (Glue PySpark → Apache Iceberg)   ◄── YOU ARE HERE
                     │
                     ▼
             Silver  (cleaning & conforming)            ── next
                     │
                     ▼
         Feature pipeline → Feature store
                     │
                     ▼
         Training → MLflow → Model registry
                     │
                     ▼
         Serving  +  Drift monitoring

   Orchestration + Terraform (IaC) sit across the whole flow
```

## Tech stack

- **Storage / table format:** Amazon S3 + Apache Iceberg (open lakehouse, no vendor lock-in)
- **Compute:** AWS Glue 5.0 (serverless Spark 3.5)
- **Catalog:** AWS Glue Data Catalog
- **Query engine:** Amazon Athena (Trino under the hood)
- **Infrastructure as Code:** Terraform
- **Data generation:** Python (Faker, NumPy)

Everything is serverless and pay-per-use. Nothing runs 24/7, so the whole lab costs cents while active and $0 when torn down.

## Key design decisions

These are the choices that make this a production-shaped pipeline rather than a tutorial.

**Point-in-time correctness (no label leakage).** In real life you don't know a transaction is fraudulent at swipe time — you find out days or weeks later, when a chargeback arrives. The generator models this honestly: transactions carry a separate `event_timestamp` and `ingestion_timestamp`, and fraud labels live in their own **delayed feed** with a `label_timestamp` days after the event. This forces point-in-time-correct feature joins downstream and avoids training a model on information from the future.

**Schema-on-read enforcement.** Bronze imposes an explicit schema on the raw JSONL instead of letting Spark infer types. The type contract is controlled at the boundary of the system, so downstream layers never fight surprise casts.

**Idempotent ingestion via Iceberg `MERGE`.** The Bronze job can be re-run any number of times without duplicating data — new rows are inserted, existing ones (matched on the natural key) are left alone. Re-running a failed job is safe.

**Infrastructure as Code.** The full stack — S3 bucket, IAM role, catalog database, Glue job — is declared in Terraform. `terraform apply` builds it; `terraform destroy` tears it down cleanly (which doubles as cost control).

**Repeatable operations.** Running the pipeline is a single command (`./run_bronze.sh`) rather than a sequence of copy-pasted CLI calls. The script fails fast, validates its prerequisites, sources configuration from Terraform outputs instead of hardcoded values, and polls the job to completion — so a run is reproducible for anyone who clones the repo.

## Repository structure

```
fraud-detection-mlops/
├── README.md
├── requirements.txt          # pinned Python dependencies
├── run_bronze.sh             # deploy + run the Bronze job end to end
├── data_generator/
│   └── data_generator.py           # synthetic transactions + delayed fraud labels + dimensions
├── glue_jobs/
│   └── bronze_ingest.py      # raw JSONL → idempotent Iceberg tables
└── infra/
    └── main.tf               # S3, IAM, Glue Catalog DB, Glue job (Terraform)
```

> Note: the Terraform `aws_s3_object` uploads the Glue script, so its `source` path points at wherever `bronze_ingest.py` lives.

## How to run

**Prerequisites:** an AWS account, the AWS CLI configured (`aws configure`), and Terraform (built for your CPU architecture — on Apple Silicon, verify with `file $(which terraform)`).

All commands run from the repository root.

```bash
# 1. Set up the Python environment and generate the dataset
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python data_generator/generate.py --customers 2000 --days 60 --out ./output --seed 42

# 2. Provision infrastructure
terraform -chdir=infra init
terraform -chdir=infra apply

# 3. Deploy and run the Bronze job (uploads script, syncs raw, polls to completion)
./run_bronze.sh

# 4. Tear everything down when finished
terraform -chdir=infra destroy
```

`run_bronze.sh` reads the bucket and job name straight from the Terraform outputs, uploads the Glue script, syncs the raw landing to S3, triggers the job, and polls until it reports `SUCCEEDED` or fails with its error message. Defaults can be overridden without editing the script:

```bash
REGION=us-west-2 RAW_DIR=data/raw ./run_bronze.sh
```

**Validate in Athena** (database `bronze`):

```sql
-- fraud distribution by pattern
SELECT fraud_type, COUNT(*) AS n
FROM bronze.fraud_labels
WHERE is_fraud = 1
GROUP BY fraud_type;

-- Iceberg snapshots (version history + idempotency proof)
SELECT * FROM "bronze"."transactions$snapshots";
```

## What I learned in Part 1

- **Idempotency is what makes a pipeline trustworthy.** Because ingestion uses a `MERGE` on the natural key, the job can be re-run any number of times and the table stays identical — one run or ten, same result. I could see it directly in the Iceberg snapshots: re-runs added a new snapshot with `added-records = 0`. In the real world jobs crash, get retried, and get triggered twice; idempotency means re-running is always safe.
- **Iceberg internals are just pointers.** A table is a catalog pointer → metadata file → manifest lists → manifest files → Parquet data files. Writes create new immutable files and atomically move the pointer, which is where ACID and time travel come from.
- **A `MERGE` source must be deterministic.** Spark rejected the MERGE because the ingestion adds a `current_timestamp()` lineage column, and MERGE scans the source more than once. The fix — materializing the source to a temporary table so those values freeze — also produces a cleaner audit column (one consistent timestamp per run).
- **Serverless keeps a learning lab nearly free.** The full run costs a few cents; `terraform destroy` returns the account to zero.

## Cost

All services are serverless and billed per use. A full ingestion run is a couple of Glue DPU-minutes (cents); S3 storage for this dataset is fractions of a dollar per month. Running `terraform destroy` after each session keeps standing cost at zero.

---

*Part 2 builds the Silver layer: cleaning, conforming, and validating the Bronze tables into trustworthy data ready for feature engineering.*