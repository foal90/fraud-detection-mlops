# Part 1 — Batch ingestion: Bronze on Iceberg

*[← Back to overview](../README.md) · Next: Part 2 — Silver*

The offline foundation of the platform: generate realistic transaction data, land it in a serverless lakehouse, and make the ingestion trustworthy enough to build on.

```
generate.py  ──►  raw (S3, JSONL)  ──►  Bronze (Glue PySpark → Iceberg)  ──►  Athena
                                          typed · idempotent · lineage-tracked
```

**What ships in this part**

- A data generator producing transactions, a **delayed** fraud-label feed, and reference dimensions
- A Glue PySpark job landing raw JSONL as Iceberg tables in the Glue Data Catalog
- Terraform for the whole stack: S3 bucket, IAM role, catalog database, Glue job
- `run_bronze.sh` — one command to deploy and run the pipeline

**Bronze tables**

| Table | Load pattern | Partitioned by |
|---|---|---|
| `bronze.transactions` | insert-only (`MERGE`, no update) | `event_date` |
| `bronze.fraud_labels` | upsert (`MERGE` with update) | `label_date` |
| `bronze.customers` / `cards` / `merchants` | full snapshot | — |

---

## Design decisions

### Point-in-time correctness — no label leakage

In real life you don't know a transaction is fraudulent at swipe time. You find out days or weeks later, when a chargeback arrives. A generator that stamps `is_fraud` on each transaction at creation is quietly teaching the model to use information from the future — and that model collapses in production.

So the generator models the delay honestly:

- Transactions carry **`event_timestamp`** (when it happened) separate from **`ingestion_timestamp`** (when it landed).
- Fraud labels live in **their own feed**, with a `label_timestamp` set days after the event.
- Labels are partitioned by the day they became **known**, not the day of the event.

This forces point-in-time-correct joins downstream, and makes it possible to reconstruct what was actually knowable at any moment in time.

### Schema-on-read, enforced

Bronze imposes an explicit schema on the raw JSONL rather than letting Spark infer it. Inference is non-deterministic — types can shift depending on which file gets sampled first. Declaring the schema puts the type contract under version control at the boundary of the system, so downstream layers never fight surprise casts.

### Idempotent ingestion

The job loads via Iceberg `MERGE` on the natural key: new rows insert, existing rows are left alone. Re-running is always safe — jobs crash, get retried, and get triggered twice, and none of that duplicates data.

Transactions are **insert-only**; fraud labels **allow updates**, because a chargeback can be reversed and a label reclassified. Modelling that difference is a semantic choice, not a technical one.

### Iceberg over plain Parquet

Iceberg is an open Apache table format (born at Netflix, not an AWS product) that turns a pile of Parquet files into a real table: ACID transactions, time travel, schema evolution, and `MERGE`. It runs on any engine — Spark, Trino, Snowflake, another cloud — so the lakehouse stays portable.

### Glue over EMR

Glue is serverless Spark: no cluster to size, patch, or leave running. You pay per DPU-second and nothing at rest. EMR offers more control at the cost of more operations and a meter that never stops.

### Infrastructure as Code

The full stack is declared in Terraform. `terraform apply` builds it, `terraform destroy` removes it cleanly — which doubles as cost control, since there's no chance of leaving an orphaned resource billing quietly.

### Repeatable operations

Running the pipeline is one command (`./run_bronze.sh`) rather than a sequence of copy-pasted CLI calls. The script fails fast (`set -euo pipefail`), validates prerequisites before touching anything, sources the bucket and job name from Terraform outputs instead of hardcoded values, and polls the job to completion.

---

## How to run

All commands from the repository root. See the [README](../README.md#quick-start) for prerequisites.

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python data_generator/generate.py --customers 2000 --days 60 --out ./output --seed 42

terraform -chdir=infra init
terraform -chdir=infra apply

./run_bronze.sh

terraform -chdir=infra destroy
```

Defaults can be overridden without editing the script:

```bash
REGION=us-west-2 RAW_DIR=data/raw ./run_bronze.sh
```

## Validation

In Athena, against the `bronze` database:

```sql
-- fraud distribution by injected pattern
SELECT fraud_type, COUNT(*) AS n
FROM bronze.fraud_labels
WHERE is_fraud = 1
GROUP BY fraud_type;
```

```sql
-- Iceberg snapshots: version history and idempotency proof
SELECT committed_at, snapshot_id, parent_id, operation,
       summary['added-records'] AS added,
       summary['total-records'] AS total
FROM "bronze"."transactions$snapshots"
ORDER BY committed_at;
```

Re-running the job appends a snapshot whose `parent_id` points at the previous one, with **`added-records` absent (zero)** and `total-records` unchanged — the `MERGE` matched every key and inserted nothing. That's idempotency, visible in the metadata.

```sql
-- the physical Parquet files behind the table
SELECT file_path, record_count, file_size_in_bytes
FROM "bronze"."transactions$files";
```

Note the quoting: the whole `table$metadata` identifier goes inside one pair of double quotes — `"bronze"."transactions$snapshots"`. Splitting it triggers a `TABLE_REDIRECTION_ERROR`.

---

## What I learned

**Idempotency is what makes a pipeline trustworthy.** Because ingestion uses `MERGE` on the natural key, the job can run any number of times and the table stays identical. I could watch it directly in the Iceberg snapshots: re-runs added a snapshot with zero new records. In the real world jobs crash, get retried, and get triggered twice — idempotency means re-running is always safe.

**Iceberg internals are just pointers.** A table is a chain: catalog pointer → metadata file (schema + snapshot list) → manifest lists → manifest files (with per-column min/max stats) → Parquet data files. Data files are **immutable** — an update writes new files, new metadata, and then atomically moves the catalog's single pointer. That atomic pointer swap is where ACID comes from; keeping the old metadata around is where time travel comes from. A table is an immutable version tree, and the catalog is the finger pointing at the current one.

**A `MERGE` source must be deterministic.** The job failed with `INVALID_NON_DETERMINISTIC_EXPRESSIONS` on its second run. `MERGE` scans the source more than once, and the ingestion adds a `current_timestamp()` lineage column that returns a different value on each evaluation — so Spark rejected the plan before executing anything. The fix is to materialize the source to a temporary table first, freezing those values as data. It also improves the audit column: every row in a run now shares one consistent timestamp instead of drifting by microseconds.

**Serverless keeps a learning lab nearly free.** A full run costs a few cents; `terraform destroy` returns the account to zero.

**Architecture mismatches are silent killers.** An `x86_64` Terraform binary on Apple Silicon downloads an `x86_64` provider plugin, which hangs at 100% CPU under Rosetta with a `timeout while waiting for plugin to start`. Nothing in the error points at architecture. `file $(which terraform)` is the diagnostic.

---

## Mapping to the Hadoop world

For anyone coming from on-prem Hadoop, the pieces map cleanly — with one common mix-up:

| Hadoop | Here |
|---|---|
| HDFS | Amazon S3 |
| Spark / MapReduce on YARN | AWS Glue (serverless Spark) |
| Hive Metastore | Glue Data Catalog (literally compatible) |
| Impala | **Athena** |
| Hive tables | **Apache Iceberg** — the table format, but transactional |

Iceberg is *not* the Impala equivalent; Athena is. Iceberg is the successor to the Hive table format.

The deeper difference: Hadoop **couples** storage and compute on the same nodes, and the cluster runs 24/7 regardless of use. The cloud lakehouse **decouples** them — S3 holds the data, Glue and Athena spin up compute on demand and scale independently. That decoupling is why this costs cents instead of a standing bill.