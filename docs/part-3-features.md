# Part 3 — Gold: the feature layer

*[← Back to overview](../README.md) · [Part 2 — Silver](part-2-silver.md) · Next: Part 4 — Training*

Silver made the data trustworthy. Gold turns it into what a model consumes — one row per transaction, each described **only with what was knowable at the moment it happened**.

```
silver.transactions ──►  feature engine  ──►  gold.customer_features   (shared)
   (+ dimensions)             │                gold.fraud_features      (model)
                              │                gold.feature_registry    (catalog)
                       point-in-time windows
```

**What ships in this part**

- A `features/` package where feature definitions live as Python — "delivered by data science"
- A Glue engine that discovers, executes and materializes them
- Point-in-time windows the platform guarantees, so a feature can't see its own future
- Label-maturity handling: a transaction is only trainable once its chargeback window has closed
- `run_gold.sh` — packages the feature code into a zip and ships it via `--extra-py-files`

**Gold tables**

| Table | Contents |
|---|---|
| `gold.customer_features` | shared, entity-level features (key: `customer_id` + time) |
| `gold.<owner>_features` | one per model: its own features + the shared ones, joined |
| `gold.feature_registry` | catalog of declared features, appended per run |

---

## The role this project plays

This isn't about designing a model — it's about running one in production. So *which* features to compute is not a decision made here; it belongs to data science. The platform's job is to **execute, materialize, version and reproduce** whatever definitions it's handed, and to make it **impossible to compute them incorrectly** in ways that are systems problems rather than modeling choices.

That draws a clean line:

| | Owner |
|---|---|
| Which features, what formula, what window | **Data science** |
| The engine that runs them, and its documented semantics | **Platform** |
| Point-in-time correctness, reproducibility, materialization | **Platform** |
| Pairing each row with the label knowable at a cutoff | **Platform** |
| Model preprocessing (scaling, encoding, imputation) | **The model** — not Gold |

## Design decisions

### Definitions in code, not a config vocabulary

An earlier design put features in YAML with a fixed set of rule "types". That has a ceiling: the day someone wants an exponential moving average or a haversine distance, they can't express it — they'd file a ticket to extend the engine. Real feature stores (Feast, Tecton) define features in **code** for exactly this reason.

So features are Python functions. The platform doesn't restrict the vocabulary; it provides a frame and guarantees a property:

```python
@feature(tier="shared", entity="customer_id", windows=[300, 3600, 86400])
def txn_count(f):
    return F.count("*").over(f.window)
```

The `f` argument (a `Ctx`) hands the function a window that's **already** partitioned by the entity, ordered by event time, and bounded to exclude the current row. Data science writes any Spark expression over it; the platform owns the frame.

### One definition, many features

`windows=[300, 3600, 86400]` registers `txn_count_5min`, `txn_count_1h`, `txn_count_24h` from a single definition. Adding a window is adding a number to a list — no logic touched, no consumer broken. Feature definitions are effectively **append-only**: changing a window that already has consumers silently changes their model's inputs.

### Two tiers, and why the model never reads the shared table directly

- **`tier="shared"`** — a fact about the entity, no owner. Lives in `gold.customer_features`. Any model can use it.
- **`tier="model"`** — specific to one model. Lives in `gold.<owner>_features`.

The model always consumes its own `<owner>_features` table, which carries its features **plus the shared ones via a join** — never `customer_features` directly. That indirection is the point: when a model-specific feature becomes broadly useful, its definition moves to the shared tier and the model's table keeps exposing it through the join. The consumer changes nothing. So the tier is **metadata, not a folder hierarchy** that would force migrating a feature (and breaking its consumers) when its status changes.

The engine discovers model owners from the registry — a second model adds `features/<owner>.py` and gets its own table with the shared features inherited, without anyone touching the engine or the Terraform.

### The reuse promise is modest — and that's fine

"Define once, reuse everywhere" mostly doesn't happen; in practice ~20% of features are genuinely shared and 80% have a single consumer. Designing as if the reverse were true is how these projects die. The real failure mode isn't low reuse — it's that if adding a feature requires asking the platform team, data science just computes it in a notebook, and now there are two implementations and the skew the store existed to prevent.

So the platform optimizes for **making a new feature cheap**, not for forcing reuse. Reuse is discovered by observation, not decreed up front — which is why the tier isn't fixed in advance. A long tail of single-consumer features is the nature of the work, not technical debt.

### Model preprocessing does not belong in Gold

A semantic feature (`txn_count_24h = 5`) is a fact about the customer — reusable. Scaling that 5 to 0.73 because a neural net needs normalized inputs is a requirement of *one algorithm* — an XGBoost doesn't need it. Putting scaling, one-hot encoding or median imputation in Gold contaminates the feature for the next consumer, and fitting a scaler on training statistics inside the data layer is a classic leakage path. Those transforms live **with the model**.

---

## Temporal correctness

A feature may only use information that existed **before** the transaction it describes. Snapshot architectures get this for free — the future didn't exist when the job ran. A full-history recompute like this one gets it from the window bounds, which is why the platform owns them instead of leaving them to each definition.

A real case — a customer whose history sits around 159, hit by a fraudulent 1,409:

| "customer's average amount" | avg | std | **z-score of the fraud** |
|---|---|---|---|
| ❌ over all rows | 193 | 203 | **6.0** |
| ✅ prior rows only | 159 | 32 | **39.5** |

The 1,409 inflates its own standard deviation from 32 to 203 — the fraud camouflages itself by dividing by itself. And since production only ever has the prior rows, 39.5 *is* the correct value: a model trained on 6.0 was fed a distribution it will never see again. That's **training-serving skew**. A test confirms the frame holds — changing a transaction's future rows leaves its features unchanged to the decimal.

**Events and labels run on different clocks.** Events (amounts, countries, devices) are known instantly, so features use them freely looking backward. Labels arrive weeks later via chargeback. None of this project's 25 features read labels, so they're safe by construction; one that did — "customer's prior fraud count" — would need its own cutoff, since that value was 0 at scoring time even if it's 1 today.

## Label maturity — who is trainable

The chargeback lands 7–35 days after the swipe, so the engine takes an `--as_of` cutoff — the date it pretends "today" is — and asks whether each label was known by then:

```
Transaction 1 Feb, chargeback arrives 20 Feb:
  --as_of 2026-02-15  →  not yet known  →  is_trainable = false, is_fraud = NULL
  --as_of 2026-03-01  →  known          →  is_trainable = true
```

An unknown label is **not** filled with `is_fraud = 0` — that invents negatives and teaches the model that recent fraud is legitimate just because its chargeback hasn't arrived. The row is marked non-trainable and excluded.

`--as_of` also makes the training set reproducible in time: `AS_OF=2026-03-01` rebuilds exactly the dataset that existed on 1 March.

## How it's shipped — the build step

Unlike Bronze and Silver, Gold has a **build step**, and that step *is* the point. Feature definitions are a Python package handed over by data science, but a Glue job is a lone script. To let the job `import features`, `run_gold.sh` zips the package and uploads it, and Terraform points the job at it with `--extra-py-files`:

```
features/  ──zip──►  .build/features.zip  ──s3 cp──►  s3://…/features.zip
                                                            │  --extra-py-files
                                                            ▼
                                              Glue adds it to sys.path → import features
```

The zip is built in the runner, not Terraform, on purpose: packaging is a build concern, so data science can add files to `features/` without touching infrastructure. The `.tf` only declares the S3 path where the zip will be.

## How to run

```bash
python data_generator/generate.py --customers 2000 --days 60 --dirty-rate 0.001 --out ./output --seed 42

terraform -chdir=infra init
terraform -chdir=infra apply

./run_bronze.sh && ./run_silver.sh && ./run_gold.sh

# rebuild the feature set as it stood on a past date
AS_OF=2026-02-15 ./run_gold.sh
```

## Validation

```sql
-- the feature catalog (latest run only; the registry is an append-only log)
SELECT feature_name, tier, owner, window_seconds, description
FROM gold.feature_registry
WHERE materialized_at = (SELECT MAX(materialized_at) FROM gold.feature_registry)
ORDER BY tier, feature_name;
```

```sql
-- trainable split — the label-maturity concept, made visible
SELECT is_trainable, COUNT(*) AS n, SUM(CAST(is_fraud AS INT)) AS fraud
FROM gold.fraud_features GROUP BY is_trainable;
```

```sql
-- the table the model consumes: model features + shared features, joined
SELECT * FROM gold.fraud_features LIMIT 20;
```

With `--as_of now` against data that ends in March, every row is trainable (all chargeback windows have closed). An `--as_of` inside the data's lifetime splits the result into trainable and not-yet-known.

---

## What I learned

**A "faithful mirror" fact feed and a snapshot-replaced dimension will break referential integrity across generations** — this bit in Part 2 and shaped Part 3's join design: features join dimensions that Silver already guaranteed exist, so the joins can't silently drop rows.

**The registry catalogs *declaration*, not *consumption*.** The `owner` field says who defined a feature, not who reads it. The engine can't know consumption — it materializes and never sees who queries later. That lineage comes from the training side (Part 4), where each run logs the features it used; crossing the two answers "which features are actually shared?" — the question that justifies the tier at all.

**Building expressions in a loop is free; the frame is what has to be right.** The engine loops over 25 definitions to assemble columns, all evaluated in one pass, with features sharing an (entity, window) reusing the same shuffle. The care isn't in the loop — it's in the window bounds the platform sets once, so no individual feature can get them wrong.

---

## Limitations & scaling

Like Silver, this is a **full refresh** — every run recomputes all features over all history. At ~140k rows it's seconds. At terabytes the window operations (`partitionBy` + `orderBy`) shuffle the entire dataset each run.

How it would be fixed: incremental materialization per entity, computing windows only for entities the increment touched and pulling prior state from the already-materialized table — cost proportional to new data, not to history. This is also where an **online store** enters (Part 5): short-window features like "5 transactions in 5 minutes" can't be served from a daily batch snapshot, so the classic split is batch snapshots for slow features plus streaming compute for fast ones — the offline/online pair a feature store exists to provide.