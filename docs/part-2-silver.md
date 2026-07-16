# Part 2 — Silver: data quality, quarantine & profiling

*[← Back to overview](../README.md) · [Part 1 — Bronze](part-1-bronze.md) · Next: Part 3 — Features*

Bronze is a faithful mirror of what arrived. Silver is where data becomes **trustworthy** — and where anything that isn't stays visible instead of vanishing.

```
bronze.transactions ──►  row validation  ──►  referential integrity  ──►  dedup  ──►  silver.transactions
                                │                      │                    │
                                └──────────────────────┴────────────────────┘
                                                       ▼
                                              silver.quarantine
                                         (every reject, with its reason)
```

**What ships in this part**

- A Glue PySpark job that validates, quarantines, deduplicates and profiles
- **Declarative quality rules in YAML** — versioned in the repo, read at runtime
- Dirty-data injection in the generator, to prove the quality layer actually catches things
- `run_silver.sh` — one command to deploy, run, and reconcile

**Silver tables**

| Table | Contents | Load |
|---|---|---|
| `silver.transactions` | valid, deduplicated, joined to fraud labels | replace |
| `silver.quarantine` | every rejected row, with `reject_reason` | replace |
| `silver.data_profile` | per-column stats, one snapshot per run | append |
| `silver.quality_checks` | per-check verdict with observed value | append |

---

## Design decisions

### Quarantine, not filtering

The easy move is to drop rows that fail validation. It's less code — and it's a silent leak. If Silver has 137,400 rows and Bronze had 137,882, nobody can say what the missing 482 were or why they went. Worse: if a rule is *wrong* and discards good data, you never find out.

So rejected rows aren't dropped, they're **routed** — to `silver.quarantine`, tagged with the reason they failed. That buys an invariant:

```
count(silver.transactions) + count(silver.quarantine) == count(bronze.transactions)
```

This isn't documentation — it's an `assert` in the job. A row that disappears without a trace is a bug, and the job fails loudly rather than quietly losing data.

### Policy in config, mechanics in code

Quality rules live in `config/quality_rules.yaml`, not in PySpark. Row validations are declared as **Spark SQL expressions**:

```yaml
- name: amount_non_negative
  expr: "amount >= 0"
  reject_reason: "monto negativo"
```

Adding a rule means adding four lines of YAML. Changing a threshold means editing a file, not redeploying a job. The config is versioned in Git — so there's a history of who changed which threshold and when, which is exactly what an audit would ask for in a banking context.

YAML over JSON specifically because rules need **comments**. A threshold without its rationale is a magic number:

```yaml
- name: quarantine_rate_acceptable
  severity: critical
  metric: quarantine_rate
  max: 0.05
  description: ">5% del lote rechazado indica problema sistemico, no ruido"
```

### One row, one reason — first rule wins

Each rejected row carries exactly **one** `reject_reason`: the first rule it fails, in config order. That's what makes rejects reconcile 1:1 against what went in.

It's implemented as a `coalesce` over a list of `when` expressions — each returns its reason if the rule fails, `NULL` if it passes; `coalesce` takes the first non-null. One flat expression, evaluated in a single pass, regardless of how many rules there are.

Order matters, and the config encodes it: `amount_present` sits before `amount_non_negative`, so a null amount is rejected as *"monto nulo"* rather than *"monto negativo"*. A rule evaluating to `NULL` counts as a failure, so null checks come first by design.

### Two levels of quality: per-row and per-batch

Row validation answers *"is this row valid?"*. It can't answer *"does this batch look right?"* — a feed that suddenly arrives 90% from one country has valid rows and a broken shape.

So there's a second layer of **batch checks** on distribution: fraud rate in range, channel mix not skewed, mean amount plausible. All verdicts land in `silver.quality_checks` with their observed value, pass or fail — history accumulates even when nothing breaks.

### Mixed severity

Batch checks aren't equal, so they don't fail equally:

- **`critical`** → *the pipeline or the source is broken*. The job aborts and Silver is not published. Non-negotiable: empty Bronze, >5% quarantined, >50% nulls in a key column.
- **`warn`** → *the data changed, possibly for a legitimate reason*. Recorded and the job continues: fraud rate drift, channel mix, mean amount.

The dividing line is "broken" vs "different". Treating both the same means either blocking on noise or shipping on garbage.

Critically, **the evidence is written before the abort**. A critical failure persists to `quality_checks` and *then* raises. A job that fails without recording why leaves you blind exactly when you need to see.

### Anomalies are not cleaned here

A generic pipeline might flag and scrub extreme values. In fraud detection that would be a serious mistake: **the outlier is the signal**. An absurd amount, a purchase abroad minutes after a local one — those are precisely what the model must learn to catch. Filtering them in Silver would delete the fraud before ever trying to detect it.

So anomalies aren't cleaned. They're **measured** in the feature layer (Part 3), as deviation from the customer's historical pattern, and become model input.

### Proving it, not claiming it

A quarantine table nobody ever fills is a claim. So the generator grows a `--dirty-rate` flag that injects defects covering **every** rule — one defect per row, so counts reconcile exactly:

```bash
python data_generator/data_generator.py --customers 2000 --days 60 --dirty-rate 0.001 --out ./output
```

It prints a manifest of what it injected. That manifest is the expectation; `silver.quarantine` is the result.

---

## Results

**138 defects injected, 138 caught, every one classified correctly:**

| Reject reason | Injected | In quarantine |
|---|---|---|
| llave ausente | 14 | ✅ 14 |
| event_timestamp nulo | 14 | ✅ 14 |
| timestamp futuro | 14 | ✅ 14 |
| monto nulo | 14 | ✅ 14 |
| monto negativo | 14 | ✅ 14 |
| customer_id nulo | 14 | ✅ 14 |
| merchant_id nulo | 14 | ✅ 14 |
| cliente inexistente | 14 | ✅ 14 |
| comercio inexistente | 13 | ✅ 13 |
| duplicado (version antigua) | 13 | ✅ 13 |

**Every batch check traces back to a generator parameter** — the system measuring itself end to end:

| Check | Observed | Where it comes from |
|---|---|---|
| `bronze_not_empty` | 138,020 | 137,882 clean + 138 injected |
| `quarantine_rate` | 0.09999% | 138 / 138,020 — the `--dirty-rate 0.001` measured back |
| `key_columns_not_mostly_null` | 0.0101% | 14 / 138,020 — the nulls in one key column |
| `fraud_rate` | 0.247% | 341 / 137,882 |
| `channel_mix` | 0.548 | generator weights channels `[0.55, 0.35, 0.10]` |
| `country_mix` | 0.550 | generator weights `MX` at 0.55 |
| `amount_mean` | 119.91 | per-customer mean drawn from `uniform(15, 220)` |

Note `bronze_not_empty` reads 138,020 — not 137,882. Bronze loaded the defective rows too, because Bronze mirrors the source and doesn't judge. Silver is where they get held back. That separation is the medallion architecture doing its job.

**And the hard fail works.** Re-running at `--dirty-rate 0.06` pushes the quarantine rate to 5.66%, past the 5% critical threshold:

```
[fail]  job FAILED - RuntimeError: 1 chequeo(s) critico(s) fallaron.
[warn]  Parece un chequeo de calidad CRITICO, no un error del pipeline.
```

Silver was **not published**. `quality_checks` recorded the failure with `observed_value = 0.0566` against `max_expected = 0.05`. The runner distinguishes an expected quality failure from a pipeline bug — so nobody burns an afternoon debugging a system that's working correctly.

---

## How to run

```bash
python data_generator/data_generator.py --customers 2000 --days 60 \
       --dirty-rate 0.001 --out ./output --seed 42

terraform -chdir=infra init
terraform -chdir=infra apply

./run_bronze.sh
./run_silver.sh
```

## Validation

```sql
-- reconciliation: should match the generator's manifest exactly
SELECT reject_reason, COUNT(*) AS n
FROM silver.quarantine GROUP BY reject_reason ORDER BY n DESC;
```

```sql
-- the invariant, visible
SELECT (SELECT COUNT(*) FROM silver.transactions) AS valid,
       (SELECT COUNT(*) FROM silver.quarantine)   AS quarantined,
       (SELECT COUNT(*) FROM bronze.transactions) AS bronze;
```

```sql
-- batch check verdicts with observed values
SELECT check_name, severity, observed_value, min_expected, max_expected, passed
FROM silver.quality_checks ORDER BY severity, check_name;
```

```sql
-- column profile
SELECT column_name, null_count, null_rate, distinct_count, mean_value
FROM silver.data_profile ORDER BY null_rate DESC;
```

---

## What I learned

### The quality layer found a bug before I did

Testing the hard fail, the critical check fired — but at **51.5% quarantined** instead of the expected 6%. Investigating that gap surfaced a bug that had been silently present since day one.

`--seed 42` was cosmetic. IDs came from `uuid.uuid4()`, which draws from the OS entropy pool and **ignores the numpy seed** — so every generator run produced entirely new `transaction_id`s and `customer_id`s. The reproducibility the README advertised didn't exist.

That collided with Bronze's load pattern:

- `bronze.transactions` loads **insert-only**. New UUIDs didn't collide with old ones, so regenerating **added** a second generation instead of replacing the first: 138,020 → 284,175 rows.
- `bronze.customers` loads as a **full snapshot** — replaced entirely, now holding only generation 2's customers.

Every transaction from generation 1 was suddenly an orphan. Silver did exactly what it was told and quarantined all 138,020 of them. The math checked out to sixteen decimal places: `(138,020 + 8,273) / 284,175 = 0.5147989795020674` — the exact observed value.

The fix: derive IDs from the seeded RNG, so the same seed produces the same IDs and Bronze's `MERGE` recognizes existing rows. Regenerating now leaves **zero** orphans.

Two lessons, one shallow and one deep. Shallow: a seed only seeds what actually asks it for numbers. Deep: **an insert-only fact feed against snapshot-replaced dimensions will break referential integrity** — a real design tension in medallion architectures, not a Python detail.

### Building expressions is free; running actions is not

Looping over rules to build a `coalesce` costs nothing — it runs once on the driver, assembling a plan. Seven rules or seventy still evaluate in one pass.

Looping over columns to run `.collect()` is a different animal. The first profiler did one aggregation per column: 11 columns, 11 Spark jobs, 11 full reads of the dataset. Rewritten to build every expression first and execute a single `agg()`: one job, one read.

The related subtlety: `collect()` isn't inherently dangerous. `df.collect()` pulls every row to the driver and will OOM. `df.agg(...).collect()` reduces to one row **in the cluster** before returning — safe at any volume. The cost of doing it in a loop is repeated I/O, not driver memory.

So *this* problem is fixed by aggregating once, not by materializing to S3 — there'd be nothing to write but a single row. But materializing **is** the right fix for a neighbouring problem: when a long lineage gets recomputed across many actions and `cache()` can't hold it, writing an intermediate table truncates the lineage and turns recomputation into a read. This project does exactly that in the Bronze `MERGE`, where the staging table both freezes the non-deterministic columns and cuts the recompute. The lesson isn't "never write to S3" — it's to know which of the two problems you have: too many passes over a short lineage, or a long lineage recomputed repeatedly.

---

## Limitations & scaling

**This job is a full refresh, not incremental.** Every run reads all of Bronze and rewrites all of Silver. At this project's scale (~140k rows / ~6 MB) that costs seconds, and the code reads without the complexity of incremental bookkeeping. It was a deliberate trade.

At terabytes per day it does not survive. Where it breaks, in order:

1. `spark.table(bronze)` with no filter re-reads all history every run, even when only a few GB arrived. This is the real ceiling.
2. `createOrReplace()` drops and rewrites the entire Silver dataset.
3. `cache()` no longer fits in memory — it spills to disk and thrashes.
4. The dedup window shuffles the full dataset every run.

**How it would be fixed** — incremental processing:

- Read only what's new via Iceberg's **incremental read**: the snapshots after the last processed one. Here time travel stops being a curiosity and becomes the core mechanism.
- Keep a **watermark table** recording "processed through snapshot X", so re-execution stays idempotent.
- Publish Silver with `MERGE` instead of `createOrReplace` — only affected partitions get touched.
- Dedup in two stages: within the increment (small shuffle) plus an anti-join against already-published keys.
- Profile and check the increment, not the full history.

The effect: cost stops scaling with **history** and starts scaling with **new data** — the only way a daily pipeline survives for years.

**Config in a table** is the natural next step for the rules: it would let a risk analyst edit thresholds without touching Git. Planned as a closing bonus for the project.