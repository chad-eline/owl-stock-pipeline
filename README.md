# OWL Stock Data Pipeline

A Python pipeline that loads denormalized daily stock data into a normalized SQLite
database and runs an analytics query over the result. It is idempotent: running it
again on the same file changes nothing, and running it on a revised file brings the
database back into sync without duplicating or corrupting rows.

Writes are upserts keyed on each row's business identity, so a reload updates rows in
place instead of appending. Pointed at the revised source, the same command adds the
new `mktcap_usd` column, backfills it, and restates Apple's history for a 2-for-1 split.

## Quick start

Requires Python 3.12+ and [uv](https://docs.astral.sh/uv/).

```bash
uv sync                                                   # install from uv.lock

# build the normalized store
uv run pipeline.py --file data/stock-data-se-owl.xlsx

# inspect: cumulative return per company (DuckDB over the SQLite file)
uv run queries.py

# re-run: nothing changes, since the file hash already matches
uv run pipeline.py --file data/stock-data-se-owl.xlsx

# load the revised source: migrate, backfill mktcap_usd, restate the split
uv run pipeline.py --file data/stock-data-se-owl-part2.xlsx

# tests
uv run pytest -q
```

CLI flags: `--file` (required), `--db` (default `owl.db`), `--schema` (default `./schema.sql`).

The sources were provided as `.xlsx`. `read_file` picks a reader by extension and also
handles `.csv` (the format the brief names), so either input works.

## Architecture

Three tools, each doing what it is good at:

| Stage | Tool | Why |
| --- | --- | --- |
| Ingest + clean | Polars | Fast column operations; reads the strict-OOXML xlsx directly via fastexcel |
| System of record | SQLite | Transactional store for idempotent upserts |
| Analytics read | DuckDB | Columnar engine for the join-and-aggregate query, reading the SQLite file in place |

This separates the write path (correct, transactional, idempotent updates) from the read
path (analytical scans). At 17,983 rows a single engine would be enough; the split is here
to show the shape that matters at scale, not because this volume needs it.

## Data model

The source is denormalized: one wide row per company-day, with the sector labels repeated
on every row. It is normalized into three tables plus a load-tracking table.

```sql
sectors    (sector_id PK, sector_level1, sector_level2, UNIQUE(level1, level2))
companies  (company_id PK, name UNIQUE, sector_id FK -> sectors)
prices     (company_id FK -> companies, asof, volume, close_usd, mktcap_usd,
            PRIMARY KEY (company_id, asof))   -- mktcap_usd added by the v2 migration
load_runs  (run_id PK, loaded_at, source_path, source_sha256,
            source_columns, rows_read, rows_upserted)
```

A few decisions worth calling out:

- **The price key is `(company_id, asof)`, not the source `#` column.** `#` is row order in
  the file, not a stable identifier, so keying on it would make every reload append
  duplicates. A price row is identified by which company and which date, so that pair is the
  primary key, and it is what makes the upsert idempotent.
- **Sector lives in its own table.** Sector is determined by the company (two level-1 and
  three level-2 values across four companies), so it does not need to repeat on every price row.
- **`asof` is stored as ISO `YYYY-MM-DD` text.** ISO dates sort correctly as strings, so range
  and ordering queries work without a date type, and the value round-trips cleanly across
  Polars, SQLite, and DuckDB.
- **Money is stored as `REAL`.** SQLite has no decimal type, and floats are fine at this scale.
  A production system of record for money would use fixed-precision (or integer minor units)
  to avoid rounding drift on `close_usd` and `mktcap_usd`.
- **`load_runs` records each applied load:** source path, file hash, the header that was read,
  and row counts. It drives the idempotency check (below) and doubles as an audit trail of what
  was loaded and when, including when `mktcap_usd` first appears in the header.

## Idempotency

Re-running never duplicates or corrupts data, for two reasons:

1. **Upsert on the natural key.** Every write is `INSERT ... ON CONFLICT(...) DO UPDATE` on the
   table's business key: `(company_id, asof)` for prices, `name` for companies,
   `(sector_level1, sector_level2)` for sectors. A reload updates in place.
2. **File-hash short-circuit.** Before loading, the pipeline hashes the source file (SHA-256)
   and checks `load_runs`. If that exact file has already been applied, the run is a no-op. This
   skips redundant work and keeps `load_runs` honest about what actually changed the data.

### Atomic schema and load

The schema step and the data load run in one transaction (`con.autocommit = False`, Python
3.12+). If any step fails, the run rolls back to its prior state, so a failure cannot leave the
schema half-applied or the data half-loaded. `PRAGMA foreign_keys = ON` is set before the
transaction opens so referential integrity is enforced throughout. `test_schema_and_load_are_atomic`
confirms this: an induced mid-run failure leaves no tables behind.

## Schema evolution and the v2 migration

The revised source does two things, handled in one code path:

1. **Adds `mktcap_usd`.** The fact measures the pipeline knows about live in one place,
   `PRICE_MEASURES` (column -> SQL type). On each run, `reconcile_prices` compares that list to the
   live `prices` columns and runs `ALTER TABLE prices ADD COLUMN ...` for any measure the source
   has that the table does not. The ALTER is guarded by a `PRAGMA table_info` check (SQLite has no
   `ADD COLUMN IF NOT EXISTS`), so it is idempotent. The same list drives the upsert, so the column
   is backfilled for every row on the v2 load. `schema.sql` stays the v1 baseline, and adding a
   future measure is a one-line change to `PRICE_MEASURES`.
2. **Restates Apple's price history** for a 2-for-1 split (close halved, volume doubled on every
   Apple row). Because writes upsert on `(company_id, asof)`, the restated rows overwrite the
   existing ones in place: no duplicates, the row count stays 17,983, and the other companies are
   untouched.

### How the change propagates

The thing that depended on the old shape is the cumulative-return query, which reads the close
prices the split changed. Two things keep it correct:

- **Restate in place.** Apple's full history is overwritten on `(company_id, asof)` rather than
  appended as a new series or a split event. Every downstream read sees the corrected values, and
  cumulative return stays consistent with no spurious 50% drop on the split date. Because the whole
  series is halved, the first/last ratio is unchanged.
- **Compute on read.** The query layer runs on demand, so there are no materialized aggregates to go
  stale; the restatement shows up for free, and `load_runs` records which file changed the data.

Not materializing downstream tables is the right call at this size, since there is nothing to
recompute. At scale you would materialize (for example, dbt marts), and a restatement would trigger
an incremental recompute of the affected `(company_id, asof)` partitions. For corporate actions
specifically, the more durable design keeps a raw, unadjusted close plus an adjustment-factor table
and derives split-adjusted prices at read time, which preserves unadjusted history and extends to
dividends. For this assignment, restating in place is the simpler correct answer.

## Input validation

`reconcile_prices` checks the source header against the same contract before anything is written.
A missing required column, or a column the contract does not know about, stops the run rather than
loading a partial row and silently dropping data. Because validation, migration, and the upsert
column list all come from one place, the set of allowed columns cannot drift from the set actually
written: the known evolution (`mktcap_usd`) is accepted, and any other new column fails fast. A
stopped run writes no `load_runs` row, so it can be fixed and retried cleanly.

## Sync semantics

Re-running on an updated source keeps the database in sync by insert-and-update (upsert on the
natural key). Rows that disappear from the source are not deleted, which is a scope choice for
append-only price history where past bars do not change. A full mirror including deletes would use
a merge with tombstones or soft-deletes; that is noted as the at-scale path rather than built here.

## Data quality notes

Handled on purpose, and called out rather than hidden:

- **`#` dropped.** Row order, not a key (see Data model).
- **`Facebook Class A` has a trailing space** in the source. Name values are trimmed on load, so it
  resolves to a single `companies` row.
- **Zero-volume rows are kept**, not dropped. They are plausible (trading halts, holidays), and
  dropping them would quietly distort history.
- **Dedupe on `(name, asof)`**, keeping the last occurrence, in case the source repeats a company-day.

## Example queries

`queries.py` attaches the SQLite file from DuckDB and runs the required query: cumulative return per
company over its full history, as a join across `prices` and `companies` with a grouped aggregation
(plus a per-sector variant). After the v2 load it also reports the latest `mktcap_usd` per company,
showing the new column flowing through. It prints a small sample of each table as a sanity check.

## Tests

`tests/` runs the real pipeline functions against a throwaway SQLite database in pytest's `tmp_path`,
using the committed source files as fixtures:

- `clean_file` drops `#`, normalizes `asof` to ISO text (from either a date or a string), trims
  names, and dedupes.
- `read_file` raises on a missing file and on an unsupported extension.
- `reconcile_prices` accepts the contract's `mktcap_usd`, adds it via the migration, and fails fast
  on any column outside the contract.
- A v1 load produces the expected table counts, leaves no orphan foreign keys, and preserves Apple's
  close and volume.
- A second v1 load is a no-op (counts unchanged, no new `load_runs` row).
- Schema and load are atomic: an induced mid-run failure rolls back both.
- The v2 load migrates and backfills `mktcap_usd`, restates Apple's split in place (close 88.325,
  volume 159,658,500), and keeps the row count at 17,983.

## Project layout

```text
pipeline.py        CLI: read + clean (Polars) -> ensure schema -> reconcile (validate + migrate) -> upsert (SQLite)
schema.sql         DDL for sectors / companies / prices / load_runs (v1 baseline)
queries.py         DuckDB analytics queries over the SQLite file
tests/             pytest suite (reconcile, idempotency, FK integrity, atomicity, v2 migration)
data/              committed source files
pyproject.toml     dependencies (Polars, DuckDB, fastexcel, pyarrow; pytest dev)
uv.lock            pinned versions for reproducibility
```

## At scale

What this design points toward if the four companies became thousands and the daily file became a
continuous feed:

- **A warehouse instead of SQLite** (Snowflake, BigQuery, or Postgres), with the same idempotent
  `MERGE` on the natural key.
- **Incremental and CDC loads** instead of full-file reloads, with the fact partitioned by date and
  the file hash replaced by per-batch watermarks.
- **A transform layer in dbt:** staging models, conformed dimensions, and an incremental fact with
  `unique_key=(company_id, asof)` and an `on_schema_change` policy, plus dbt tests (`unique`,
  `not_null`, relationships) as enforced data quality.
- **Schema governance.** Dynamic ingestion belongs in a bronze landing layer that captures whatever
  arrives, with a schema registry or contract defining the allowed evolution. The modeled fact is
  then promoted by a reviewed transform, keeping the capture-everything boundary separate from the
  contracted one rather than letting the fact table auto-evolve.
- **Corporate actions as first-class data:** a raw close plus an adjustment-factor or dividend table,
  with split-adjusted prices derived at read time, so history stays reproducible across splits and
  dividends.
- **Operational concerns:** orchestration (Airflow or Dagster), audit columns (`loaded_at`,
  `source_version`), slowly changing dimensions for company and sector attribute changes, and
  alerting on data-quality failures.
