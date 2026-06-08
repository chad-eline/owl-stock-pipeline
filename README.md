# OWL Stock Data Pipeline

An idempotent Python pipeline that loads denormalized daily stock data into a
normalized relational store without duplicating or corrupting data, and exposes a
small analytics query over the result.

Running it twice on the same file is a no-op: writes are upserts keyed on each row's
business identity, so a reload updates rows in place rather than appending duplicates.

## Quick start

Requires Python 3.12+ and [uv](https://docs.astral.sh/uv/).

```bash
uv sync                                                   # install from uv.lock

# build the normalized store
uv run pipeline.py --file data/stock-data-se-owl.xlsx

# inspect: cumulative return per company (DuckDB over the SQLite file)
uv run queries.py

# re-run: idempotent, counts unchanged (hash no-op)
uv run pipeline.py --file data/stock-data-se-owl.xlsx

# tests
uv run pytest -q
```

CLI flags: `--file` (required), `--db` (default `owl.db`), `--schema` (default `./schema.sql`).

The source was provided as `.xlsx`; `read_file` dispatches on extension and also reads
`.csv` (the format named in the brief), so either input works.

## Architecture

Three tools, each used for what it is best at:

| Stage | Tool | Why |
| --- | --- | --- |
| Ingest + clean | Polars | Fast, expressive column ops; reads the strict-OOXML xlsx directly via fastexcel |
| System of record | SQLite | Transactional store for idempotent upserts |
| Analytics read | DuckDB | Columnar engine for the join-plus-aggregation query, reading the SQLite file in place |

This is a deliberate OLTP-write, OLAP-read split: the writer optimizes for correct,
transactional, idempotent updates, and the reader optimizes for analytical scans.
Honest caveat: at 17,983 rows a single engine would suffice. The split is here to show
the shape that matters at scale, not because this volume needs it.

## Data model

The source arrives denormalized (one wide row per company-day, with sector labels
repeated on every row). It is normalized into three tables plus a lineage table.

```sql
sectors    (sector_id PK, sector_level1, sector_level2, UNIQUE(level1, level2))
companies  (company_id PK, name UNIQUE, sector_id FK -> sectors)
prices     (company_id FK -> companies, asof, volume, close_usd,
            PRIMARY KEY (company_id, asof))
load_runs  (run_id PK, loaded_at, source_path, source_sha256,
            source_columns, rows_read, rows_upserted)
```

Key decisions:

- **The fact key is `(company_id, asof)`, not the source `#` column.** `#` is source
  row order, not a stable business key. Keying on it would make every reload append
  duplicates. The business identity of a price row is the company and the date, so that
  is the primary key, and it is what makes the upsert idempotent.
- **Sector is normalized out.** Sector is functionally determined by the company (two
  level-1 and three level-2 values across four companies), so it lives in its own table
  rather than being repeated on every price row.
- **`asof` is stored as ISO `YYYY-MM-DD` text.** ISO dates sort lexically, so range and
  ordering queries are correct without a date type, and the value round-trips cleanly
  across Polars, SQLite, and DuckDB.
- **`load_runs` records lineage for every applied load:** source path, file hash,
  detected header, and row counts. It is both the idempotency signal (see below) and a
  visible audit trail of what was loaded and when.

## Idempotency

Re-running the pipeline never duplicates or corrupts data. Two layers enforce this:

1. **Upsert on the natural key.** Every write is `INSERT ... ON CONFLICT(...) DO UPDATE`
   keyed on the table's business key (`(company_id, asof)` for prices, `name` for
   companies, `(sector_level1, sector_level2)` for sectors). A reload updates rows in
   place rather than appending.
2. **Source hash no-op.** Before loading, the pipeline hashes the source file
   (SHA-256) and checks `load_runs`. If that exact file has already been applied, the
   run is a no-op and records nothing. This skips redundant work and keeps the lineage
   table honest about what actually changed the data.

### Atomic schema and load

On Python 3.12+, `con.autocommit = False` wraps the schema step (DDL) and the data load
in a single transaction. If any step fails, the whole run rolls back to its prior state,
so a failure can never leave the schema half-applied or the data half-loaded.
`PRAGMA foreign_keys = ON` is set before entering manual-commit mode so referential
integrity is enforced throughout.

## Sync semantics

Re-running on an updated source keeps the database in sync by **insert and update**
(upsert on the natural key). Rows that disappear from the source are not deleted - a
deliberate scope choice for append-only price history, where past bars are immutable.
A full mirror including deletes would use a merge with tombstones or soft-deletes; that
is noted as the at-scale path rather than built here.

## Data quality notes

Handled deliberately, and called out rather than hidden:

- **`#` dropped.** Source row order, not a key (see Data model).
- **`Facebook Class A` arrives with a trailing space** in the source. Name values are
  trimmed on load, so the company resolves to a single `companies` row.
- **Zero-volume rows are kept**, not dropped. They are plausible (trading halts,
  holidays) and removing them would silently distort history.
- **Defensive dedupe** on `(name, asof)`, keeping the last occurrence, guards against a
  source that repeats a company-day.

## Example queries

`queries.py` attaches the SQLite file from DuckDB and runs the required analytics query:
cumulative return per company over its full history, as a join across `prices` and
`companies` plus a grouped aggregation. It also prints small samples of each table for a
quick sanity check after a load.

## Tests

`tests/` drives the real pipeline functions against a throwaway SQLite database in
pytest's `tmp_path`, using the committed source files as fixtures:

- `clean_file` drops `#`, casts `asof` to ISO text, trims names, and dedupes.
- `read_file` raises on a missing file and on an unsupported extension.
- A v1 load produces the expected table counts, leaves no orphan foreign keys, and
  preserves Apple's close and volume.
- A second v1 load is a no-op (counts unchanged, no new `load_runs` row).

## Project layout

```text
pipeline.py        CLI: read + clean (Polars) -> ensure schema -> upsert (SQLite)
schema.sql         DDL for sectors / companies / prices / load_runs
queries.py         DuckDB analytics query over the SQLite file
tests/             pytest suite (idempotency, FK integrity, v1 values)
data/              committed source files
pyproject.toml     dependencies (Polars, DuckDB, fastexcel, pyarrow; pytest dev)
uv.lock            pinned versions for reproducibility
```

## At scale

What this design points at if the four companies became thousands and the daily file
became a continuous feed:

- **Warehouse, not SQLite.** Snowflake, BigQuery, or Postgres as the store, with the
  same idempotent `MERGE` on the natural key.
- **Incremental and CDC loads** instead of full-file reloads, with the fact partitioned
  by date and the source hash replaced by per-batch watermarks.
- **A transform layer in dbt:** staging models, conformed dimensions, and an incremental
  fact with `unique_key=(company_id, asof)` and `on_schema_change` policy, plus dbt
  tests (`unique`, `not_null`, relationship tests) as enforced data quality.
- **Schema governance.** Dynamic ingestion belongs in a bronze landing layer that
  captures whatever arrives faithfully, with a schema registry or contract defining the
  allowed evolution. The modeled fact is then promoted by a reviewed transform. That
  keeps the dynamic, capture-everything boundary separate from the contracted,
  decisions-made-by-humans boundary, rather than letting the fact table auto-evolve.
- **Corporate actions as first-class data:** a raw close plus an adjustment-factor or
  dividend table, with split-adjusted prices derived at read time, so history stays
  reproducible across splits and dividends.
- **Operational concerns:** orchestration (Airflow or Dagster), audit columns
  (`loaded_at`, `source_version`), slowly changing dimensions for company and sector
  attribute changes, and alerting on data-quality failures.
