CREATE TABLE IF NOT EXISTS sectors (
  sector_id INTEGER PRIMARY KEY,
  sector_level1 TEXT NOT NULL,
  sector_level2 TEXT NOT NULL,
  UNIQUE (sector_level1, sector_level2)
);
CREATE TABLE IF NOT EXISTS companies (
  company_id INTEGER PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  sector_id INTEGER NOT NULL REFERENCES sectors(sector_id)
);
CREATE TABLE IF NOT EXISTS prices (
  company_id INTEGER NOT NULL REFERENCES companies(company_id),
  asof TEXT NOT NULL, -- ISO YYYY-MM-DD (sorts correctly)
  volume INTEGER,
  close_usd REAL,
  PRIMARY KEY (company_id, asof)
);

-- lightweight load tracking / lineage
CREATE TABLE IF NOT EXISTS load_runs (
  run_id         INTEGER PRIMARY KEY,
  loaded_at      TEXT    NOT NULL DEFAULT (datetime('now')),
  source_path    TEXT    NOT NULL,
  source_sha256  TEXT    NOT NULL,
  source_columns TEXT    NOT NULL,
  rows_read      INTEGER NOT NULL,
  rows_upserted  INTEGER NOT NULL
);