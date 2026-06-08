"""
Tests for the OWL stock-data pipeline.

Strategy: drive the real functions (no argparse) against a throwaway SQLite db in
pytest's tmp_path, using the committed data files as fixtures.
"""

import datetime as dt
import sqlite3
import sys
from pathlib import Path

import polars as pl
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import pipeline  # noqa: E402

V1 = ROOT / "data" / "stock-data-se-owl.xlsx"
V2 = ROOT / "data" / "stock-data-se-owl-part2.xlsx"
SCHEMA = ROOT / "schema.sql"


def run(db_path, src):
    """Run one pipeline pass the way main() does, minus argparse."""
    con = sqlite3.connect(db_path)
    con.execute("PRAGMA foreign_keys = ON")  # must be set before the txn opens
    con.autocommit = False  # mirror main(): schema + load in one transaction
    try:
        pipeline.ensure_schema(con, str(SCHEMA))
        df = pipeline.clean_file(pipeline.read_file(str(src)))
        measures = pipeline.reconcile_prices(con, df)
        pipeline.load(con, df, measures, str(src))
        con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()


def query(db_path, sql, params=()):
    con = sqlite3.connect(db_path)
    try:
        return con.execute(sql, params).fetchall()
    finally:
        con.close()


def count(db_path, table):
    assert table in {
        "sectors",
        "companies",
        "prices",
        "load_runs",
    }  # guard the f-string
    return query(db_path, f"SELECT count(*) FROM {table}")[0][0]


@pytest.fixture
def db(tmp_path):
    return str(tmp_path / "test.db")


# --------------------------------------------------------------------------- #
# Unit tests: clean_file / read_file (no db needed)
# --------------------------------------------------------------------------- #


def test_clean_file_drops_hash_strips_and_dedupes():
    raw = pl.DataFrame(
        {
            "#": [1, 2, 3],
            "name": ["Apple", "Apple ", "Apple"],  # row 2 has a trailing space
            "asof": [dt.date(2024, 1, 1), dt.date(2024, 1, 1), dt.date(2024, 1, 2)],
            "volume": [10, 20, 30],
            "close_usd": [1.0, 2.0, 3.0],
            "sector_level1": ["Technology", "Technology", "Technology"],
            "sector_level2": ["Hardware", "Hardware", "Hardware"],
        }
    )
    cleaned = pipeline.clean_file(raw)

    assert "#" not in cleaned.columns  # surrogate dropped
    assert cleaned.schema["asof"] == pl.String  # date cast to ISO text
    assert set(cleaned["name"].to_list()) == {"Apple"}  # trailing space stripped
    # rows 1 and 2 collapse to the same (name, asof) -> one row; row 3 distinct
    assert cleaned.height == 2


def test_clean_file_parses_string_asof():
    # csv inputs arrive with asof as text rather than a date dtype; it must
    # still normalize to ISO YYYY-MM-DD strings.
    raw = pl.DataFrame(
        {
            "#": [1],
            "name": ["Apple"],
            "asof": ["2023-11-03"],
            "volume": [100],
            "close_usd": [176.65],
            "sector_level1": ["Technology"],
            "sector_level2": ["Technology Equipment"],
        }
    )
    cleaned = pipeline.clean_file(raw)
    assert cleaned.schema["asof"] == pl.String
    assert cleaned["asof"][0] == "2023-11-03"


def test_read_file_missing(tmp_path):
    with pytest.raises(FileNotFoundError):
        pipeline.read_file(str(tmp_path / "nope.xlsx"))


def test_read_file_unsupported_extension(tmp_path):
    p = tmp_path / "data.txt"
    p.write_text("not a supported format")
    with pytest.raises(ValueError):
        pipeline.read_file(str(p))


# --------------------------------------------------------------------------- #
# reconcile_prices: contract validation + migration
# --------------------------------------------------------------------------- #

CORE = {"name", "asof", "volume", "close_usd", "sector_level1", "sector_level2"}


def _schema_con(db_path):
    con = sqlite3.connect(db_path)
    con.execute("PRAGMA foreign_keys = ON")
    pipeline.ensure_schema(con, str(SCHEMA))
    return con


def test_reconcile_v1_shape_returns_baseline_measures(db):
    con = _schema_con(db)
    measures = pipeline.reconcile_prices(con, pl.DataFrame({c: [1] for c in CORE}))
    con.close()
    assert set(measures) == {"volume", "close_usd"}  # mktcap not present in v1


def test_reconcile_adds_and_returns_mktcap(db):
    con = _schema_con(db)
    df = pl.DataFrame({c: [1] for c in CORE | {"mktcap_usd"}})
    measures = pipeline.reconcile_prices(con, df)
    cols = {r[1] for r in con.execute("PRAGMA table_info(prices)")}
    con.close()
    assert "mktcap_usd" in measures  # drives the upsert
    assert "mktcap_usd" in cols  # migration added the column


def test_reconcile_raises_on_missing_required(db):
    con = _schema_con(db)
    with pytest.raises(ValueError, match="required"):
        pipeline.reconcile_prices(con, pl.DataFrame({"name": [1], "asof": [1]}))
    con.close()


def test_reconcile_raises_on_unknown_column(db):
    # mktcap is in the contract, but a genuinely new column still fails fast
    con = _schema_con(db)
    df = pl.DataFrame({c: [1] for c in CORE | {"dividend"}})
    with pytest.raises(ValueError, match="Unrecognized"):
        pipeline.reconcile_prices(con, df)
    con.close()


# --------------------------------------------------------------------------- #
# Commit 1: load + idempotency (active)
# --------------------------------------------------------------------------- #


def test_initial_load_counts(db):
    run(db, V1)
    assert count(db, "sectors") == 3
    assert count(db, "companies") == 4
    assert count(db, "prices") == 17983
    assert count(db, "load_runs") == 1


def test_reload_same_file_is_idempotent(db):
    run(db, V1)
    run(db, V1)  # identical file -> hash no-op
    assert count(db, "prices") == 17983  # no duplicate rows
    assert count(db, "load_runs") == 1  # the no-op recorded nothing


def test_facebook_name_trimmed(db):
    run(db, V1)
    names = {r[0] for r in query(db, "SELECT name FROM companies")}
    assert "Facebook Class A" in names  # value stripped
    assert "Facebook Class A " not in names  # the dirty variant is gone


def test_no_orphan_foreign_keys(db):
    run(db, V1)
    assert (
        query(
            db,
            "SELECT count(*) FROM companies WHERE sector_id NOT IN (SELECT sector_id FROM sectors)",
        )[0][0]
        == 0
    )
    assert (
        query(
            db,
            "SELECT count(*) FROM prices WHERE company_id NOT IN (SELECT company_id FROM companies)",
        )[0][0]
        == 0
    )


def test_apple_v1_close_preserved(db):
    run(db, V1)
    rows = query(
        db,
        "SELECT close_usd, volume FROM prices "
        "WHERE asof = '2023-11-03' "
        "AND company_id = (SELECT company_id FROM companies WHERE name = 'Apple')",
    )
    close, volume = rows[0]
    assert close == pytest.approx(176.65)  # pre-split v1 values
    assert volume == 79829250


def test_schema_and_load_are_atomic(tmp_path):
    # A mid-run failure must roll back the schema AND the data together, so a
    # corrected re-run is never blocked by a half-applied load (autocommit=False).
    db = str(tmp_path / "atomic.db")
    con = sqlite3.connect(db)
    con.execute("PRAGMA foreign_keys = ON")  # set before the txn so it takes effect
    con.autocommit = False
    try:
        pipeline.ensure_schema(con, str(SCHEMA))
        # company_id 1 does not exist -> FK violation -> IntegrityError
        con.execute("INSERT INTO prices(company_id, asof) VALUES (1, '2020-01-01')")
        con.commit()
    except sqlite3.IntegrityError:
        con.rollback()
    finally:
        con.close()

    con = sqlite3.connect(db)
    tables = [
        r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")
    ]
    con.close()
    assert tables == []  # DDL + data rolled back as one unit


# --------------------------------------------------------------------------- #
# Commit 2: migration + backfill + in-place update
# --------------------------------------------------------------------------- #


def test_v2_migration_backfill_and_split(db):
    run(db, V1)
    run(db, V2)

    # schema migrated and backfilled
    cols = {r[1] for r in query(db, "PRAGMA table_info(prices)")}
    assert "mktcap_usd" in cols
    assert query(db, "SELECT count(*) FROM prices WHERE mktcap_usd IS NULL")[0][0] == 0

    # Apple restated in place for the 2-for-1 split (close halved, volume doubled)
    close, volume = query(
        db,
        "SELECT close_usd, volume FROM prices "
        "WHERE asof = '2023-11-03' "
        "AND company_id = (SELECT company_id FROM companies WHERE name = 'Apple')",
    )[0]
    assert close == pytest.approx(88.325)
    assert volume == 159658500

    # no duplicates from the re-run
    assert count(db, "prices") == 17983

    # two distinct runs recorded; v2 header shows the new column
    runs = query(
        db, "SELECT source_sha256, source_columns FROM load_runs ORDER BY run_id"
    )
    assert len(runs) == 2
    assert runs[0][0] != runs[1][0]
    assert "mktcap_usd" in runs[1][1]
