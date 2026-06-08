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
SCHEMA = ROOT / "schema.sql"


def run(db_path, src):
    """Run one pipeline pass the way main() does, minus argparse."""
    con = sqlite3.connect(db_path)
    con.execute("PRAGMA foreign_keys = ON")  # must be set before the txn opens
    con.autocommit = False  # mirror main(): schema + load in one transaction
    try:
        pipeline.ensure_schema(con, str(SCHEMA))
        df = pipeline.clean_file(pipeline.read_file(str(src)))
        pipeline.load(con, df, str(src))
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


def test_read_file_missing(tmp_path):
    with pytest.raises(FileNotFoundError):
        pipeline.read_file(str(tmp_path / "nope.xlsx"))


def test_read_file_unsupported_extension(tmp_path):
    p = tmp_path / "data.txt"
    p.write_text("not a supported format")
    with pytest.raises(ValueError):
        pipeline.read_file(str(p))


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
