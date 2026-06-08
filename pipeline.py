from pathlib import Path
import argparse
import polars as pl
import sqlite3
import hashlib

# prices fact contract: the single place column handling is declared.
# REQUIRED_COLUMNS must be present on every source. PRICE_MEASURES are the fact
# measures the pipeline knows, with their declared SQL types; adding a measure is a
# one-line change here and it drives validation, migration, and the upsert together.
REQUIRED_COLUMNS = {
    "name",
    "asof",
    "volume",
    "close_usd",
    "sector_level1",
    "sector_level2",
}
PRICE_MEASURES = {"volume": "INTEGER", "close_usd": "REAL", "mktcap_usd": "REAL"}


def parse_args() -> argparse.Namespace:
    """
    Parses cmd line args.

    Args:
        None

    Returns:
        argparse.Namespace: The parsed command-line arguments.
    """
    parser = argparse.ArgumentParser(
        description="Pipeline: Runs the pipeline for the OWL example."
    )
    parser.add_argument(
        "--db",
        dest="db",
        type=str,
        default="owl.db",
        help="The path to where your .db will be written, default is ./owl.db. Ex: --db owl.db.",
    )
    parser.add_argument(
        "--file",
        dest="file",
        type=str,
        required=True,
        help="The path to the file containing your data. Ex: --file data/stock-data-se-owl.xlsx",
    )
    parser.add_argument(
        "--schema",
        dest="schema",
        type=str,
        default="./schema.sql",
        help="The path to the file with your schema definitions. Default is ./schema.sql",
    )
    args = parser.parse_args()
    return args


def read_file(path: str) -> pl.DataFrame:
    """
    Reads the input file at the path provided and returns a Polars df
    if it exists.

    Args:
        path: path to the input file.

    Returns:
        pl.DataFrame: A polars df of the source file.
    """

    # Map of file extensions to polars read functions
    file_readers = {
        ".csv": pl.read_csv,
        ".xlsx": pl.read_excel,
        ".xls": pl.read_excel,
    }

    p = Path(path)
    extension = p.suffix.lower()

    if not p.exists():
        raise FileNotFoundError(f"{p} not found.")

    if extension not in file_readers:
        raise ValueError(f"Unsupported file extension: {extension}")

    print(f"Processing file {p}")
    df = file_readers[extension](p)
    return df


def clean_file(raw: pl.DataFrame) -> pl.DataFrame:
    """
    Cleans the raw source file.

    Args:
        raw: A dataframe representing the source file.

    Returns:
        pl.DataFrame: A polars df of the cleaned raw file.
    """

    # drop '#' and strip spaces on the column names
    result = raw.drop("#")
    result.columns = [str(c).strip() for c in result.columns]

    # strip blanks on text values; normalize asof to ISO YYYY-MM-DD text.
    # asof comes in as a date from xlsx but as text from csv, so parse the
    # text case first; both inputs then end up as ISO strings.
    asof = pl.col("asof")
    if result.schema["asof"] == pl.String:
        asof = asof.str.to_date()
    result = result.with_columns(
        pl.col("name").str.strip_chars(),
        pl.col("sector_level1").str.strip_chars(),
        pl.col("sector_level2").str.strip_chars(),
        asof.dt.strftime("%Y-%m-%d"),
    )

    # defensive dedupe on name and asof
    result = result.unique(subset=["name", "asof"], keep="last")

    return result


def reconcile_prices(con, df) -> list:
    """
    Validates the source columns against the contract and migrates prices to match.
    Fails if any REQUIRED_COLUMNS column is missing or on any column outside the contract,
    i.e. no silent absorb of unknown data. Adds any known measure not yet in prices, using
    its declared type (an idempotent ALTER, since SQLite has no ADD COLUMN IF NOT EXISTS).

    Args:
        con (sqlite3.Connection): SQLite connection (prices must already exist).
        df (pl.DataFrame): Cleaned source data.

    Returns:
        list: the measure columns present in this source, in contract order; this
        drives the prices upsert in load().
    """
    cols = set(df.columns)
    if missing := REQUIRED_COLUMNS - cols:
        raise ValueError(f"Source missing required columns: {sorted(missing)}")
    if unknown := cols - REQUIRED_COLUMNS - set(PRICE_MEASURES):
        raise ValueError(f"Unrecognized column(s) {sorted(unknown)} found on input.")

    present = [c for c in PRICE_MEASURES if c in cols]
    existing = {row[1] for row in con.execute("PRAGMA table_info(prices)")}
    for c in present:
        if c not in existing:
            # declared type, not inferred; column name is a trusted contract key
            con.execute(f"ALTER TABLE prices ADD COLUMN {c} {PRICE_MEASURES[c]}")
    return present


def ensure_schema(con, schema):
    """
    Creates the schema in the SQLite database (idempotent CREATE TABLE statements).
    Reads the schema file, converts it to text, and executes it. Migrations that
    evolve an existing table (new measure columns) live in reconcile_prices.

    Args:
        con (sqlite3 Connection): Sqlite database connection.
        schema (str): Path to the schema sql file. Default is ./schema.sql.

    Returns:
        None
    """
    p = Path(schema)
    if not p.exists():
        raise FileNotFoundError(f"{p} not found.")
    else:
        print(f"Using schema file {p}.")

    # read the schema file, convert it to text, and execute it.
    ddl = p.read_text()
    con.executescript(ddl)


def load(con, df, measures, file):
    """
    Upserts data from the source file into sectors/companies/prices + load_runs tables.

    Args:
        con (sqlite3.Connection): SQLite database connection.
        df (pl.DataFrame): Cleaned, validated source data.
        measures (list): Price measure columns to write, from reconcile_prices.
        file (str): Path to the source file (hashed for the idempotency no-op).

    Returns:
        None
    """
    cur = con.cursor()

    # Check for if the file has been loaded before
    file_hash = hashlib.sha256(Path(file).read_bytes()).hexdigest()
    if cur.execute(
        "SELECT 1 FROM load_runs WHERE source_sha256=?", (file_hash,)
    ).fetchone():
        print(f"{file} already loaded (hash match). No reload needed.")
        return

    # Upsert sectors
    sectors = df.select("sector_level1", "sector_level2").unique()
    cur.executemany(
        """
        INSERT INTO sectors(sector_level1, sector_level2) VALUES(?,?)
        ON CONFLICT(sector_level1, sector_level2) DO NOTHING
        ;
        """,
        sectors.iter_rows(),
    )

    # Upsert companies
    companies = df.select("name", "sector_level1", "sector_level2").unique()
    cur.executemany(
        """
        INSERT INTO companies(name, sector_id)
        VALUES(?, (SELECT sector_id FROM sectors WHERE sector_level1=? AND sector_level2=?))
        ON CONFLICT(name) DO UPDATE SET sector_id=excluded.sector_id
        """,
        companies.iter_rows(),  # yields (name, level1, level2)
    )

    # Upsert prices. The column list is the measures reconcile_prices resolved for
    # this source, so v1 and v2 share one path and the SQL is built from the contract
    # (identifiers are trusted contract keys, never source data).
    prices = df.select("name", "asof", *measures).unique(
        subset=["name", "asof"], keep="last"
    )
    cols = ", ".join(measures)
    set_clause = ", ".join(f"{m}=excluded.{m}" for m in measures)
    slots = ", ".join(
        ["(SELECT company_id FROM companies WHERE name=?)", "?", *["?"] * len(measures)]
    )
    prices_sql = (
        f"INSERT INTO prices(company_id, asof, {cols}) "
        f"VALUES({slots}) "
        f"ON CONFLICT(company_id, asof) DO UPDATE SET {set_clause}"
    )
    cur.executemany(prices_sql, prices.iter_rows())

    # record this run in load_runs (the load tracking table)
    cur.execute(
        "INSERT INTO load_runs(source_path, source_sha256, source_columns, rows_read, rows_upserted)"
        "VALUES(?,?,?,?,?)",
        (str(file), file_hash, ",".join(df.columns), df.height, prices.height),
    )


def main() -> None:
    """
    Function kicks off the pipeline run.
    """

    # Parse cli args
    args = parse_args()

    # Read the source file and clean it
    raw = read_file(args.file)
    clean = clean_file(raw)

    # Open the database connection.
    con = sqlite3.connect(args.db)  # creates/opens owl.db at this path
    con.execute("PRAGMA foreign_keys = ON")  # SQLite ignores FK constraints otherwise

    # 3.12+: schema + load become one atomic transaction, for Py<3.12 do this in 2 transactions
    con.autocommit = False
    try:
        ensure_schema(con, args.schema)  # create base tables (idempotent)
        measures = reconcile_prices(
            con, clean
        )  # validate source + migrate prices to match
        load(
            con, clean, measures, args.file
        )  # upsert sectors/companies/prices + load_runs
        con.commit()  # schema, migration, and data in one db commit
    except Exception:
        con.rollback()  # roll back schema + data together on any failure
        raise
    finally:
        con.close()


if __name__ == "__main__":
    main()
