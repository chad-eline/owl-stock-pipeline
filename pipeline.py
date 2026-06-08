from pathlib import Path
import argparse
import polars as pl
import sqlite3
import hashlib


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

    # strip blanks on text values; normalize asof to ISO YYYY-MM-DD text
    result = result.with_columns(
        pl.col("name").str.strip_chars(),
        pl.col("sector_level1").str.strip_chars(),
        pl.col("sector_level2").str.strip_chars(),
        pl.col("asof").dt.strftime("%Y-%m-%d"),
    )

    # defensive dedupe on name and asof
    result = result.unique(subset=["name", "asof"], keep="last")

    return result


def ensure_schema(con, schema):
    """
    Creates the schema in the SQLite database.
    Reads the schema file, converts it to text, and executes it.
    Ensures that the schema exists in the database.

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


def load(con, df, file):
    """
    Upserts data from the source file into sectors/companies/prices + load_runs tables.

    Args:
        con (sqlite3.Connection): SQLite database connection.
        df (pl.DataFrame): Cleaned, validated source data.
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

    # Upsert prices
    prices = df.select("name", "asof", "volume", "close_usd").unique()
    cur.executemany(
        """
        INSERT INTO prices(company_id, asof, volume, close_usd)
        VALUES((select company_id from companies where name=?), ?, ?, ?)
        ON CONFLICT(company_id, asof) DO UPDATE SET
            volume=excluded.volume, 
            close_usd=excluded.close_usd
        ;
        """,
        prices.iter_rows(),
    )

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
        ensure_schema(con, args.schema)  # deploy DDL (idempotent)
        load(con, clean, args.file)  # upsert sectors/companies/prices + load_runs
        con.commit()  # both ddl and data load in one db commit
    except Exception:
        con.rollback()  # roll back schema + data together on any failure
        raise
    finally:
        con.close()


if __name__ == "__main__":
    main()
