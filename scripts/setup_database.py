#!/usr/bin/env python3
"""
Initialize the DuckDB database with schema and sample data.
Run this script after installing dependencies.

Usage:
    python scripts/setup_database.py
"""

from pathlib import Path
import sys

# Add project root to path for imports
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

try:
    import duckdb
except ImportError:
    print("Error: duckdb is required. Install with: pip install duckdb")
    exit(1)

# Paths
DB_PATH = PROJECT_ROOT / "data" / "processed" / "analytics.duckdb"
SQL_DIR = PROJECT_ROOT / "sql"
DATA_DIR = PROJECT_ROOT / "data" / "raw"


def run_sql_file(conn, sql_path: Path):
    """Execute a SQL file."""
    if sql_path.exists():
        sql = sql_path.read_text()
        conn.execute(sql)
        print(f"  Executed: {sql_path.name}")
    else:
        print(f"  Warning: {sql_path} not found")


def main():
    """Initialize the database."""
    print("Setting up DuckDB database...")
    print("-" * 40)

    # Ensure directories exist
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Connect to database (creates if not exists)
    conn = duckdb.connect(str(DB_PATH))
    print(f"Database: {DB_PATH}")

    # Install and load Excel extension
    print("\nLoading extensions...")
    conn.execute("INSTALL spatial;")  # Required for some Excel features
    conn.execute("LOAD spatial;")
    print("  Loaded: spatial")

    # Create schema tables
    print("\nCreating schema...")
    schema_dir = SQL_DIR / "schema"
    if schema_dir.exists():
        for sql_file in sorted(schema_dir.glob("*.sql")):
            run_sql_file(conn, sql_file)
    else:
        print("  No schema files found")

    # Create views
    print("\nCreating views...")
    views_dir = SQL_DIR / "views"
    if views_dir.exists():
        for sql_file in sorted(views_dir.glob("*.sql")):
            run_sql_file(conn, sql_file)
    else:
        print("  No view files found")

    # Check for sample data
    print("\nChecking for sample data...")
    excel_files = list(DATA_DIR.glob("*.xlsx"))
    if excel_files:
        print(f"  Found {len(excel_files)} Excel files in data/raw/")
        print("  Run notebook 01_ingest_data.py to import data")
    else:
        print("  No Excel files found in data/raw/")
        print("  Run: python scripts/generate_sample_data.py")

    # Close connection
    conn.close()

    print("-" * 40)
    print("Database setup complete!")
    print(f"\nDatabase location: {DB_PATH}")


if __name__ == "__main__":
    main()
