import marimo

__generated_with = "0.9.0"
app = marimo.App(width="medium")


@app.cell
def __():
    import marimo as mo
    return mo,


@app.cell
def __(mo):
    mo.md(
        r"""
        # 01 - Data Ingestion

        Load Excel files from `data/raw/` into DuckDB for analysis.

        ## Workflow
        1. Scan for Excel files in the raw data folder
        2. Preview file contents
        3. Load data into DuckDB tables
        4. Verify import success
        """
    )
    return


@app.cell
def __():
    import duckdb
    from pathlib import Path
    import pandas as pd
    return Path, duckdb, pd


@app.cell
def __(Path):
    # Project paths
    PROJECT_ROOT = Path(__file__).parent.parent
    DATA_RAW = PROJECT_ROOT / "data" / "raw"
    DATA_PROCESSED = PROJECT_ROOT / "data" / "processed"
    DB_PATH = DATA_PROCESSED / "analytics.duckdb"

    # Ensure directories exist
    DATA_PROCESSED.mkdir(parents=True, exist_ok=True)
    return DATA_PROCESSED, DATA_RAW, DB_PATH, PROJECT_ROOT


@app.cell
def __(DATA_RAW, mo):
    # List available Excel files
    excel_files = list(DATA_RAW.glob("*.xlsx"))

    mo.md(f"""
    ## Available Files

    Found **{len(excel_files)}** Excel files in `data/raw/`:
    """)
    return excel_files,


@app.cell
def __(excel_files, mo):
    # Create file list
    if excel_files:
        file_list = "\n".join([f"- `{f.name}`" for f in excel_files])
    else:
        file_list = "_No Excel files found. Run `python scripts/generate_sample_data.py` first._"

    mo.md(file_list)
    return file_list,


@app.cell
def __(DB_PATH, duckdb):
    # Connect to DuckDB
    conn = duckdb.connect(str(DB_PATH))

    # Install and load required extensions
    conn.execute("INSTALL spatial;")
    conn.execute("LOAD spatial;")
    return conn,


@app.cell
def __(conn, excel_files, mo, pd):
    # Preview each Excel file
    previews = {}

    for file_path in excel_files:
        df = pd.read_excel(file_path)
        previews[file_path.stem] = df

    mo.md("## File Previews")
    return df, file_path, previews


@app.cell
def __(mo, previews):
    # Display previews as tabs
    if previews:
        tabs = {name: mo.ui.table(df.head(10)) for name, df in previews.items()}
        mo.ui.tabs(tabs)
    else:
        mo.md("_No files to preview_")
    return tabs,


@app.cell
def __(mo):
    mo.md("## Load Data into DuckDB")
    return


@app.cell
def __(conn, excel_files, mo):
    # Load each Excel file into a DuckDB table
    loaded_tables = []

    for file_path in excel_files:
        table_name = file_path.stem.replace("sample_", "raw_")

        # Create table from Excel file
        conn.execute(f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT * FROM read_excel('{file_path}')
        """)

        # Get row count
        result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        row_count = result[0] if result else 0

        loaded_tables.append({"Table": table_name, "Rows": row_count, "Source": file_path.name})

    mo.md(f"Loaded **{len(loaded_tables)}** tables into DuckDB")
    return file_path, loaded_tables, result, row_count, table_name


@app.cell
def __(loaded_tables, mo, pd):
    # Show loaded tables summary
    if loaded_tables:
        mo.ui.table(pd.DataFrame(loaded_tables))
    else:
        mo.md("_No tables loaded_")
    return


@app.cell
def __(mo):
    mo.md(
        r"""
        ## Verify Import

        Query the loaded tables to confirm data was imported correctly.
        """
    )
    return


@app.cell
def __(conn, mo):
    # Show all tables in the database
    tables_query = conn.execute("""
        SELECT table_name, estimated_size
        FROM duckdb_tables()
        ORDER BY table_name
    """).fetchdf()

    mo.ui.table(tables_query)
    return tables_query,


@app.cell
def __(mo):
    mo.md(
        r"""
        ## Next Steps

        Data is now loaded into DuckDB. Continue to:

        - **02_explore_data.py** - Interactive data exploration
        - **03_normalize_data.py** - Create normalized schema
        - **04_generate_reports.py** - Export formatted Excel reports
        """
    )
    return


@app.cell
def __(conn):
    # Close connection when done
    # Note: In practice, you might want to keep this open
    # conn.close()
    pass
    return


if __name__ == "__main__":
    app.run()
