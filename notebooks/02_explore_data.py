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
        # 02 - Data Exploration

        Interactive exploration of the imported data. Use this notebook to:
        - Understand data structure and quality
        - Identify relationships between tables
        - Spot issues that need cleaning
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
def __(Path, duckdb):
    # Connect to database
    PROJECT_ROOT = Path(__file__).parent.parent
    DB_PATH = PROJECT_ROOT / "data" / "processed" / "analytics.duckdb"
    conn = duckdb.connect(str(DB_PATH))
    return DB_PATH, PROJECT_ROOT, conn


@app.cell
def __(conn, mo):
    # List all tables
    tables = conn.execute("""
        SELECT table_name
        FROM duckdb_tables()
        ORDER BY table_name
    """).fetchdf()

    table_list = tables["table_name"].tolist()

    mo.md(f"## Available Tables\n\nFound **{len(table_list)}** tables in the database.")
    return table_list, tables


@app.cell
def __(mo, table_list):
    # Table selector
    table_selector = mo.ui.dropdown(
        options=table_list,
        value=table_list[0] if table_list else None,
        label="Select a table to explore:"
    )
    table_selector
    return table_selector,


@app.cell
def __(conn, mo, table_selector):
    # Show table schema
    if table_selector.value:
        schema_query = f"DESCRIBE {table_selector.value}"
        schema_df = conn.execute(schema_query).fetchdf()

        mo.md(f"### Schema: `{table_selector.value}`")
    return schema_df, schema_query


@app.cell
def __(mo, schema_df, table_selector):
    if table_selector.value:
        mo.ui.table(schema_df)
    return


@app.cell
def __(conn, mo, table_selector):
    # Row count and sample data
    if table_selector.value:
        count = conn.execute(f"SELECT COUNT(*) FROM {table_selector.value}").fetchone()[0]
        mo.md(f"### Data Preview\n\nTotal rows: **{count:,}**")
    return count,


@app.cell
def __(conn, mo, table_selector):
    # Sample data
    if table_selector.value:
        sample_df = conn.execute(f"SELECT * FROM {table_selector.value} LIMIT 25").fetchdf()
        mo.ui.table(sample_df)
    return sample_df,


@app.cell
def __(mo):
    mo.md("## Data Quality Checks")
    return


@app.cell
def __(conn, mo, table_selector):
    # Null value analysis
    if table_selector.value:
        # Get column info
        cols = conn.execute(f"DESCRIBE {table_selector.value}").fetchdf()
        col_names = cols["column_name"].tolist()

        # Build null count query
        null_counts = []
        for col in col_names:
            null_query = f"SELECT '{col}' as column_name, COUNT(*) - COUNT({col}) as null_count FROM {table_selector.value}"
            result = conn.execute(null_query).fetchdf()
            null_counts.append(result)

        import pandas as pd
        null_df = pd.concat(null_counts, ignore_index=True)
        null_df = null_df[null_df["null_count"] > 0]

        if len(null_df) > 0:
            mo.md(f"### Null Values in `{table_selector.value}`")
        else:
            mo.md(f"### Null Values in `{table_selector.value}`\n\n_No null values found._")
    return col, col_names, cols, null_counts, null_df, null_query, pd, result


@app.cell
def __(mo, null_df):
    if len(null_df) > 0:
        mo.ui.table(null_df)
    return


@app.cell
def __(mo):
    mo.md("## Custom SQL Query")
    return


@app.cell
def __(mo):
    # SQL input
    sql_input = mo.ui.text_area(
        value="SELECT * FROM raw_orders LIMIT 10",
        label="Enter SQL query:",
        full_width=True
    )
    sql_input
    return sql_input,


@app.cell
def __(mo):
    run_button = mo.ui.run_button(label="Run Query")
    run_button
    return run_button,


@app.cell
def __(conn, mo, run_button, sql_input):
    # Execute query
    mo.stop(not run_button.value)

    try:
        query_result = conn.execute(sql_input.value).fetchdf()
        mo.ui.table(query_result)
    except Exception as e:
        mo.md(f"**Error:** {str(e)}")
    return query_result,


@app.cell
def __(mo):
    mo.md(
        r"""
        ## Relationship Analysis

        Explore relationships between tables by joining on key columns.
        """
    )
    return


@app.cell
def __(conn, mo):
    # Join orders with customers and products
    join_query = """
    SELECT
        o.order_id,
        c.customer_name,
        c.region,
        p.product_name,
        p.category,
        o.quantity,
        o.total_amount,
        o.order_date,
        o.status
    FROM raw_orders o
    LEFT JOIN raw_customers c ON o.customer_id = c.customer_id
    LEFT JOIN raw_products p ON o.product_id = p.product_id
    LIMIT 20
    """

    try:
        joined_df = conn.execute(join_query).fetchdf()
        mo.md("### Orders with Customer and Product Details")
    except Exception as e:
        mo.md(f"_Run 01_ingest_data.py first to load sample data._\n\nError: {e}")
        joined_df = None
    return join_query, joined_df


@app.cell
def __(joined_df, mo):
    if joined_df is not None:
        mo.ui.table(joined_df)
    return


@app.cell
def __(mo):
    mo.md(
        r"""
        ## Summary Statistics

        Aggregate metrics for the dataset.
        """
    )
    return


@app.cell
def __(conn, mo):
    # Summary stats
    try:
        summary_query = """
        SELECT
            COUNT(DISTINCT o.order_id) as total_orders,
            COUNT(DISTINCT o.customer_id) as unique_customers,
            COUNT(DISTINCT o.product_id) as unique_products,
            SUM(o.total_amount) as total_revenue,
            AVG(o.total_amount) as avg_order_value,
            MIN(o.order_date) as first_order,
            MAX(o.order_date) as last_order
        FROM raw_orders o
        """
        summary_df = conn.execute(summary_query).fetchdf()
        mo.ui.table(summary_df)
    except Exception as e:
        mo.md(f"_Run 01_ingest_data.py first._")
    return summary_df, summary_query


@app.cell
def __(mo):
    mo.md(
        r"""
        ## Next Steps

        - **03_normalize_data.py** - Create normalized schema with proper relationships
        - **04_generate_reports.py** - Export formatted Excel reports
        """
    )
    return


if __name__ == "__main__":
    app.run()
