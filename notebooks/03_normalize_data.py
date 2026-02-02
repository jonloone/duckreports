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
        # 03 - Normalize Data

        Transform raw Excel data into a normalized schema with:
        - Dimension tables (customers, products)
        - Fact tables (orders)
        - Referential integrity
        - Analytical views
        """
    )
    return


@app.cell
def __():
    import duckdb
    from pathlib import Path
    return Path, duckdb


@app.cell
def __(Path, duckdb):
    # Connect to database
    PROJECT_ROOT = Path(__file__).parent.parent
    DB_PATH = PROJECT_ROOT / "data" / "processed" / "analytics.duckdb"
    SQL_DIR = PROJECT_ROOT / "sql"
    conn = duckdb.connect(str(DB_PATH))
    return DB_PATH, PROJECT_ROOT, SQL_DIR, conn


@app.cell
def __(mo):
    mo.md(
        r"""
        ## Schema Design

        We'll create a star schema with:

        ```
                    ┌─────────────────┐
                    │  dim_customers  │
                    │─────────────────│
                    │ customer_id (PK)│
                    │ customer_name   │
                    │ region          │
                    │ segment         │
                    └────────┬────────┘
                             │
        ┌─────────────────┐  │  ┌─────────────────┐
        │  dim_products   │  │  │   fact_orders   │
        │─────────────────│  │  │─────────────────│
        │ product_id (PK) │──┼──│ order_id (PK)   │
        │ product_name    │  │  │ customer_id (FK)│
        │ category        │  └──│ product_id (FK) │
        │ unit_price      │     │ quantity        │
        └─────────────────┘     │ amount          │
                                │ order_date      │
                                └─────────────────┘
        ```
        """
    )
    return


@app.cell
def __(mo):
    mo.md("## Create Dimension Tables")
    return


@app.cell
def __(conn, mo):
    # Create dim_customers
    conn.execute("""
        CREATE OR REPLACE TABLE dim_customers AS
        SELECT
            customer_id,
            customer_name,
            region,
            segment,
            email,
            CAST(created_date AS DATE) as created_date
        FROM raw_customers
    """)

    customer_count = conn.execute("SELECT COUNT(*) FROM dim_customers").fetchone()[0]
    mo.md(f"Created `dim_customers` with **{customer_count}** records")
    return customer_count,


@app.cell
def __(conn, mo):
    # Create dim_products
    conn.execute("""
        CREATE OR REPLACE TABLE dim_products AS
        SELECT
            product_id,
            product_name,
            category,
            CAST(unit_price AS DECIMAL(10,2)) as unit_price,
            stock_quantity
        FROM raw_products
    """)

    product_count = conn.execute("SELECT COUNT(*) FROM dim_products").fetchone()[0]
    mo.md(f"Created `dim_products` with **{product_count}** records")
    return product_count,


@app.cell
def __(mo):
    mo.md("## Create Fact Table")
    return


@app.cell
def __(conn, mo):
    # Create fact_orders
    conn.execute("""
        CREATE OR REPLACE TABLE fact_orders AS
        SELECT
            order_id,
            customer_id,
            product_id,
            quantity,
            CAST(unit_price AS DECIMAL(10,2)) as unit_price,
            CAST(total_amount AS DECIMAL(10,2)) as total_amount,
            CAST(order_date AS DATE) as order_date,
            status
        FROM raw_orders
    """)

    order_count = conn.execute("SELECT COUNT(*) FROM fact_orders").fetchone()[0]
    mo.md(f"Created `fact_orders` with **{order_count}** records")
    return order_count,


@app.cell
def __(mo):
    mo.md("## Verify Referential Integrity")
    return


@app.cell
def __(conn, mo):
    # Check for orphan customer references
    orphan_customers = conn.execute("""
        SELECT COUNT(*) as orphan_count
        FROM fact_orders f
        LEFT JOIN dim_customers c ON f.customer_id = c.customer_id
        WHERE c.customer_id IS NULL
    """).fetchone()[0]

    orphan_products = conn.execute("""
        SELECT COUNT(*) as orphan_count
        FROM fact_orders f
        LEFT JOIN dim_products p ON f.product_id = p.product_id
        WHERE p.product_id IS NULL
    """).fetchone()[0]

    if orphan_customers == 0 and orphan_products == 0:
        mo.md("**Referential integrity verified.** All foreign keys resolve correctly.")
    else:
        mo.md(f"""
        **Integrity issues found:**
        - Orphan customer references: {orphan_customers}
        - Orphan product references: {orphan_products}
        """)
    return orphan_customers, orphan_products


@app.cell
def __(mo):
    mo.md("## Create Analytical Views")
    return


@app.cell
def __(conn, mo):
    # Monthly summary view
    conn.execute("""
        CREATE OR REPLACE VIEW vw_monthly_summary AS
        SELECT
            DATE_TRUNC('month', order_date) as month,
            COUNT(DISTINCT order_id) as order_count,
            COUNT(DISTINCT customer_id) as customer_count,
            SUM(total_amount) as revenue,
            AVG(total_amount) as avg_order_value
        FROM fact_orders
        GROUP BY DATE_TRUNC('month', order_date)
        ORDER BY month
    """)

    mo.md("Created `vw_monthly_summary`")
    return


@app.cell
def __(conn, mo):
    # Customer report view
    conn.execute("""
        CREATE OR REPLACE VIEW vw_customer_report AS
        SELECT
            c.customer_id,
            c.customer_name,
            c.region,
            c.segment,
            COUNT(DISTINCT f.order_id) as total_orders,
            SUM(f.total_amount) as total_revenue,
            AVG(f.total_amount) as avg_order_value,
            MIN(f.order_date) as first_order,
            MAX(f.order_date) as last_order
        FROM dim_customers c
        LEFT JOIN fact_orders f ON c.customer_id = f.customer_id
        GROUP BY c.customer_id, c.customer_name, c.region, c.segment
        ORDER BY total_revenue DESC
    """)

    mo.md("Created `vw_customer_report`")
    return


@app.cell
def __(conn, mo):
    # Product performance view
    conn.execute("""
        CREATE OR REPLACE VIEW vw_product_performance AS
        SELECT
            p.product_id,
            p.product_name,
            p.category,
            p.unit_price,
            COUNT(DISTINCT f.order_id) as times_ordered,
            SUM(f.quantity) as total_quantity_sold,
            SUM(f.total_amount) as total_revenue
        FROM dim_products p
        LEFT JOIN fact_orders f ON p.product_id = f.product_id
        GROUP BY p.product_id, p.product_name, p.category, p.unit_price
        ORDER BY total_revenue DESC
    """)

    mo.md("Created `vw_product_performance`")
    return


@app.cell
def __(mo):
    mo.md("## Preview Normalized Data")
    return


@app.cell
def __(conn, mo):
    mo.md("### Monthly Summary")
    return


@app.cell
def __(conn, mo):
    monthly_df = conn.execute("SELECT * FROM vw_monthly_summary").fetchdf()
    mo.ui.table(monthly_df)
    return monthly_df,


@app.cell
def __(mo):
    mo.md("### Customer Report")
    return


@app.cell
def __(conn, mo):
    customer_df = conn.execute("SELECT * FROM vw_customer_report LIMIT 10").fetchdf()
    mo.ui.table(customer_df)
    return customer_df,


@app.cell
def __(mo):
    mo.md("### Product Performance")
    return


@app.cell
def __(conn, mo):
    product_df = conn.execute("SELECT * FROM vw_product_performance LIMIT 10").fetchdf()
    mo.ui.table(product_df)
    return product_df,


@app.cell
def __(mo):
    mo.md("## Export SQL Definitions")
    return


@app.cell
def __(SQL_DIR, mo):
    # SQL definitions to export
    schema_sql = {
        "dim_customers.sql": """-- Dimension: Customers
CREATE OR REPLACE TABLE dim_customers AS
SELECT
    customer_id,
    customer_name,
    region,
    segment,
    email,
    CAST(created_date AS DATE) as created_date
FROM raw_customers;
""",
        "dim_products.sql": """-- Dimension: Products
CREATE OR REPLACE TABLE dim_products AS
SELECT
    product_id,
    product_name,
    category,
    CAST(unit_price AS DECIMAL(10,2)) as unit_price,
    stock_quantity
FROM raw_products;
""",
        "fact_orders.sql": """-- Fact: Orders
CREATE OR REPLACE TABLE fact_orders AS
SELECT
    order_id,
    customer_id,
    product_id,
    quantity,
    CAST(unit_price AS DECIMAL(10,2)) as unit_price,
    CAST(total_amount AS DECIMAL(10,2)) as total_amount,
    CAST(order_date AS DATE) as order_date,
    status
FROM raw_orders;
"""
    }

    views_sql = {
        "vw_monthly_summary.sql": """-- View: Monthly Summary
CREATE OR REPLACE VIEW vw_monthly_summary AS
SELECT
    DATE_TRUNC('month', order_date) as month,
    COUNT(DISTINCT order_id) as order_count,
    COUNT(DISTINCT customer_id) as customer_count,
    SUM(total_amount) as revenue,
    AVG(total_amount) as avg_order_value
FROM fact_orders
GROUP BY DATE_TRUNC('month', order_date)
ORDER BY month;
""",
        "vw_customer_report.sql": """-- View: Customer Report
CREATE OR REPLACE VIEW vw_customer_report AS
SELECT
    c.customer_id,
    c.customer_name,
    c.region,
    c.segment,
    COUNT(DISTINCT f.order_id) as total_orders,
    SUM(f.total_amount) as total_revenue,
    AVG(f.total_amount) as avg_order_value,
    MIN(f.order_date) as first_order,
    MAX(f.order_date) as last_order
FROM dim_customers c
LEFT JOIN fact_orders f ON c.customer_id = f.customer_id
GROUP BY c.customer_id, c.customer_name, c.region, c.segment
ORDER BY total_revenue DESC;
"""
    }

    # Write schema files
    schema_dir = SQL_DIR / "schema"
    schema_dir.mkdir(parents=True, exist_ok=True)
    for filename, sql in schema_sql.items():
        (schema_dir / filename).write_text(sql)

    # Write view files
    views_dir = SQL_DIR / "views"
    views_dir.mkdir(parents=True, exist_ok=True)
    for filename, sql in views_sql.items():
        (views_dir / filename).write_text(sql)

    mo.md(f"""
    **SQL files exported:**
    - Schema: {len(schema_sql)} files
    - Views: {len(views_sql)} files

    Location: `sql/`
    """)
    return filename, schema_dir, schema_sql, sql, views_dir, views_sql


@app.cell
def __(mo):
    mo.md(
        r"""
        ## Next Steps

        Data is now normalized and ready for reporting.

        - **04_generate_reports.py** - Export formatted Excel reports
        """
    )
    return


if __name__ == "__main__":
    app.run()
