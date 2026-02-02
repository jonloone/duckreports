import marimo

__generated_with = "0.19.7"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _(mo):
    mo.md(r"""
    # ERA 01 - Ingest Lab EDD Data

    Import Environmental Data Deliverables (EDD) from Excel into DuckDB.

    ## Workflow
    1. Load site locations
    2. Import lab sample metadata
    3. Import analytical results
    4. Import field measurements
    5. Validate data quality
    """)
    return


@app.cell
def _():
    import duckdb
    from pathlib import Path
    import pandas as pd
    return Path, duckdb, pd


@app.cell
def _(Path):
    # Project paths
    PROJECT_ROOT = Path(__file__).parent.parent
    DATA_RAW = PROJECT_ROOT / "data" / "raw"
    DATA_PROCESSED = PROJECT_ROOT / "data" / "processed"
    SQL_DIR = PROJECT_ROOT / "sql"
    DB_PATH = DATA_PROCESSED / "analytics.duckdb"

    DATA_PROCESSED.mkdir(parents=True, exist_ok=True)
    return DATA_RAW, DB_PATH, SQL_DIR


@app.cell
def _(DB_PATH, SQL_DIR, duckdb):
    # Connect to DuckDB and initialize schema
    conn = duckdb.connect(str(DB_PATH))

    # Load ERA schema
    schema_path = SQL_DIR / "schema" / "era_schema.sql"
    if schema_path.exists():
        schema_sql = schema_path.read_text()
        for statement in schema_sql.split(';'):
            statement = statement.strip()
            if statement and not statement.startswith('--'):
                try:
                    conn.execute(statement)
                except Exception as e:
                    pass  # Views may fail if tables don't exist yet
    return (conn,)


@app.cell
def _(DATA_RAW, mo):
    # Check for EDD files
    edd_files = {
        "locations": DATA_RAW / "site_locations.xlsx",
        "lab_results": DATA_RAW / "lab_results_edd.xlsx",
        "field_measurements": DATA_RAW / "field_measurements.xlsx",
    }

    file_status = []
    for name, path in edd_files.items():
        file_status.append({
            "File": name,
            "Path": path.name,
            "Exists": "Yes" if path.exists() else "No"
        })

    mo.md("## Input Files")
    return edd_files, file_status


@app.cell
def _(file_status, mo, pd):
    mo.ui.table(pd.DataFrame(file_status))
    return


@app.cell
def _(edd_files, mo):
    if not edd_files["lab_results"].exists():
        mo.md("""
        **Missing data files!**

        Run the following command to generate sample ERA data:
        ```bash
        python scripts/generate_era_sample_data.py
        python scripts/generate_era_reference_data.py
        ```
        """)
    return


@app.cell
def _(mo):
    mo.md("""
    ## Load Site Locations
    """)
    return


@app.cell
def _(conn, edd_files, mo, pd):
    # Load locations
    if edd_files["locations"].exists():
        locations_df = pd.read_excel(edd_files["locations"])

        # Insert into dim_locations
        conn.execute("DELETE FROM dim_locations")

        for _, row in locations_df.iterrows():
            conn.execute("""
                INSERT INTO dim_locations (location_id, location_name, location_type,
                    latitude, longitude, elevation_ft, total_depth_ft, install_date, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                row.get('location_id'),
                row.get('location_name'),
                row.get('location_type'),
                row.get('latitude'),
                row.get('longitude'),
                row.get('elevation_ft'),
                row.get('total_depth_ft'),
                row.get('install_date'),
                row.get('status', 'Active')
            ])

        loc_count = conn.execute("SELECT COUNT(*) FROM dim_locations").fetchone()[0]
        mo.md(f"Loaded **{loc_count}** locations")
    else:
        mo.md("_Location file not found_")
        locations_df = None
    return


@app.cell
def _(conn, mo):
    # Preview locations
    loc_preview = conn.execute("SELECT * FROM dim_locations").fetchdf()
    mo.ui.table(loc_preview)
    return


@app.cell
def _(mo):
    mo.md("""
    ## Load Lab Results (EDD)
    """)
    return


@app.cell
def _(conn, edd_files, mo, pd):
    # Load samples sheet
    if edd_files["lab_results"].exists():
        samples_df = pd.read_excel(edd_files["lab_results"], sheet_name="Samples")

        # Clear existing samples
        conn.execute("DELETE FROM fact_samples")

        for _, row in samples_df.iterrows():
            conn.execute("""
                INSERT INTO fact_samples (sample_id, location_id, sample_date, sample_time,
                    matrix_code, sample_type, depth_top_ft, depth_bottom_ft,
                    sample_method, sampler_name, lab_name, lab_sample_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                row.get('sample_id'),
                row.get('location_id'),
                str(row.get('sample_date'))[:10] if pd.notna(row.get('sample_date')) else None,
                row.get('sample_time'),
                row.get('matrix_code'),
                row.get('sample_type', 'N'),
                row.get('depth_top_ft') if pd.notna(row.get('depth_top_ft')) else None,
                row.get('depth_bottom_ft') if pd.notna(row.get('depth_bottom_ft')) else None,
                row.get('sample_method'),
                row.get('sampler_name'),
                row.get('lab_name'),
                row.get('lab_sample_id')
            ])

        sample_count = conn.execute("SELECT COUNT(*) FROM fact_samples").fetchone()[0]
        mo.md(f"Loaded **{sample_count}** samples")
    else:
        samples_df = None
        mo.md("_Lab results file not found_")
    return


@app.cell
def _(conn, edd_files, mo, pd):
    # Load results sheet
    if edd_files["lab_results"].exists():
        results_df = pd.read_excel(edd_files["lab_results"], sheet_name="Results")

        # Clear existing results
        conn.execute("DELETE FROM fact_results")

        # Get max result_id
        result_id = 1

        for _, row in results_df.iterrows():
            # Also ensure analyte exists in dim_analytes
            cas = row.get('cas_rn')
            name = row.get('analyte_name')

            conn.execute("""
                INSERT OR IGNORE INTO dim_analytes (cas_rn, analyte_name)
                VALUES (?, ?)
            """, [cas, name])

            conn.execute("""
                INSERT INTO fact_results (result_id, sample_id, cas_rn, result_value,
                    result_unit, detection_limit, detect_flag, lab_qualifier,
                    dilution_factor, analysis_method, analysis_date, basis, percent_moisture)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                result_id,
                row.get('sample_id'),
                cas,
                row.get('result_value'),
                row.get('result_unit'),
                row.get('detection_limit'),
                row.get('detect_flag', 'Y'),
                row.get('lab_qualifier', ''),
                row.get('dilution_factor', 1),
                row.get('analysis_method'),
                str(row.get('analysis_date'))[:10] if pd.notna(row.get('analysis_date')) else None,
                row.get('basis'),
                row.get('percent_moisture') if pd.notna(row.get('percent_moisture')) else None
            ])
            result_id += 1

        result_count = conn.execute("SELECT COUNT(*) FROM fact_results").fetchone()[0]
        mo.md(f"Loaded **{result_count}** analytical results")
    else:
        results_df = None
    return


@app.cell
def _(mo):
    mo.md("""
    ## Data Summary
    """)
    return


@app.cell
def _(conn, mo):
    # Summary statistics
    summary_query = """
    SELECT
        'Locations' as category,
        COUNT(*) as count
    FROM dim_locations
    UNION ALL
    SELECT 'Samples', COUNT(*) FROM fact_samples
    UNION ALL
    SELECT 'Results', COUNT(*) FROM fact_results
    UNION ALL
    SELECT 'Unique Analytes', COUNT(DISTINCT cas_rn) FROM fact_results
    UNION ALL
    SELECT 'Screening Levels', COUNT(*) FROM ref_screening_levels
    """
    summary_df = conn.execute(summary_query).fetchdf()
    mo.ui.table(summary_df)
    return


@app.cell
def _(mo):
    mo.md("""
    ## Sample Matrix Distribution
    """)
    return


@app.cell
def _(conn, mo):
    matrix_query = """
    SELECT
        m.matrix_name,
        COUNT(DISTINCT s.sample_id) as sample_count,
        COUNT(r.result_id) as result_count,
        COUNT(DISTINCT s.location_id) as location_count
    FROM fact_samples s
    JOIN dim_matrix m ON s.matrix_code = m.matrix_code
    LEFT JOIN fact_results r ON s.sample_id = r.sample_id
    GROUP BY m.matrix_name
    """
    matrix_df = conn.execute(matrix_query).fetchdf()
    mo.ui.table(matrix_df)
    return


@app.cell
def _(mo):
    mo.md("""
    ## Data Quality Check
    """)
    return


@app.cell
def _(conn, mo):
    # Check for data quality issues
    qc_checks = []

    # Orphan samples (no results)
    orphan_samples = conn.execute("""
        SELECT COUNT(*) FROM fact_samples s
        LEFT JOIN fact_results r ON s.sample_id = r.sample_id
        WHERE r.result_id IS NULL
    """).fetchone()[0]
    qc_checks.append({"Check": "Samples without results", "Count": orphan_samples, "Status": "OK" if orphan_samples == 0 else "Review"})

    # Results without screening levels
    no_rsl = conn.execute("""
        SELECT COUNT(DISTINCT r.cas_rn) FROM fact_results r
        LEFT JOIN ref_screening_levels sl ON r.cas_rn = sl.cas_rn
        WHERE sl.cas_rn IS NULL
    """).fetchone()[0]
    qc_checks.append({"Check": "Analytes without RSLs", "Count": no_rsl, "Status": "OK" if no_rsl == 0 else "Note"})

    # Non-detect percentage
    nd_pct = conn.execute("""
        SELECT ROUND(100.0 * SUM(CASE WHEN detect_flag = 'N' THEN 1 ELSE 0 END) / COUNT(*), 1)
        FROM fact_results
    """).fetchone()[0]
    qc_checks.append({"Check": "Non-detect percentage", "Count": f"{nd_pct}%", "Status": "OK"})

    # Qualified results
    qualified = conn.execute("""
        SELECT COUNT(*) FROM fact_results WHERE lab_qualifier != '' AND lab_qualifier IS NOT NULL
    """).fetchone()[0]
    qc_checks.append({"Check": "Qualified results (J, U, etc.)", "Count": qualified, "Status": "OK"})

    mo.md("### Quality Control Summary")
    return (qc_checks,)


@app.cell
def _(mo, pd, qc_checks):
    mo.ui.table(pd.DataFrame(qc_checks))
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Lab Qualifier Distribution

    Understanding qualifiers is critical for data usability:
    - **U** = Non-detect (use detection limit or 1/2 DL for statistics)
    - **J** = Estimated (use value, flag in reports)
    - **B** = Found in blank (evaluate for usability)
    - **R** = Rejected (do not use)
    """)
    return


@app.cell
def _(conn, mo):
    qualifier_query = """
    SELECT
        COALESCE(NULLIF(r.lab_qualifier, ''), 'None') as qualifier,
        q.qualifier_name,
        q.detection_status,
        COUNT(*) as count,
        ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) as percentage
    FROM fact_results r
    LEFT JOIN dim_qualifiers q ON r.lab_qualifier = q.qualifier
    GROUP BY r.lab_qualifier, q.qualifier_name, q.detection_status
    ORDER BY count DESC
    """
    qualifier_df = conn.execute(qualifier_query).fetchdf()
    mo.ui.table(qualifier_df)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Next Steps

    Data is now loaded and validated. Continue to:

    - **era_02_screening.py** - Compare results to EPA screening levels
    - **era_03_statistics.py** - Calculate UCL95, detection frequency
    - **era_04_reports.py** - Generate ERA summary tables
    """)
    return


if __name__ == "__main__":
    app.run()
