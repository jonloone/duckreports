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
        # ERA 02 - Screening Level Comparison

        Compare analytical results to EPA Regional Screening Levels (RSLs)
        and identify Chemicals of Potential Concern (COPCs).

        ## Screening Approach
        - **Tier 1**: Compare to most conservative RSLs (residential)
        - Calculate Hazard Quotients (HQ = Concentration / Screening Level)
        - HQ > 1 indicates potential concern requiring further evaluation
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
    PROJECT_ROOT = Path(__file__).parent.parent
    DB_PATH = PROJECT_ROOT / "data" / "processed" / "analytics.duckdb"
    conn = duckdb.connect(str(DB_PATH))
    return DB_PATH, PROJECT_ROOT, conn


@app.cell
def __(mo):
    mo.md("## Screening Level Coverage")
    return


@app.cell
def __(conn, mo):
    # Check RSL coverage
    coverage_query = """
    SELECT
        a.analyte_group,
        COUNT(DISTINCT r.cas_rn) as analytes_tested,
        COUNT(DISTINCT CASE WHEN sl.cas_rn IS NOT NULL THEN r.cas_rn END) as has_rsl,
        ROUND(100.0 * COUNT(DISTINCT CASE WHEN sl.cas_rn IS NOT NULL THEN r.cas_rn END) /
              NULLIF(COUNT(DISTINCT r.cas_rn), 0), 0) as coverage_pct
    FROM fact_results r
    LEFT JOIN dim_analytes a ON r.cas_rn = a.cas_rn
    LEFT JOIN ref_screening_levels sl ON r.cas_rn = sl.cas_rn
    GROUP BY a.analyte_group
    ORDER BY analytes_tested DESC
    """
    coverage_df = conn.execute(coverage_query).fetchdf()
    mo.ui.table(coverage_df)
    return coverage_df, coverage_query


@app.cell
def __(mo):
    mo.md("## Select Screening Scenario")
    return


@app.cell
def __(mo):
    scenario = mo.ui.dropdown(
        options=["Residential", "Industrial/Commercial"],
        value="Residential",
        label="Land Use Scenario:"
    )
    scenario
    return scenario,


@app.cell
def __(mo):
    matrix_filter = mo.ui.dropdown(
        options=["All", "Soil", "Groundwater"],
        value="All",
        label="Matrix:"
    )
    matrix_filter
    return matrix_filter,


@app.cell
def __(mo):
    mo.md("## Screening Results Summary")
    return


@app.cell
def __(conn, matrix_filter, mo, scenario):
    # Build dynamic query based on selections
    rsl_column = "rsl_residential_soil_mg_kg" if scenario.value == "Residential" else "rsl_industrial_soil_mg_kg"

    matrix_clause = ""
    if matrix_filter.value == "Soil":
        matrix_clause = "AND s.matrix_code = 'SO'"
    elif matrix_filter.value == "Groundwater":
        matrix_clause = "AND s.matrix_code = 'GW'"

    screening_query = f"""
    WITH screening AS (
        SELECT
            r.sample_id,
            s.location_id,
            l.location_name,
            s.sample_date,
            s.matrix_code,
            m.matrix_name,
            a.analyte_name,
            a.analyte_group,
            r.cas_rn,
            r.result_value,
            r.result_unit,
            r.detect_flag,
            r.lab_qualifier,
            CASE s.matrix_code
                WHEN 'SO' THEN sl.{rsl_column}
                WHEN 'GW' THEN sl.rsl_residential_tap_ug_l
                ELSE NULL
            END as screening_level,
            sl.carcinogen
        FROM fact_results r
        JOIN fact_samples s ON r.sample_id = s.sample_id
        LEFT JOIN dim_locations l ON s.location_id = l.location_id
        LEFT JOIN dim_matrix m ON s.matrix_code = m.matrix_code
        LEFT JOIN dim_analytes a ON r.cas_rn = a.cas_rn
        LEFT JOIN ref_screening_levels sl ON r.cas_rn = sl.cas_rn
        WHERE r.detect_flag = 'Y'
        {matrix_clause}
    )
    SELECT
        analyte_name,
        analyte_group,
        matrix_name,
        COUNT(*) as detections,
        MIN(result_value) as min_conc,
        MAX(result_value) as max_conc,
        result_unit,
        screening_level,
        ROUND(MAX(result_value) / NULLIF(screening_level, 0), 2) as max_hq,
        CASE
            WHEN MAX(result_value) > screening_level THEN 'EXCEEDS'
            ELSE 'BELOW'
        END as status,
        carcinogen
    FROM screening
    WHERE screening_level IS NOT NULL
    GROUP BY analyte_name, analyte_group, matrix_name, result_unit, screening_level, carcinogen
    ORDER BY max_hq DESC NULLS LAST
    """

    screening_df = conn.execute(screening_query).fetchdf()

    # Summary counts
    total_analytes = len(screening_df)
    exceeds_count = len(screening_df[screening_df['status'] == 'EXCEEDS']) if 'status' in screening_df.columns else 0

    mo.md(f"""
    ### Screening Summary ({scenario.value})

    - **Analytes Evaluated**: {total_analytes}
    - **Exceeding Screening Levels**: {exceeds_count}
    - **Below Screening Levels**: {total_analytes - exceeds_count}
    """)
    return (
        exceeds_count,
        matrix_clause,
        rsl_column,
        screening_df,
        screening_query,
        total_analytes,
    )


@app.cell
def __(mo, screening_df):
    mo.ui.table(screening_df, selection=None)
    return


@app.cell
def __(mo):
    mo.md(
        r"""
        ## Chemicals of Potential Concern (COPCs)

        Analytes with at least one exceedance of screening levels.
        These require further evaluation in risk assessment.
        """
    )
    return


@app.cell
def __(conn, matrix_clause, mo, rsl_column):
    copc_query = f"""
    WITH screening AS (
        SELECT
            r.cas_rn,
            a.analyte_name,
            a.analyte_group,
            s.matrix_code,
            m.matrix_name,
            r.result_value,
            CASE s.matrix_code
                WHEN 'SO' THEN sl.{rsl_column}
                WHEN 'GW' THEN sl.rsl_residential_tap_ug_l
            END as screening_level,
            sl.carcinogen,
            sl.target_organ
        FROM fact_results r
        JOIN fact_samples s ON r.sample_id = s.sample_id
        LEFT JOIN dim_matrix m ON s.matrix_code = m.matrix_code
        LEFT JOIN dim_analytes a ON r.cas_rn = a.cas_rn
        LEFT JOIN ref_screening_levels sl ON r.cas_rn = sl.cas_rn
        WHERE r.detect_flag = 'Y'
        AND sl.cas_rn IS NOT NULL
        {matrix_clause}
    )
    SELECT
        cas_rn,
        analyte_name,
        analyte_group,
        matrix_name,
        COUNT(*) as exceedance_count,
        MAX(result_value / NULLIF(screening_level, 0)) as max_hq,
        carcinogen,
        target_organ
    FROM screening
    WHERE result_value > screening_level
    GROUP BY cas_rn, analyte_name, analyte_group, matrix_name, carcinogen, target_organ
    ORDER BY max_hq DESC
    """

    copc_df = conn.execute(copc_query).fetchdf()

    if len(copc_df) > 0:
        mo.md(f"### Identified {len(copc_df)} COPCs")
    else:
        mo.md("### No COPCs identified - all results below screening levels")
    return copc_df, copc_query


@app.cell
def __(copc_df, mo):
    if len(copc_df) > 0:
        mo.ui.table(copc_df)
    return


@app.cell
def __(mo):
    mo.md("## Exceedance Details by Location")
    return


@app.cell
def __(conn, matrix_clause, mo, rsl_column):
    location_exceed_query = f"""
    SELECT
        s.location_id,
        l.location_name,
        l.location_type,
        m.matrix_name,
        a.analyte_name,
        r.result_value,
        r.result_unit,
        CASE s.matrix_code
            WHEN 'SO' THEN sl.{rsl_column}
            WHEN 'GW' THEN sl.rsl_residential_tap_ug_l
        END as screening_level,
        ROUND(r.result_value / NULLIF(
            CASE s.matrix_code
                WHEN 'SO' THEN sl.{rsl_column}
                WHEN 'GW' THEN sl.rsl_residential_tap_ug_l
            END, 0), 2) as hazard_quotient,
        s.sample_date,
        s.depth_top_ft,
        s.depth_bottom_ft
    FROM fact_results r
    JOIN fact_samples s ON r.sample_id = s.sample_id
    LEFT JOIN dim_locations l ON s.location_id = l.location_id
    LEFT JOIN dim_matrix m ON s.matrix_code = m.matrix_code
    LEFT JOIN dim_analytes a ON r.cas_rn = a.cas_rn
    LEFT JOIN ref_screening_levels sl ON r.cas_rn = sl.cas_rn
    WHERE r.detect_flag = 'Y'
    AND r.result_value > CASE s.matrix_code
        WHEN 'SO' THEN sl.{rsl_column}
        WHEN 'GW' THEN sl.rsl_residential_tap_ug_l
    END
    {matrix_clause}
    ORDER BY hazard_quotient DESC
    """

    location_exceed_df = conn.execute(location_exceed_query).fetchdf()
    mo.md(f"### All Exceedances ({len(location_exceed_df)} results)")
    return location_exceed_df, location_exceed_query


@app.cell
def __(location_exceed_df, mo):
    if len(location_exceed_df) > 0:
        mo.ui.table(location_exceed_df)
    else:
        mo.md("_No exceedances found_")
    return


@app.cell
def __(mo):
    mo.md("## Hazard Index by Location")
    return


@app.cell
def __(conn, mo, rsl_column):
    # Calculate cumulative hazard index by location
    hi_query = f"""
    WITH hq_calc AS (
        SELECT
            s.location_id,
            l.location_name,
            s.matrix_code,
            r.cas_rn,
            a.analyte_name,
            sl.target_organ,
            r.result_value / NULLIF(
                CASE s.matrix_code
                    WHEN 'SO' THEN sl.{rsl_column}
                    WHEN 'GW' THEN sl.rsl_residential_tap_ug_l
                END, 0) as hq
        FROM fact_results r
        JOIN fact_samples s ON r.sample_id = s.sample_id
        LEFT JOIN dim_locations l ON s.location_id = l.location_id
        LEFT JOIN dim_analytes a ON r.cas_rn = a.cas_rn
        LEFT JOIN ref_screening_levels sl ON r.cas_rn = sl.cas_rn
        WHERE r.detect_flag = 'Y'
        AND sl.cas_rn IS NOT NULL
    )
    SELECT
        location_id,
        location_name,
        matrix_code,
        target_organ,
        COUNT(DISTINCT cas_rn) as analyte_count,
        ROUND(SUM(hq), 3) as hazard_index,
        CASE
            WHEN SUM(hq) > 1 THEN 'EXCEEDS HI=1'
            ELSE 'BELOW HI=1'
        END as status
    FROM hq_calc
    WHERE hq IS NOT NULL
    GROUP BY location_id, location_name, matrix_code, target_organ
    HAVING SUM(hq) > 0.1
    ORDER BY hazard_index DESC
    """

    hi_df = conn.execute(hi_query).fetchdf()
    mo.md("""
    ### Cumulative Hazard Index by Target Organ

    When multiple chemicals affect the same target organ, their HQs should be summed.
    Hazard Index (HI) > 1 indicates potential concern.
    """)
    return hi_df, hi_query


@app.cell
def __(hi_df, mo):
    if len(hi_df) > 0:
        mo.ui.table(hi_df)
    else:
        mo.md("_No significant hazard indices calculated_")
    return


@app.cell
def __(mo):
    mo.md(
        r"""
        ## Next Steps

        - **era_03_statistics.py** - Statistical analysis (UCL95, ProUCL methods)
        - **era_04_reports.py** - Generate formal ERA tables and reports

        ### Notes on Screening Results

        1. **Exceedances ≠ Risk**: Exceeding a screening level triggers further evaluation,
           not automatic determination of unacceptable risk
        2. **Background Comparison**: Compare to background locations to distinguish
           site-related vs. naturally occurring concentrations
        3. **Exposure Pathway Evaluation**: Consider whether complete exposure pathways exist
        """
    )
    return


if __name__ == "__main__":
    app.run()
