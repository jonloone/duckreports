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
        # ERA 03 - Statistical Analysis

        Calculate exposure point concentrations (EPCs) using EPA-recommended methods.

        ## Key Calculations
        - **Detection Frequency**: % of samples with detectable concentrations
        - **UCL95**: 95% Upper Confidence Limit on the mean (ProUCL methods)
        - **Non-Detect Handling**: Kaplan-Meier, ROS, or substitution methods

        ## EPA Guidance
        Per RAGS (Risk Assessment Guidance for Superfund), the 95% UCL is the
        recommended EPC for reasonable maximum exposure (RME) scenarios.
        """
    )
    return


@app.cell
def __():
    import duckdb
    from pathlib import Path
    import pandas as pd
    import numpy as np
    from scipy import stats
    return Path, duckdb, np, pd, stats


@app.cell
def __(Path, duckdb):
    PROJECT_ROOT = Path(__file__).parent.parent
    DB_PATH = PROJECT_ROOT / "data" / "processed" / "analytics.duckdb"
    conn = duckdb.connect(str(DB_PATH))
    return DB_PATH, PROJECT_ROOT, conn


@app.cell
def __(mo):
    mo.md("## Detection Summary by Analyte")
    return


@app.cell
def __(conn, mo):
    detection_query = """
    SELECT
        a.analyte_name,
        a.analyte_group,
        m.matrix_name,
        COUNT(*) as n_samples,
        SUM(CASE WHEN r.detect_flag = 'Y' THEN 1 ELSE 0 END) as n_detect,
        SUM(CASE WHEN r.detect_flag = 'N' THEN 1 ELSE 0 END) as n_nondetect,
        ROUND(100.0 * SUM(CASE WHEN r.detect_flag = 'Y' THEN 1 ELSE 0 END) / COUNT(*), 1) as detect_freq_pct,
        r.result_unit
    FROM fact_results r
    JOIN fact_samples s ON r.sample_id = s.sample_id
    LEFT JOIN dim_analytes a ON r.cas_rn = a.cas_rn
    LEFT JOIN dim_matrix m ON s.matrix_code = m.matrix_code
    GROUP BY a.analyte_name, a.analyte_group, m.matrix_name, r.result_unit
    ORDER BY detect_freq_pct DESC, n_samples DESC
    """

    detection_df = conn.execute(detection_query).fetchdf()
    mo.md(f"### Detection Frequency for {len(detection_df)} Analyte/Matrix Combinations")
    return detection_df, detection_query


@app.cell
def __(detection_df, mo):
    mo.ui.table(detection_df)
    return


@app.cell
def __(mo):
    mo.md(
        r"""
        ## Non-Detect Handling Methods

        EPA ProUCL guidance recommends:

        | Detection Frequency | Recommended Method |
        |--------------------|--------------------|
        | > 80% detects | Standard parametric (Student's t) |
        | 50-80% detects | Kaplan-Meier or ROS |
        | < 50% detects | Use max detect or 1/2 max DL |
        | All non-detects | Use 1/2 max detection limit |
        """
    )
    return


@app.cell
def __(mo):
    analyte_select = mo.ui.dropdown(
        options=[],  # Will be populated
        label="Select Analyte for Analysis:",
        value=None
    )
    return analyte_select,


@app.cell
def __(conn, mo):
    # Get list of analytes with sufficient data
    analyte_list_query = """
    SELECT DISTINCT
        a.analyte_name || ' (' || m.matrix_name || ')' as display_name,
        r.cas_rn,
        s.matrix_code
    FROM fact_results r
    JOIN fact_samples s ON r.sample_id = s.sample_id
    LEFT JOIN dim_analytes a ON r.cas_rn = a.cas_rn
    LEFT JOIN dim_matrix m ON s.matrix_code = m.matrix_code
    GROUP BY a.analyte_name, m.matrix_name, r.cas_rn, s.matrix_code
    HAVING COUNT(*) >= 4
    ORDER BY a.analyte_name
    """
    analyte_options = conn.execute(analyte_list_query).fetchdf()

    # Create selector
    if len(analyte_options) > 0:
        options_list = analyte_options['display_name'].tolist()
        analyte_selector = mo.ui.dropdown(
            options=options_list,
            value=options_list[0],
            label="Select Analyte:"
        )
        mo.hstack([analyte_selector])
    else:
        analyte_selector = None
        mo.md("_No analytes with sufficient data for statistics_")
    return analyte_list_query, analyte_options, analyte_selector, options_list


@app.cell
def __(analyte_options, analyte_selector, conn, mo, np, pd, stats):
    if analyte_selector is not None and analyte_selector.value:
        # Parse selection
        selected = analyte_selector.value
        analyte_name = selected.split(' (')[0]
        matrix_name = selected.split('(')[1].rstrip(')')

        # Get data for selected analyte
        data_query = f"""
        SELECT
            r.result_value,
            r.detection_limit,
            r.detect_flag,
            r.lab_qualifier,
            s.sample_date,
            s.location_id
        FROM fact_results r
        JOIN fact_samples s ON r.sample_id = s.sample_id
        LEFT JOIN dim_analytes a ON r.cas_rn = a.cas_rn
        LEFT JOIN dim_matrix m ON s.matrix_code = m.matrix_code
        WHERE a.analyte_name = '{analyte_name}'
        AND m.matrix_name = '{matrix_name}'
        ORDER BY s.sample_date
        """
        data_df = conn.execute(data_query).fetchdf()

        # Calculate statistics
        n_total = len(data_df)
        n_detect = len(data_df[data_df['detect_flag'] == 'Y'])
        n_nondetect = n_total - n_detect
        detect_freq = 100.0 * n_detect / n_total if n_total > 0 else 0

        # Get detected values
        detected_values = data_df[data_df['detect_flag'] == 'Y']['result_value'].values

        # Get detection limits for non-detects
        dl_values = data_df[data_df['detect_flag'] == 'N']['detection_limit'].values

        # Calculate basic statistics on detects
        if len(detected_values) > 0:
            min_detect = np.min(detected_values)
            max_detect = np.max(detected_values)
            mean_detect = np.mean(detected_values)
            std_detect = np.std(detected_values, ddof=1) if len(detected_values) > 1 else 0
        else:
            min_detect = max_detect = mean_detect = std_detect = None

        # Determine recommended method based on detection frequency
        if detect_freq >= 80:
            method = "Student's t-UCL"
            # Simple substitution: use 1/2 DL for non-detects
            all_values = list(detected_values)
            for dl in dl_values:
                all_values.append(dl / 2)
            all_values = np.array(all_values)

            if len(all_values) > 1:
                mean_val = np.mean(all_values)
                std_val = np.std(all_values, ddof=1)
                se = std_val / np.sqrt(len(all_values))
                t_crit = stats.t.ppf(0.95, len(all_values) - 1)
                ucl95 = mean_val + t_crit * se
            else:
                mean_val = all_values[0] if len(all_values) > 0 else None
                ucl95 = mean_val
        elif detect_freq >= 50:
            method = "Kaplan-Meier (simplified)"
            # For this demo, using simple substitution
            all_values = list(detected_values)
            for dl in dl_values:
                all_values.append(dl / 2)
            all_values = np.array(all_values)

            if len(all_values) > 1:
                mean_val = np.mean(all_values)
                std_val = np.std(all_values, ddof=1)
                se = std_val / np.sqrt(len(all_values))
                ucl95 = mean_val + 1.645 * se  # Using z for larger samples
            else:
                mean_val = all_values[0] if len(all_values) > 0 else None
                ucl95 = mean_val
        elif detect_freq > 0:
            method = "Max Detect"
            mean_val = np.mean(detected_values) if len(detected_values) > 0 else None
            ucl95 = max_detect
        else:
            method = "1/2 Max Detection Limit"
            mean_val = None
            ucl95 = np.max(dl_values) / 2 if len(dl_values) > 0 else None

        mo.md(f"""
        ## Statistical Analysis: {selected}

        ### Dataset Summary
        | Metric | Value |
        |--------|-------|
        | Total Samples | {n_total} |
        | Detects | {n_detect} |
        | Non-Detects | {n_nondetect} |
        | Detection Frequency | {detect_freq:.1f}% |

        ### Detected Values
        | Statistic | Value |
        |-----------|-------|
        | Minimum | {min_detect if min_detect else 'N/A'} |
        | Maximum | {max_detect if max_detect else 'N/A'} |
        | Mean | {round(mean_detect, 4) if mean_detect else 'N/A'} |
        | Std Dev | {round(std_detect, 4) if std_detect else 'N/A'} |

        ### Exposure Point Concentration (EPC)
        | Method | UCL95 |
        |--------|-------|
        | {method} | **{round(ucl95, 4) if ucl95 else 'N/A'}** |
        """)
    return (
        all_values,
        analyte_name,
        data_df,
        data_query,
        detect_freq,
        detected_values,
        dl,
        dl_values,
        max_detect,
        matrix_name,
        mean_detect,
        mean_val,
        method,
        min_detect,
        n_detect,
        n_nondetect,
        n_total,
        se,
        selected,
        std_detect,
        std_val,
        t_crit,
        ucl95,
    )


@app.cell
def __(mo):
    mo.md("## EPC Summary Table (All COPCs)")
    return


@app.cell
def __(conn, mo, np, stats):
    # Calculate EPCs for all analytes with exceedances
    epc_query = """
    WITH copc_list AS (
        SELECT DISTINCT r.cas_rn, s.matrix_code
        FROM fact_results r
        JOIN fact_samples s ON r.sample_id = s.sample_id
        LEFT JOIN ref_screening_levels sl ON r.cas_rn = sl.cas_rn
        WHERE r.detect_flag = 'Y'
        AND (
            (s.matrix_code = 'SO' AND r.result_value > sl.rsl_residential_soil_mg_kg)
            OR (s.matrix_code = 'GW' AND r.result_value > sl.rsl_residential_tap_ug_l)
        )
    )
    SELECT
        a.analyte_name,
        m.matrix_name,
        r.cas_rn,
        s.matrix_code,
        r.result_value,
        r.detection_limit,
        r.detect_flag,
        r.result_unit,
        CASE s.matrix_code
            WHEN 'SO' THEN sl.rsl_residential_soil_mg_kg
            WHEN 'GW' THEN sl.rsl_residential_tap_ug_l
        END as screening_level
    FROM fact_results r
    JOIN fact_samples s ON r.sample_id = s.sample_id
    LEFT JOIN dim_analytes a ON r.cas_rn = a.cas_rn
    LEFT JOIN dim_matrix m ON s.matrix_code = m.matrix_code
    LEFT JOIN ref_screening_levels sl ON r.cas_rn = sl.cas_rn
    WHERE (r.cas_rn, s.matrix_code) IN (SELECT cas_rn, matrix_code FROM copc_list)
    ORDER BY a.analyte_name, s.matrix_code
    """

    raw_data = conn.execute(epc_query).fetchdf()

    # Calculate EPCs for each analyte/matrix combo
    epc_results = []

    for (cas, matrix), group in raw_data.groupby(['cas_rn', 'matrix_code']):
        analyte_name = group['analyte_name'].iloc[0]
        matrix_name = group['matrix_name'].iloc[0]
        unit = group['result_unit'].iloc[0]
        sl = group['screening_level'].iloc[0]

        n_total = len(group)
        detects = group[group['detect_flag'] == 'Y']
        n_detect = len(detects)
        detect_freq = 100.0 * n_detect / n_total

        detected_vals = detects['result_value'].values
        dl_vals = group[group['detect_flag'] == 'N']['detection_limit'].values

        # Calculate EPC
        if detect_freq >= 80 and n_detect >= 4:
            # Use t-UCL
            all_vals = np.concatenate([detected_vals, dl_vals / 2]) if len(dl_vals) > 0 else detected_vals
            mean_v = np.mean(all_vals)
            std_v = np.std(all_vals, ddof=1)
            se = std_v / np.sqrt(len(all_vals))
            t_crit = stats.t.ppf(0.95, len(all_vals) - 1)
            epc = mean_v + t_crit * se
            method = "t-UCL95"
        elif detect_freq >= 50:
            all_vals = np.concatenate([detected_vals, dl_vals / 2]) if len(dl_vals) > 0 else detected_vals
            epc = np.mean(all_vals) + 1.645 * np.std(all_vals, ddof=1) / np.sqrt(len(all_vals))
            method = "z-UCL95"
        elif n_detect > 0:
            epc = np.max(detected_vals)
            method = "Max Detect"
        else:
            epc = np.max(dl_vals) / 2 if len(dl_vals) > 0 else None
            method = "1/2 DL"

        hq = epc / sl if epc and sl else None

        epc_results.append({
            'Analyte': analyte_name,
            'Matrix': matrix_name,
            'N': n_total,
            'Detect %': f"{detect_freq:.0f}%",
            'EPC': round(epc, 4) if epc else None,
            'Unit': unit,
            'Method': method,
            'RSL': sl,
            'HQ': round(hq, 2) if hq else None
        })

    import pandas as pd
    epc_df = pd.DataFrame(epc_results)
    mo.md(f"### Exposure Point Concentrations for {len(epc_df)} COPCs")
    return (
        all_vals,
        analyte_name,
        cas,
        detect_freq,
        detected_vals,
        detects,
        dl_vals,
        epc,
        epc_df,
        epc_query,
        epc_results,
        group,
        hq,
        matrix,
        matrix_name,
        mean_v,
        method,
        n_detect,
        n_total,
        pd,
        raw_data,
        se,
        sl,
        std_v,
        t_crit,
        unit,
    )


@app.cell
def __(epc_df, mo):
    if len(epc_df) > 0:
        mo.ui.table(epc_df)
    else:
        mo.md("_No COPCs identified requiring EPC calculation_")
    return


@app.cell
def __(mo):
    mo.md(
        r"""
        ## Background Comparison

        Compare site concentrations to background/upgradient locations.
        """
    )
    return


@app.cell
def __(conn, mo):
    background_query = """
    WITH site_data AS (
        SELECT
            a.analyte_name,
            m.matrix_name,
            CASE WHEN l.location_name LIKE '%Background%' THEN 'Background' ELSE 'Site' END as area,
            r.result_value,
            r.detect_flag
        FROM fact_results r
        JOIN fact_samples s ON r.sample_id = s.sample_id
        LEFT JOIN dim_locations l ON s.location_id = l.location_id
        LEFT JOIN dim_analytes a ON r.cas_rn = a.cas_rn
        LEFT JOIN dim_matrix m ON s.matrix_code = m.matrix_code
        WHERE r.detect_flag = 'Y'
    )
    SELECT
        analyte_name,
        matrix_name,
        area,
        COUNT(*) as n,
        MIN(result_value) as min_val,
        MAX(result_value) as max_val,
        ROUND(AVG(result_value), 4) as mean_val
    FROM site_data
    GROUP BY analyte_name, matrix_name, area
    ORDER BY analyte_name, matrix_name, area
    """

    background_df = conn.execute(background_query).fetchdf()
    mo.md("### Site vs Background Concentrations")
    return background_df, background_query


@app.cell
def __(background_df, mo):
    mo.ui.table(background_df)
    return


@app.cell
def __(mo):
    mo.md(
        r"""
        ## Notes on Statistical Methods

        ### ProUCL Software
        For production ERA work, use **EPA's ProUCL software** for:
        - Proper Kaplan-Meier estimates for censored data
        - Regression on Order Statistics (ROS)
        - Goodness-of-fit tests
        - Appropriate UCL method selection

        Download: https://www.epa.gov/land-research/proucl-software

        ### This Implementation
        This notebook provides simplified calculations suitable for:
        - Screening-level assessments
        - Data exploration
        - Preliminary risk evaluation

        For formal risk assessments, export data and use ProUCL.

        ## Next Steps

        - **era_04_reports.py** - Generate formal ERA summary tables
        """
    )
    return


if __name__ == "__main__":
    app.run()
