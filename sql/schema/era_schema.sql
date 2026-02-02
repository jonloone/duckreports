-- ============================================
-- Environmental Risk Assessment (ERA) Schema
-- Based on EQuIS EDD and EPA standards
-- ============================================

-- ============================================
-- DIMENSION TABLES
-- ============================================

-- Sampling Locations (monitoring wells, soil borings, etc.)
CREATE TABLE IF NOT EXISTS dim_locations (
    location_id VARCHAR PRIMARY KEY,
    location_name VARCHAR,
    location_type VARCHAR,          -- MW, SB, SS, SW, SD (well, boring, surface soil, surface water, sediment)
    latitude DECIMAL(10,6),
    longitude DECIMAL(10,6),
    elevation_ft DECIMAL(8,2),
    total_depth_ft DECIMAL(6,2),
    screen_top_ft DECIMAL(6,2),     -- For wells
    screen_bottom_ft DECIMAL(6,2),
    install_date DATE,
    status VARCHAR DEFAULT 'Active' -- Active, Abandoned, Destroyed
);

-- Analytes/Parameters (chemicals being tested)
CREATE TABLE IF NOT EXISTS dim_analytes (
    cas_rn VARCHAR PRIMARY KEY,     -- CAS Registry Number (use 'SW' prefix for non-CAS like pH)
    analyte_name VARCHAR NOT NULL,
    analyte_group VARCHAR,          -- VOC, SVOC, Metal, Pesticide, PCB, PFAS, General
    chemical_formula VARCHAR,
    molecular_weight DECIMAL(10,4)
);

-- Matrix codes (sample media types)
CREATE TABLE IF NOT EXISTS dim_matrix (
    matrix_code VARCHAR PRIMARY KEY,
    matrix_name VARCHAR,
    matrix_description VARCHAR
);

-- Insert standard matrix codes
INSERT OR REPLACE INTO dim_matrix VALUES
    ('SO', 'Soil', 'Surface or subsurface soil sample'),
    ('GW', 'Groundwater', 'Groundwater sample from monitoring well'),
    ('SE', 'Sediment', 'Stream, lake, or pond sediment'),
    ('SW', 'Surface Water', 'River, stream, lake, or pond water'),
    ('AI', 'Air Indoor', 'Indoor air sample'),
    ('AO', 'Air Outdoor', 'Ambient outdoor air sample'),
    ('SG', 'Soil Gas', 'Subsurface soil vapor sample'),
    ('WW', 'Wastewater', 'Industrial or sanitary wastewater'),
    ('DW', 'Drinking Water', 'Potable water supply sample'),
    ('WI', 'Wipe', 'Surface wipe sample'),
    ('TI', 'Tissue', 'Biological tissue sample');

-- Lab qualifiers and their meanings
CREATE TABLE IF NOT EXISTS dim_qualifiers (
    qualifier VARCHAR PRIMARY KEY,
    qualifier_name VARCHAR,
    description VARCHAR,
    use_result BOOLEAN,             -- Should result be used in calculations?
    detection_status VARCHAR        -- Detected, Non-Detect, Estimated
);

-- Insert standard lab qualifiers
INSERT OR REPLACE INTO dim_qualifiers VALUES
    ('', 'None', 'No qualification - detected result', TRUE, 'Detected'),
    ('U', 'Non-Detect', 'Analyte not detected above detection limit', TRUE, 'Non-Detect'),
    ('J', 'Estimated', 'Result is estimated (detected but below quantitation limit)', TRUE, 'Estimated'),
    ('UJ', 'Est. Non-Detect', 'Detection limit is estimated', TRUE, 'Non-Detect'),
    ('B', 'Blank Contamination', 'Analyte found in method blank', TRUE, 'Detected'),
    ('R', 'Rejected', 'Result rejected due to QC failure', FALSE, 'Rejected'),
    ('N', 'Tentative ID', 'Tentative identification', TRUE, 'Estimated'),
    ('E', 'Exceeded Cal', 'Result exceeded calibration range', TRUE, 'Detected'),
    ('D', 'Diluted', 'Sample was diluted', TRUE, 'Detected');

-- ============================================
-- REFERENCE DATA TABLES
-- ============================================

-- EPA Regional Screening Levels (RSLs)
-- Updated semi-annually - this is a subset for common analytes
CREATE TABLE IF NOT EXISTS ref_screening_levels (
    cas_rn VARCHAR,
    analyte_name VARCHAR,
    rsl_residential_soil_mg_kg DECIMAL(15,6),
    rsl_industrial_soil_mg_kg DECIMAL(15,6),
    rsl_residential_tap_ug_l DECIMAL(15,6),
    rsl_mcl_ug_l DECIMAL(15,6),              -- Maximum Contaminant Level
    eco_ssl_plants_mg_kg DECIMAL(15,6),
    eco_ssl_soil_inverts_mg_kg DECIMAL(15,6),
    eco_ssl_avian_mg_kg DECIMAL(15,6),
    eco_ssl_mammalian_mg_kg DECIMAL(15,6),
    carcinogen VARCHAR,                       -- Yes, No, Pending
    target_organ VARCHAR,
    update_date DATE,
    PRIMARY KEY (cas_rn)
);

-- ============================================
-- FACT TABLES
-- ============================================

-- Sample collection events
CREATE TABLE IF NOT EXISTS fact_samples (
    sample_id VARCHAR PRIMARY KEY,
    location_id VARCHAR REFERENCES dim_locations(location_id),
    sample_date DATE NOT NULL,
    sample_time TIME,
    matrix_code VARCHAR REFERENCES dim_matrix(matrix_code),
    sample_type VARCHAR DEFAULT 'N',  -- N=Normal, FD=Field Dup, TB=Trip Blank, EB=Equip Blank, MS=Matrix Spike
    depth_top_ft DECIMAL(6,2),
    depth_bottom_ft DECIMAL(6,2),
    sample_method VARCHAR,
    sampler_name VARCHAR,
    field_notes TEXT,
    lab_name VARCHAR,
    lab_sample_id VARCHAR
);

-- Analytical results (core fact table)
CREATE TABLE IF NOT EXISTS fact_results (
    result_id INTEGER PRIMARY KEY,
    sample_id VARCHAR REFERENCES fact_samples(sample_id),
    cas_rn VARCHAR REFERENCES dim_analytes(cas_rn),
    result_value DECIMAL(15,6),
    result_unit VARCHAR NOT NULL,
    detection_limit DECIMAL(15,6),
    quantitation_limit DECIMAL(15,6),
    detect_flag VARCHAR(1) DEFAULT 'Y',  -- Y=Detected, N=Non-Detect
    lab_qualifier VARCHAR(10),
    dilution_factor DECIMAL(8,2) DEFAULT 1,
    analysis_method VARCHAR,
    analysis_date DATE,
    prep_method VARCHAR,
    prep_date DATE,
    basis VARCHAR DEFAULT 'Wet',         -- Wet, Dry (for soils)
    percent_moisture DECIMAL(5,2),
    validation_qualifier VARCHAR(10),
    validated_by VARCHAR,
    validation_date DATE
);

-- Field measurements (pH, conductivity, turbidity, etc.)
CREATE TABLE IF NOT EXISTS fact_field_measurements (
    measurement_id INTEGER PRIMARY KEY,
    sample_id VARCHAR REFERENCES fact_samples(sample_id),
    parameter_code VARCHAR,
    parameter_name VARCHAR,
    result_value DECIMAL(12,4),
    result_unit VARCHAR,
    measurement_time TIME,
    instrument_id VARCHAR,
    notes TEXT
);

-- ============================================
-- ANALYTICAL VIEWS
-- ============================================

-- View: Results with screening level comparison
CREATE OR REPLACE VIEW vw_screening_comparison AS
SELECT
    s.sample_id,
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
    r.detection_limit,
    r.detect_flag,
    r.lab_qualifier,
    q.detection_status,
    -- Screening levels based on matrix
    CASE s.matrix_code
        WHEN 'SO' THEN sl.rsl_residential_soil_mg_kg
        WHEN 'GW' THEN sl.rsl_residential_tap_ug_l
        WHEN 'DW' THEN sl.rsl_residential_tap_ug_l
    END as screening_level,
    -- Hazard Quotient calculation
    CASE
        WHEN s.matrix_code = 'SO' AND sl.rsl_residential_soil_mg_kg > 0
        THEN ROUND(r.result_value / sl.rsl_residential_soil_mg_kg, 4)
        WHEN s.matrix_code IN ('GW', 'DW') AND sl.rsl_residential_tap_ug_l > 0
        THEN ROUND(r.result_value / sl.rsl_residential_tap_ug_l, 4)
    END as hazard_quotient,
    -- Exceedance flag
    CASE
        WHEN s.matrix_code = 'SO' AND r.result_value > sl.rsl_residential_soil_mg_kg THEN 'EXCEEDS'
        WHEN s.matrix_code IN ('GW', 'DW') AND r.result_value > sl.rsl_residential_tap_ug_l THEN 'EXCEEDS'
        ELSE 'BELOW'
    END as screening_status,
    sl.carcinogen
FROM fact_results r
JOIN fact_samples s ON r.sample_id = s.sample_id
LEFT JOIN dim_locations l ON s.location_id = l.location_id
LEFT JOIN dim_matrix m ON s.matrix_code = m.matrix_code
LEFT JOIN dim_analytes a ON r.cas_rn = a.cas_rn
LEFT JOIN dim_qualifiers q ON r.lab_qualifier = q.qualifier
LEFT JOIN ref_screening_levels sl ON r.cas_rn = sl.cas_rn;

-- View: Detection summary by analyte
CREATE OR REPLACE VIEW vw_detection_summary AS
SELECT
    a.analyte_name,
    a.analyte_group,
    m.matrix_name,
    COUNT(*) as total_samples,
    SUM(CASE WHEN r.detect_flag = 'Y' THEN 1 ELSE 0 END) as detected_count,
    ROUND(100.0 * SUM(CASE WHEN r.detect_flag = 'Y' THEN 1 ELSE 0 END) / COUNT(*), 1) as detection_frequency_pct,
    MIN(CASE WHEN r.detect_flag = 'Y' THEN r.result_value END) as min_detected,
    MAX(CASE WHEN r.detect_flag = 'Y' THEN r.result_value END) as max_detected,
    AVG(CASE WHEN r.detect_flag = 'Y' THEN r.result_value END) as mean_detected,
    r.result_unit
FROM fact_results r
JOIN fact_samples s ON r.sample_id = s.sample_id
JOIN dim_analytes a ON r.cas_rn = a.cas_rn
JOIN dim_matrix m ON s.matrix_code = m.matrix_code
GROUP BY a.analyte_name, a.analyte_group, m.matrix_name, r.result_unit;

-- View: Location summary with max concentrations
CREATE OR REPLACE VIEW vw_location_summary AS
SELECT
    l.location_id,
    l.location_name,
    l.location_type,
    m.matrix_name,
    COUNT(DISTINCT s.sample_id) as sample_count,
    MIN(s.sample_date) as first_sample,
    MAX(s.sample_date) as last_sample,
    COUNT(DISTINCT r.cas_rn) as analytes_tested,
    SUM(CASE WHEN r.detect_flag = 'Y' THEN 1 ELSE 0 END) as total_detections
FROM dim_locations l
JOIN fact_samples s ON l.location_id = s.location_id
JOIN dim_matrix m ON s.matrix_code = m.matrix_code
LEFT JOIN fact_results r ON s.sample_id = r.sample_id
GROUP BY l.location_id, l.location_name, l.location_type, m.matrix_name;

-- View: Exceedances only (for rapid screening)
CREATE OR REPLACE VIEW vw_exceedances AS
SELECT * FROM vw_screening_comparison
WHERE screening_status = 'EXCEEDS'
ORDER BY hazard_quotient DESC;

-- View: Chemicals of Potential Concern (COPCs)
-- Detected above screening levels at least once
CREATE OR REPLACE VIEW vw_copcs AS
SELECT DISTINCT
    cas_rn,
    analyte_name,
    analyte_group,
    matrix_name,
    MAX(hazard_quotient) as max_hq,
    COUNT(*) as exceedance_count
FROM vw_screening_comparison
WHERE screening_status = 'EXCEEDS'
GROUP BY cas_rn, analyte_name, analyte_group, matrix_name
ORDER BY max_hq DESC;
