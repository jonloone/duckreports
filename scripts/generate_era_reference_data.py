#!/usr/bin/env python3
"""
Generate EPA Regional Screening Levels (RSLs) reference data.
Based on EPA RSL Generic Tables (May 2024 version).

Note: These are representative values. For actual site work,
always download the latest RSLs from:
https://www.epa.gov/risk/regional-screening-levels-rsls-generic-tables

Usage:
    python scripts/generate_era_reference_data.py
"""

from pathlib import Path

try:
    import duckdb
except ImportError:
    print("Error: duckdb is required. Install with: pip install duckdb")
    exit(1)

PROJECT_ROOT = Path(__file__).parent.parent
DB_PATH = PROJECT_ROOT / "data" / "processed" / "analytics.duckdb"
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

# EPA RSL data for common contaminants (representative values)
# Source: EPA RSL Generic Tables - values simplified for demonstration
# Units: Soil in mg/kg, Water in ug/L

RSL_DATA = [
    # Metals
    ("7440-38-2", "Arsenic", 0.68, 3.0, 0.052, 10, 18, 32, None, 46, "Yes", "Multiple"),
    ("7440-39-3", "Barium", 15000, 220000, 2000, 2000, 330, 2000, None, None, "No", "Kidney"),
    ("7440-43-9", "Cadmium", 71, 980, 5, 5, 32, 140, 0.77, 0.36, "Yes", "Kidney"),
    ("7440-47-3", "Chromium III", 120000, 1500000, None, 100, None, None, None, None, "No", "None"),
    ("18540-29-9", "Chromium VI", 0.31, 6.3, None, 100, None, None, None, None, "Yes", "Lung"),
    ("7439-92-1", "Lead", 400, 800, 15, 15, None, None, None, None, "Yes", "Multiple"),
    ("7439-97-6", "Mercury", 11, 46, 2, 2, 0.3, 0.05, None, None, "No", "Kidney"),
    ("7440-02-0", "Nickel", 1500, 22000, 100, None, 38, 280, None, None, "Yes", "Lung"),
    ("7782-49-2", "Selenium", 390, 5700, 50, 50, 0.52, 4.1, 1.2, 0.63, "No", "Hair/Nail"),
    ("7440-66-6", "Zinc", 23000, 350000, 3000, None, 160, 120, 79, 320, "No", "Blood"),

    # VOCs (Volatile Organic Compounds)
    ("71-43-2", "Benzene", 1.2, 5.3, 0.46, 5, None, None, None, None, "Yes", "Blood"),
    ("100-41-4", "Ethylbenzene", 5.8, 25, 1.5, 700, None, None, None, None, "Yes", "Kidney"),
    ("108-88-3", "Toluene", 4700, 68000, 1000, 1000, None, None, None, None, "No", "Nervous"),
    ("1330-20-7", "Xylenes (Total)", 630, 2600, 190, 10000, None, None, None, None, "No", "Nervous"),
    ("75-09-2", "Methylene Chloride", 53, 630, 6.3, 5, None, None, None, None, "Yes", "Liver"),
    ("127-18-4", "Tetrachloroethylene (PCE)", 11, 49, 11, 5, None, None, None, None, "Yes", "Liver"),
    ("79-01-6", "Trichloroethylene (TCE)", 0.49, 2.2, 0.49, 5, None, None, None, None, "Yes", "Multiple"),
    ("75-01-4", "Vinyl Chloride", 0.021, 0.35, 0.019, 2, None, None, None, None, "Yes", "Liver"),
    ("67-66-3", "Chloroform", 0.12, 0.53, 0.098, 70, None, None, None, None, "Yes", "Liver"),
    ("56-23-5", "Carbon Tetrachloride", 0.47, 2.1, 0.35, 5, None, None, None, None, "Yes", "Liver"),
    ("107-06-2", "1,2-Dichloroethane", 0.36, 1.6, 0.38, 5, None, None, None, None, "Yes", "Multiple"),
    ("78-87-5", "1,2-Dichloropropane", 0.53, 2.4, 0.53, 5, None, None, None, None, "Yes", "Liver"),
    ("1634-04-4", "MTBE", 35, 520, 9.5, None, None, None, None, None, "Yes", "Kidney"),

    # SVOCs (Semi-Volatile Organic Compounds)
    ("91-20-3", "Naphthalene", 3.8, 17, 0.17, None, None, None, None, None, "Yes", "Nasal"),
    ("83-32-9", "Acenaphthene", 3400, 51000, 430, None, None, None, None, None, "No", "Liver"),
    ("120-12-7", "Anthracene", 17000, 250000, 2100, None, None, None, None, None, "No", "None"),
    ("50-32-8", "Benzo(a)pyrene", 0.11, 0.21, 0.0092, 0.2, None, None, None, None, "Yes", "Multiple"),
    ("206-44-0", "Fluoranthene", 2300, 34000, 290, None, None, None, None, None, "No", "Liver"),
    ("129-00-0", "Pyrene", 1700, 25000, 210, None, None, None, None, None, "No", "Kidney"),
    ("218-01-9", "Chrysene", 11, 21, 0.92, None, None, None, None, None, "Yes", "None"),
    ("85-01-8", "Phenanthrene", 17000, 250000, 2100, None, None, None, None, None, "No", "None"),

    # PCBs
    ("1336-36-3", "Aroclor 1254", 0.24, 0.74, 0.0074, 0.5, None, None, None, None, "Yes", "Liver"),
    ("11097-69-1", "Aroclor 1260", 0.24, 0.74, 0.0074, 0.5, None, None, None, None, "Yes", "Liver"),
    ("PCB-TOTAL", "PCBs (Total)", 0.24, 0.74, 0.0074, 0.5, 40, 40, 0.07, 0.07, "Yes", "Liver"),

    # PFAS (Per- and Polyfluoroalkyl Substances)
    ("335-67-1", "PFOA", 0.00062, 0.0092, 0.004, 0.004, None, None, None, None, "Yes", "Liver"),
    ("1763-23-1", "PFOS", 0.00031, 0.0046, 0.004, 0.004, None, None, None, None, "Yes", "Liver"),
    ("375-95-1", "PFNA", 0.00062, 0.0092, 0.004, None, None, None, None, None, "Yes", "Liver"),
    ("335-76-2", "PFDA", 0.00062, 0.0092, 0.004, None, None, None, None, None, "Yes", "Liver"),
    ("2058-94-8", "PFUnA", 0.0019, 0.028, 0.012, None, None, None, None, None, "Yes", "Liver"),
    ("307-55-1", "PFDoA", 0.00031, 0.0046, 0.002, None, None, None, None, None, "Yes", "Liver"),

    # Pesticides
    ("309-00-2", "Aldrin", 0.035, 0.15, 0.0011, None, None, None, None, None, "Yes", "Liver"),
    ("57-74-9", "Chlordane", 1.6, 7.1, 0.099, 2, None, None, None, 2.1, "Yes", "Liver"),
    ("72-54-8", "DDD", 2.0, 8.7, 0.15, None, None, None, None, None, "Yes", "Liver"),
    ("72-55-9", "DDE", 1.4, 6.3, 0.11, None, None, None, None, None, "Yes", "Liver"),
    ("50-29-3", "DDT", 1.4, 6.3, 0.11, None, None, None, 1.1, 0.7, "Yes", "Liver"),
    ("60-57-1", "Dieldrin", 0.033, 0.14, 0.0011, None, None, None, 0.005, 0.003, "Yes", "Liver"),
    ("76-44-8", "Heptachlor", 0.1, 0.44, 0.0035, 0.4, None, None, None, None, "Yes", "Liver"),
    ("1024-57-3", "Heptachlor Epoxide", 0.051, 0.22, 0.0018, 0.2, None, None, None, None, "Yes", "Liver"),

    # TPH Fractions (example screening levels)
    ("TPH-GRO", "TPH Gasoline Range", 100, 500, None, None, None, None, None, None, "No", "Multiple"),
    ("TPH-DRO", "TPH Diesel Range", 500, 2500, None, None, None, None, None, None, "No", "Multiple"),
    ("TPH-ORO", "TPH Oil Range", 1000, 5000, None, None, None, None, None, None, "No", "Multiple"),
]

# Analyte groups mapping
ANALYTE_GROUPS = {
    "7440": "Metal",
    "7439": "Metal",
    "7782": "Metal",
    "18540": "Metal",
    "71-43": "VOC", "100-41": "VOC", "108-88": "VOC", "1330-20": "VOC",
    "75-": "VOC", "79-01": "VOC", "127-18": "VOC", "67-66": "VOC",
    "56-23": "VOC", "107-06": "VOC", "78-87": "VOC", "1634": "VOC",
    "91-20": "SVOC", "83-32": "SVOC", "120-12": "SVOC", "50-32": "SVOC",
    "206-44": "SVOC", "129-00": "SVOC", "218-01": "SVOC", "85-01": "SVOC",
    "1336": "PCB", "11097": "PCB", "PCB": "PCB",
    "335-67": "PFAS", "1763": "PFAS", "375-95": "PFAS", "335-76": "PFAS",
    "2058": "PFAS", "307-55": "PFAS",
    "309-00": "Pesticide", "57-74": "Pesticide", "72-": "Pesticide",
    "50-29": "Pesticide", "60-57": "Pesticide", "76-44": "Pesticide",
    "1024": "Pesticide",
    "TPH": "TPH",
}


def get_analyte_group(cas_rn):
    """Determine analyte group from CAS number."""
    for prefix, group in ANALYTE_GROUPS.items():
        if cas_rn.startswith(prefix):
            return group
    return "Other"


def main():
    print("Generating ERA reference data...")
    print("-" * 50)

    conn = duckdb.connect(str(DB_PATH))

    # Create essential tables directly (more reliable than parsing SQL file)
    print("Creating tables...")

    # Create ref_screening_levels table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ref_screening_levels (
            cas_rn VARCHAR PRIMARY KEY,
            analyte_name VARCHAR,
            rsl_residential_soil_mg_kg DECIMAL(15,6),
            rsl_industrial_soil_mg_kg DECIMAL(15,6),
            rsl_residential_tap_ug_l DECIMAL(15,6),
            rsl_mcl_ug_l DECIMAL(15,6),
            eco_ssl_plants_mg_kg DECIMAL(15,6),
            eco_ssl_soil_inverts_mg_kg DECIMAL(15,6),
            eco_ssl_avian_mg_kg DECIMAL(15,6),
            eco_ssl_mammalian_mg_kg DECIMAL(15,6),
            carcinogen VARCHAR,
            target_organ VARCHAR,
            update_date DATE
        )
    """)
    print("  Created: ref_screening_levels")

    # Create dim_analytes table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS dim_analytes (
            cas_rn VARCHAR PRIMARY KEY,
            analyte_name VARCHAR NOT NULL,
            analyte_group VARCHAR,
            chemical_formula VARCHAR,
            molecular_weight DECIMAL(10,4)
        )
    """)
    print("  Created: dim_analytes")

    # Create dim_matrix table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS dim_matrix (
            matrix_code VARCHAR PRIMARY KEY,
            matrix_name VARCHAR,
            matrix_description VARCHAR
        )
    """)

    # Insert standard matrix codes
    conn.execute("""
        INSERT OR REPLACE INTO dim_matrix VALUES
            ('SO', 'Soil', 'Surface or subsurface soil sample'),
            ('GW', 'Groundwater', 'Groundwater sample from monitoring well'),
            ('SE', 'Sediment', 'Stream, lake, or pond sediment'),
            ('SW', 'Surface Water', 'River, stream, lake, or pond water'),
            ('AI', 'Air Indoor', 'Indoor air sample'),
            ('AO', 'Air Outdoor', 'Ambient outdoor air sample'),
            ('SG', 'Soil Gas', 'Subsurface soil vapor sample'),
            ('DW', 'Drinking Water', 'Potable water supply sample')
    """)
    print("  Created: dim_matrix")

    # Create dim_qualifiers table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS dim_qualifiers (
            qualifier VARCHAR PRIMARY KEY,
            qualifier_name VARCHAR,
            description VARCHAR,
            use_result BOOLEAN,
            detection_status VARCHAR
        )
    """)

    conn.execute("""
        INSERT OR REPLACE INTO dim_qualifiers VALUES
            ('', 'None', 'No qualification - detected result', TRUE, 'Detected'),
            ('U', 'Non-Detect', 'Analyte not detected above detection limit', TRUE, 'Non-Detect'),
            ('J', 'Estimated', 'Result is estimated (detected but below quantitation limit)', TRUE, 'Estimated'),
            ('UJ', 'Est. Non-Detect', 'Detection limit is estimated', TRUE, 'Non-Detect'),
            ('B', 'Blank Contamination', 'Analyte found in method blank', TRUE, 'Detected'),
            ('R', 'Rejected', 'Result rejected due to QC failure', FALSE, 'Rejected')
    """)
    print("  Created: dim_qualifiers")

    # Insert RSL data
    print("\nLoading EPA Regional Screening Levels...")

    for row in RSL_DATA:
        cas_rn, name, rsl_res_soil, rsl_ind_soil, rsl_tap, mcl, eco_plant, eco_invert, eco_avian, eco_mamm, carcinogen, target = row

        # Insert into ref_screening_levels
        conn.execute("""
            INSERT OR REPLACE INTO ref_screening_levels VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_DATE)
        """, [cas_rn, name, rsl_res_soil, rsl_ind_soil, rsl_tap, mcl, eco_plant, eco_invert, eco_avian, eco_mamm, carcinogen, target])

        # Also insert into dim_analytes
        group = get_analyte_group(cas_rn)
        conn.execute("""
            INSERT OR IGNORE INTO dim_analytes (cas_rn, analyte_name, analyte_group) VALUES (?, ?, ?)
        """, [cas_rn, name, group])

    print(f"  Loaded {len(RSL_DATA)} screening levels")

    # Verify
    count = conn.execute("SELECT COUNT(*) FROM ref_screening_levels").fetchone()[0]
    print(f"\nVerification:")
    print(f"  ref_screening_levels: {count} records")

    count = conn.execute("SELECT COUNT(*) FROM dim_analytes").fetchone()[0]
    print(f"  dim_analytes: {count} records")

    count = conn.execute("SELECT COUNT(*) FROM dim_matrix").fetchone()[0]
    print(f"  dim_matrix: {count} records")

    count = conn.execute("SELECT COUNT(*) FROM dim_qualifiers").fetchone()[0]
    print(f"  dim_qualifiers: {count} records")

    conn.close()

    print("-" * 50)
    print("Reference data generation complete!")
    print(f"\nDatabase: {DB_PATH}")


if __name__ == "__main__":
    main()
