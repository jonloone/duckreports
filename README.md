# Marimo + DuckDB Excel Normalization Starter

A local-only toolkit for normalizing Excel data using Python, Marimo notebooks, and DuckDB.

**Now includes Environmental Risk Assessment (ERA) workflow with EPA-standard data formats.**

## IT Security Notes

This project is designed for **local-only processing** with no network dependencies after initial installation:

1. **All dependencies are open source** (Apache 2.0 / MIT licensed)
2. **No network access required** after `pip install`
3. **No server processes** - DuckDB runs entirely in-process
4. **All data stays local** - no cloud storage or external APIs
5. **Notebooks are plain `.py` files** - fully auditable source code

### Dependencies

| Package | License | Purpose |
|---------|---------|---------|
| marimo | Apache 2.0 | Interactive notebook interface |
| duckdb | MIT | In-process analytical database |
| openpyxl | MIT | Excel file reading/writing |
| pandas | BSD-3 | Data manipulation |

## Installation

### Prerequisites

- Python 3.10 or higher
- pip (Python package manager)

### Setup

1. Create and activate a virtual environment:

```bash
# Windows
python -m venv .venv
.venv\Scripts\activate

# Linux/Mac
python -m venv .venv
source .venv/bin/activate
```

2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Generate sample data:

```bash
python scripts/generate_sample_data.py
```

4. (Optional) Create Excel templates:

```bash
python scripts/create_template.py
```

5. Initialize the database (optional - notebooks will create if needed):

```bash
python scripts/setup_database.py
```

## Quick Start

1. Generate sample data (or place your own Excel files in `data/raw/`):

```bash
python scripts/generate_sample_data.py
```

2. Launch the data ingestion notebook:

```bash
marimo edit notebooks/01_ingest_data.py
```

3. Follow the workflow through the numbered notebooks:
   - `01_ingest_data.py` - Load Excel files into DuckDB
   - `02_explore_data.py` - Interactive data exploration
   - `03_normalize_data.py` - Create normalized schema
   - `04_generate_reports.py` - Export formatted Excel reports

## Project Structure

```
marimo-duckdb-starter/
├── data/
│   ├── raw/           # Source Excel files
│   ├── processed/     # DuckDB database (gitignored)
│   └── output/        # Generated reports (gitignored)
├── templates/         # Excel output templates
├── notebooks/         # Marimo notebooks (.py files)
├── sql/
│   ├── schema/        # Table definitions
│   └── views/         # View definitions
└── scripts/           # Utility scripts
```

## Sample Data

This starter includes sample data for demonstration:

- `sample_orders.xlsx` - 50 sample orders
- `sample_customers.xlsx` - 10 customers
- `sample_products.xlsx` - 15 products

## Workflow Overview

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Excel     │ ──► │   DuckDB    │ ──► │  Normalize  │ ──► │   Export    │
│   Files     │     │   Ingest    │     │   Schema    │     │   Excel     │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
     01_               02_/03_              03_                 04_
```

## Running Notebooks

Marimo notebooks can be run in two modes:

**Edit mode** (interactive development):
```bash
marimo edit notebooks/01_ingest_data.py
```

**Run mode** (execute as script):
```bash
marimo run notebooks/01_ingest_data.py
```

---

## Environmental Risk Assessment (ERA) Workflow

This repository includes a complete ERA workflow based on EPA standards.

### ERA Quick Start

```bash
# Generate sample ERA data (simulated contaminated site)
python scripts/generate_era_sample_data.py

# Load EPA Regional Screening Levels
python scripts/generate_era_reference_data.py

# Launch ERA workflow
marimo edit notebooks/era_01_ingest_edd.py
```

### ERA Notebooks

| Notebook | Purpose |
|----------|---------|
| `era_01_ingest_edd.py` | Import lab EDD data (samples + results) |
| `era_02_screening.py` | Compare to EPA RSLs, identify COPCs |
| `era_03_statistics.py` | Calculate UCL95, detection frequency |
| `era_04_reports.py` | Generate formal ERA summary tables |

### ERA Data Standards

The ERA workflow follows industry standards:

- **EQuIS-compatible schema** for sample and result data
- **EPA Regional Screening Levels (RSLs)** for comparison
- **Standard lab qualifiers** (U, J, B, R) handling
- **Hazard Quotient calculations** for risk screening
- **UCL95 statistics** per EPA ProUCL guidance

### ERA Schema

```
dim_locations      → Monitoring wells, soil borings
dim_analytes       → Chemicals (CAS numbers)
dim_matrix         → Sample media (soil, groundwater, etc.)
dim_qualifiers     → Lab qualifier codes
ref_screening_levels → EPA RSLs for comparison
fact_samples       → Sample collection metadata
fact_results       → Analytical results
```

### ERA Output Tables

The report generator creates standard ERA tables:

1. **Sample Summary** - All samples with locations and dates
2. **Detection Summary** - Detection frequency by analyte
3. **Screening Comparison** - Results vs RSLs with HQ
4. **COPC Summary** - Chemicals of Potential Concern
5. **Exposure Point Concentrations** - Statistical EPCs

---

## License

MIT License - See LICENSE file for details.

## Contributing

This is a starter template. Fork and customize for your specific needs.
