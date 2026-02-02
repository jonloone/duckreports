# Marimo + DuckDB Excel Normalization Starter

A local-only toolkit for normalizing Excel data using Python, Marimo notebooks, and DuckDB.

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

## License

MIT License - See LICENSE file for details.

## Contributing

This is a starter template. Fork and customize for your specific needs.
