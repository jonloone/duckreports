# Excel Templates

This folder contains Excel templates for report generation.

## Setup

Generate the template by running:

```bash
python scripts/create_template.py
```

## Available Templates

### Monthly_Report_Template.xlsx

A pre-formatted template with:
- **Summary** sheet - Key metrics with placeholders
- **Details** sheet - Transaction-level data
- **Charts** sheet - Placeholder for chart generation

## Using Templates

Templates use `{{placeholder}}` syntax for dynamic values. The report generation notebook (04_generate_reports.py) demonstrates how to:

1. Load a template
2. Replace placeholders with actual data
3. Add charts programmatically
4. Save as a new file

## Customization

Modify `scripts/create_template.py` to create custom templates with your organization's branding and layout requirements.
