# Online Retail PySpark Project

This project runs inside WSL and uses a Python virtual environment for local PySpark processing.

## Workflow

1. Copy the raw Excel dataset into `data/raw/`.
2. Convert the Excel file to CSV with `src/extract_excel.py`.
3. Run `src/main.py` to load the CSV with PySpark and write Parquet output.

## Commands

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python src/extract_excel.py
python src/main.py
```

## VS Code In WSL

Open the project from WSL so VS Code uses the Linux interpreter and Java runtime:

```bash
cd /home/komputer7/SparkProjects/online-retail-pyspark
code .
```

Then use the built-in tasks:

- `Convert Excel to CSV`
- `Run PySpark Pipeline`