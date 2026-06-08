from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_XLSX = PROJECT_ROOT / "data" / "raw" / "Online Retail.xlsx"
STAGING_CSV = PROJECT_ROOT / "data" / "staging" / "online_retail.csv"


def convert_excel_to_csv() -> Path:
    dataframe = pd.read_excel(RAW_XLSX)
    STAGING_CSV.parent.mkdir(parents=True, exist_ok=True)
    dataframe.to_csv(STAGING_CSV, index=False)
    return STAGING_CSV


if __name__ == "__main__":
    output_path = convert_excel_to_csv()
    print(f"Wrote {output_path}")