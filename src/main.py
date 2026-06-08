from pathlib import Path

from spark_session import create_spark_session


PROJECT_ROOT = Path(__file__).resolve().parents[1]
STAGING_CSV = PROJECT_ROOT / "data" / "staging" / "online_retail.csv"
CURATED_PARQUET = PROJECT_ROOT / "data" / "curated" / "online_retail.parquet"


def main() -> None:
    spark = create_spark_session()
    dataframe = spark.read.option("header", True).csv(str(STAGING_CSV))

    print("Schema:")
    dataframe.printSchema()

    print("Preview:")
    dataframe.show(5, truncate=False)

    dataframe.write.mode("overwrite").parquet(str(CURATED_PARQUET))
    print(f"Wrote {CURATED_PARQUET}")

    spark.stop()


if __name__ == "__main__":
    main()