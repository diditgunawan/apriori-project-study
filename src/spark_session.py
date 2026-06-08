import os

from pyspark.sql import SparkSession


def create_spark_session(app_name: str = "online-retail") -> SparkSession:
    # Prefer the PySpark distribution from the project venv over any global Spark.
    os.environ.pop("SPARK_HOME", None)
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .getOrCreate()
    )