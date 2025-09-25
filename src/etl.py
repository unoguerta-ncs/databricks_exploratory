import argparse
import logging
from pyspark.sql import SparkSession

RAW_BASE_PATH = "/Volumes/workspace/default/raw"
PROCESSED_BASE_PATH = "/Volumes/workspace/default/processed"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ETL processing for a specific run date")
    parser.add_argument("--run-date", dest="run_date", required=True, help="Run date to process (YYYY-MM-DD)")
    return parser.parse_args()


def main(run_date: str) -> None:
    logging.info("Starting ETL run for run-date=%s", run_date)

    spark = SparkSession.builder.getOrCreate()

    input_path = f"{RAW_BASE_PATH}/input.csv"
    output_path = f"{PROCESSED_BASE_PATH}/{run_date}"

    logging.info("Reading input from %s", input_path)
    df = spark.read.option("header", True).csv(input_path)

    df_filtered = df.filter(df["value"].isNotNull())

    logging.info("Writing filtered output to %s", output_path)
    df_filtered.write.format("delta").mode("overwrite").save(output_path)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()
    main(args.run_date)