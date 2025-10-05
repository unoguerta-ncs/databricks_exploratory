import os
import argparse
from datetime import date

from pyspark.sql import SparkSession
from param_utils import get_param


# Allow overriding via environment variables; fall back to defaults
RAW_BASE_PATH = os.getenv("RAW_BASE_PATH", "/Volumes/workspace/default/raw")
PROCESSED_BASE_PATH = os.getenv("PROCESSED_BASE_PATH", "/Volumes/workspace/default/processed")


def run_etl(
    run_date: str,
    raw_base_path: str = RAW_BASE_PATH,
    processed_base_path: str = PROCESSED_BASE_PATH,
    input_filename: str = "input.csv",
) -> None:
    """Execute the ETL flow for the provided parameters."""
    spark = SparkSession.builder.getOrCreate()

    input_path = f"{raw_base_path}/{input_filename}"
    parent_date_dir = f"{processed_base_path}/{run_date}"
    output_path = f"{parent_date_dir}/Python"

    df = spark.read.option("header", True).csv(input_path)
    df_filtered = df.filter(df["value"].isNotNull())

    # print(f"Read {df.count()} rows; writing {df_filtered.count()} to {output_path}")
    df_filtered.write.format("delta").mode("overwrite").save(output_path)


def main():
    parser = argparse.ArgumentParser(description="ETL Job Runner")

    parser.add_argument(
        "--run-date",
        required=False,
        default=None,
        help="Run date to process (YYYY-MM-DD). Defaults to control table or today's date",
    )
    parser.add_argument(
        "--env",
        required=False,
        default=os.getenv("ENV", "dev"),
        help="Environment key (default from ENV var or 'dev')",
    )
    
    parser.add_argument(
        "--input-filename",
        default="input.csv",
        help="Name of the input file inside the raw base path (default: input.csv)",
    )

    args = parser.parse_args()
    spark = SparkSession.builder.getOrCreate()

    # Resolve run_date and input filename using priority: explicit > control table > default
    run_date = get_param(
        spark,
        env=args.env,
        key="run_date",
        explicit=args.run_date if args.run_date else None,
        default=date.today().strftime("%Y-%m-%d"),
    )

    input_filename = get_param(
        spark,
        env=args.env,
        key="input_filename",
        explicit=args.input_filename if args.input_filename else None,
        default="input.csv",
    )

    # Use environment-configured base paths (or defaults) instead of CLI flags
    run_etl(run_date, RAW_BASE_PATH, PROCESSED_BASE_PATH, input_filename)


if __name__ == "__main__":
    main()
