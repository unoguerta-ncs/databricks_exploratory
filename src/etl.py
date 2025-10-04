import logging
import os
import argparse
from datetime import date

from pyspark.sql import SparkSession
from param_utils import get_param  # resolver

RAW_BASE_PATH = "/Volumes/workspace/default/raw"
PROCESSED_BASE_PATH = "/Volumes/workspace/default/processed"


def run_etl(
    run_date: str,
    raw_base_path: str = RAW_BASE_PATH,
    processed_base_path: str = PROCESSED_BASE_PATH,
    input_filename: str = "input.csv",
) -> None:
    """Execute the ETL flow for the provided parameters."""
    # logging.info(
    #     "Starting ETL run for run-date=%s (raw_base_path=%s, processed_base_path=%s, input_filename=%s)",
    #     run_date,
    #     raw_base_path,
    #     processed_base_path,
    #     input_filename,
    # )

    spark = SparkSession.builder.getOrCreate()

    input_path = f"{raw_base_path}/{input_filename}"

    # Ensure per-run_date subfolders: <processed>/<run_date>/Python and /Notebook
    parent_date_dir = f"{processed_base_path}/{run_date}"

    try:
        # Try to get dbutils in both notebook and job contexts
        try:
            dbutils  # type: ignore[name-defined]
        except NameError:
            from pyspark.dbutils import DBUtils  # type: ignore
            dbutils = DBUtils(spark)  # type: ignore

        try:
            files = dbutils.fs.ls(parent_date_dir)  # type: ignore
            has_delta_log = any(f.name.rstrip("/") == "_delta_log" for f in files)
            if has_delta_log:
                logging.info(
                    "Parent path %s is a Delta table from previous runs; removing to enable subfolders",
                    parent_date_dir,
                )
                dbutils.fs.rm(parent_date_dir, True)  # type: ignore
        except Exception:
            pass
    except Exception:
        pass

    output_path = f"{parent_date_dir}/Python"

    # logging.info("Reading input from %s", input_path)
    df = spark.read.option("header", True).csv(input_path)

    df_filtered = df.filter(df["value"].isNotNull())

    # logging.info("Writing filtered output to %s", output_path)
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
        default=os.getenv("ENV", "dev"),  # Env var fallback
        help="Environment key (default from ENV var or 'dev')",
    )
    parser.add_argument(
        "--raw-base-path",
        default=RAW_BASE_PATH,
        help=f"Base path where raw inputs are stored (default: {RAW_BASE_PATH})",
    )
    parser.add_argument(
        "--processed-base-path",
        default=PROCESSED_BASE_PATH,
        help=f"Base path where processed outputs will be written (default: {PROCESSED_BASE_PATH})",
    )
    parser.add_argument(
        "--input-filename",
        default="input.csv",
        help="Name of the input file inside the raw base path (default: input.csv)",
    )

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    spark = SparkSession.builder.getOrCreate()

    # Resolve run_date with priority: explicit → control table → today's date
    run_date = get_param(
        spark,
        env=args.env,
        key="run_date",
        explicit=args.run_date if args.run_date else None,
        default=date.today().strftime("%Y-%m-%d"),
    )

    # Resolve input filename with priority: explicit → control table → default
    input_filename = get_param(
        spark,
        env=args.env,
        key="input_filename",
        explicit=args.input_filename if args.input_filename else None,
        default="input.csv",
    )

    # logging.info(
    #     "Invoking run_etl with env=%s, run_date=%s, raw_base_path=%s, processed_base_path=%s, input_filename=%s",
    #     args.env,
    #     run_date,
    #     args.raw_base_path,
    #     args.processed_base_path,
    #     input_filename,
    # )
    run_etl(run_date, args.raw_base_path, args.processed_base_path, input_filename)


if __name__ == "__main__":
    main()
