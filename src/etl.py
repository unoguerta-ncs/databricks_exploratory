import logging
from datetime import date

import click
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
    logging.info(
        "Starting ETL run for run-date=%s (raw_base_path=%s, processed_base_path=%s, input_filename=%s)",
        run_date,
        raw_base_path,
        processed_base_path,
        input_filename,
    )

    spark = SparkSession.builder.getOrCreate()

    input_path = f"{raw_base_path}/{input_filename}"

    # Ensure per-run_date subfolders: <processed>/<run_date>/Python and /Notebook
    parent_date_dir = f"{processed_base_path}/{run_date}"

    # If an older run created a Delta table at <processed>/<run_date>, remove it to allow subfolders
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

    logging.info("Reading input from %s", input_path)
    df = spark.read.option("header", True).csv(input_path)

    df_filtered = df.filter(df["value"].isNotNull())

    logging.info("Writing filtered output to %s", output_path)
    df_filtered.write.format("delta").mode("overwrite").save(output_path)


@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.option(
    "--run-date",
    required=False,
    default=None,
    help="Run date to process (YYYY-MM-DD). Defaults to today's date if omitted.",
)
@click.option(
    "--env",
    required=False,
    default="dev",
    show_default=True,
    help="Environment key to resolve parameters from control table",
)
@click.option(
    "--raw-base-path",
    default=RAW_BASE_PATH,
    show_default=True,
    help="Base path where raw inputs are stored",
)
@click.option(
    "--processed-base-path",
    default=PROCESSED_BASE_PATH,
    show_default=True,
    help="Base path where processed outputs will be written",
)
@click.option(
    "--input-filename",
    default="input.csv",
    show_default=True,
    help="Name of the input file inside the raw base path",
)
def cli(run_date: str, env: str, raw_base_path: str, processed_base_path: str, input_filename: str) -> None:
    """Entrypoint for running the ETL job from the CLI."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    spark = SparkSession.builder.getOrCreate()
    # Resolve run_date with priority: explicit -> control table -> today's date
    explicit_run_date = None if (not run_date or str(run_date).strip().startswith("{{")) else run_date
    run_date = get_param(
        spark,
        env=env,
        key="run_date",
        explicit=explicit_run_date,
        default=date.today().strftime("%Y-%m-%d"),
    )
    logging.info(
        "CLI invoking run_etl with env=%s, run_date=%s, raw_base_path=%s, processed_base_path=%s, input_filename=%s",
        env,
        run_date,
        raw_base_path,
        processed_base_path,
        input_filename,
    )
    run_etl(run_date, raw_base_path, processed_base_path, input_filename)


if __name__ == "__main__":
    try:
        cli(standalone_mode=False)
    except SystemExit as exc:
        if exc.code != 0:
            raise
