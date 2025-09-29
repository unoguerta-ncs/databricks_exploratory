import logging

import click
from pyspark.sql import SparkSession

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
    output_path = f"{processed_base_path}/{run_date}"

    logging.info("Reading input from %s", input_path)
    df = spark.read.option("header", True).csv(input_path)

    df_filtered = df.filter(df["value"].isNotNull())

    logging.info("Writing filtered output to %s", output_path)
    df_filtered.write.format("delta").mode("overwrite").save(output_path)


@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.option("--run-date", required=True, help="Run date to process (YYYY-MM-DD)")
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
def cli(run_date: str, raw_base_path: str, processed_base_path: str, input_filename: str) -> None:
    """Entrypoint for running the ETL job from the CLI."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run_etl(run_date, raw_base_path, processed_base_path, input_filename)


if __name__ == "__main__":
    cli()
