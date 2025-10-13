import os
import sys
import logging
import click
from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, lit, to_utc_timestamp, upper, trim

# Ensure package imports resolve when run as a script
sys.path.append("../../")
from mgfi.common.param_utils import resolve_run_date_utc, resolve_batch_id


# Configure logging
if not logging.getLogger().hasHandlers():
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
logger = logging.getLogger("fr24_b2s")


DEFAULT_CATALOG = os.getenv("CATALOG", "mgfi_catalog_test")
DEFAULT_INPUT_TABLE = f"{DEFAULT_CATALOG}.sandbox.fr24_raw"
DEFAULT_OUTPUT_TABLE = f"{DEFAULT_CATALOG}.sandbox.fr24_silver"
UPSTREAM_TASK_KEY = os.getenv("UPSTREAM_TASK_KEY", "etl_task")


def run_bronze_to_silver(
    env: str,
    run_date_utc: Optional[str] = None,
    input_table: str = DEFAULT_INPUT_TABLE,
    output_table: str = DEFAULT_OUTPUT_TABLE,
):
    spark = SparkSession.builder.getOrCreate()

    # Resolve run_date_utc (no default allowed)
    resolved_run_date_utc = resolve_run_date_utc(spark, env, run_date_utc, UPSTREAM_TASK_KEY)
    logger.info("Using run_date_utc=%s", resolved_run_date_utc)
    logger.info("Reading bronze table: %s", input_table)
    df = spark.read.table(input_table)

    # Resolve batch_id from upstream if available (centralized helper)
    batch_id_value = resolve_batch_id(spark, UPSTREAM_TASK_KEY)

    # Derive event date in UTC and normalize callsign
    df_proj = (
        df.withColumn("event_dt_utc", to_date(to_utc_timestamp(col("event_ts"), "UTC")))
          .withColumn("callsign_norm", trim(upper(col("callsign"))))
    )

    # Filter by UTC event date
    logger.info("Filtering rows where event_dt_utc = %s", resolved_run_date_utc)
    df_filtered = df_proj.filter(
        col("event_dt_utc") == to_date(lit(resolved_run_date_utc))
    )

    # Add batch_id column (may be null if not available)
    if batch_id_value is not None and str(batch_id_value).strip() != "":
        df_filtered = df_filtered.withColumn("batch_id", lit(batch_id_value))
    else:
        df_filtered = df_filtered.withColumn("batch_id", lit(None))

    # Project required silver columns in order
    df_out = df_filtered.select(
        "event_ts",
        "event_dt_utc",
        "flight_id",
        "callsign_norm",
        "batch_id",
        "_ingested_at",
    )

    # Write to silver table
    count = df_out.count()
    logger.info("Writing %s rows to silver table: %s", count, output_table)
    df_out.write.format("delta").mode("append").saveAsTable(output_table)
    logger.info("Write complete: %s", output_table)


@click.command(help="FR24 Bronze-to-Silver filter by run_date_utc")
@click.option(
    "--env",
    "env",
    required=False,
    default=os.getenv("ENV", "dev"),
    show_default=True,
    help="Environment key used for control parameter lookups",
)
@click.option(
    "--run-date-utc",
    "run_date_utc",
    required=False,
    default=None,
    help=(
        "Run date in UTC (YYYY-MM-DD). If omitted, attempts to read from upstream task values "
        "or control parameters. No built-in default."
    ),
)
@click.option(
    "--input-table",
    "input_table",
    required=False,
    default=DEFAULT_INPUT_TABLE,
    show_default=True,
    help="Bronze Delta table to read from",
)
@click.option(
    "--output-table",
    "output_table",
    required=False,
    default=DEFAULT_OUTPUT_TABLE,
    show_default=True,
    help="Silver Delta table to append to",
)
def main(env, run_date_utc, input_table, output_table):
    run_bronze_to_silver(env=env, run_date_utc=run_date_utc, input_table=input_table, output_table=output_table)


if __name__ == "__main__":
    # Avoid Click calling sys.exit in Databricks jobs
    main(standalone_mode=False)
