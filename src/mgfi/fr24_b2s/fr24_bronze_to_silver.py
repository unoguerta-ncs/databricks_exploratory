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


UPSTREAM_TASK_KEY = os.getenv("UPSTREAM_TASK_KEY", "etl_task")
# Control parameter table location for get_param lookups used by resolver
CONTROL_PARAM_TABLE = os.getenv("CONTROL_PARAM_TABLE", "mgfi_catalog_test.sandbox.control_parameters")


def run_bronze_to_silver(
    env: str,
    run_date_utc: Optional[str] = None,
    input_table: Optional[str] = None,
    output_table: Optional[str] = None,
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
    source_system: Optional[str] = None,
):
    spark = SparkSession.builder.getOrCreate()

    # Resolve run_date_utc (no default allowed)
    resolved_run_date_utc = resolve_run_date_utc(spark, env, run_date_utc, UPSTREAM_TASK_KEY, CONTROL_PARAM_TABLE)
    logger.info("Using run_date_utc=%s", resolved_run_date_utc)

    # Resolve table names
    resolved_input_table = input_table or (f"{catalog}.{schema}.{source_system}_raw" if catalog and schema and source_system else None)
    resolved_output_table = output_table or (f"{catalog}.{schema}.{source_system}_silver" if catalog and schema and source_system else None)

    logger.info("Reading bronze table: %s", resolved_input_table)
    df = spark.read.table(resolved_input_table)

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
    logger.info("Writing %s rows to silver table: %s", count, resolved_output_table)
    df_out.write.format("delta").mode("append").saveAsTable(resolved_output_table)
    logger.info("Write complete: %s", resolved_output_table)


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
    default=None,
    help="Fully-qualified bronze Delta table to read from. If omitted, requires --catalog and --schema",
)
@click.option(
    "--output-table",
    "output_table",
    required=False,
    default=None,
    help="Fully-qualified silver Delta table to append to. If omitted, requires --catalog and --schema",
)
@click.option(
    "--catalog",
    "catalog",
    required=False,
    default=None,
    help="Unity Catalog to derive default table names",
)
@click.option(
    "--schema",
    "schema",
    required=False,
    default=None,
    help="Schema to derive default table names",
)
@click.option(
    "--source-system",
    "source_system",
    required=False,
    default=None,
    help="Source system identifier to derive table names",
)
def main(env, run_date_utc, input_table, output_table, catalog, schema, source_system):
    run_bronze_to_silver(
        env=env,
        run_date_utc=run_date_utc,
        input_table=input_table,
        output_table=output_table,
        catalog=catalog,
        schema=schema,
        source_system=source_system,
    )


if __name__ == "__main__":
    # Avoid Click calling sys.exit in Databricks jobs
    main(standalone_mode=False)
