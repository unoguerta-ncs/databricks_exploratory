import os
import sys
import logging
import click
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, lit, current_timestamp

# dynamic sys.path
sys.path.append('../../')
from mgfi.common.param_utils import get_param

# Configure basic logging (prints to driver logs / job output)
if not logging.getLogger().hasHandlers():
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
logger = logging.getLogger("fr24_etl")

# No hardcoded defaults here; paths, catalog, and schema are provided via job/CLI
# Control parameter table location for get_param lookups
CONTROL_PARAM_TABLE = os.getenv("CONTROL_PARAM_TABLE", "mgfi_catalog_test.sandbox.control_parameters")

def run_etl(
    run_date: str,
    raw_base_path: Optional[str] = None,
    processed_base_path: Optional[str] = None,
    input_filename: Optional[str] = None,
    output_table: Optional[str] = None,
    source_system: str = "fr24",
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
    batch_id: Optional[str] = None,
    run_date_utc: Optional[str] = None,
) -> None:
    spark = SparkSession.builder.getOrCreate()

    # Extract Data From Filepath (input_filename provided)
    if not raw_base_path:
        raise click.ClickException("raw_base_path is required (pass --raw-base-path)")
    if not input_filename:
        raise click.ClickException("input_filename is required (pass --input-filename or control param)")
    input_path = f"{raw_base_path.rstrip('/')}/{input_filename.lstrip('/')}"
    logger.info("EXTRACT start: reading file '%s'", input_path)
    df = spark.read.json(input_path)
    logger.info("EXTRACT done: columns=%s", ", ".join(df.columns))


    # Transform
    logger.info(
        "TRANSFORM start: mapping ts->event_ts, properties.flightId->flight_id, properties.callsign->callsign"
    )
    df = (
        df.withColumn("event_ts", col("ts"))
          .withColumn("flight_id", col("properties.flightId"))
          .withColumn("callsign", col("properties.callsign"))
    )

    # Derive batch/run metadata defaults
    resolved_batch_id = batch_id or datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    resolved_run_date_utc = run_date_utc or datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Cast/derive columns to match target schema
    df_out = (
        df.withColumn("event_ts", to_timestamp(col("event_ts"), "yyyy-MM-dd'T'HH:mm:ssX"))
        .withColumn("flight_id", col("flight_id").cast("long"))
        .withColumn("callsign", col("callsign").cast("string"))
        .withColumn("source_system", lit(source_system))
        # Unity Catalog does not allow input_file_name(); capture provided path instead
        .withColumn("_source_file", lit(input_path))
        .withColumn("_ingested_at", current_timestamp())
        .select(
            "event_ts",
            "flight_id",
            "callsign",
            "source_system",
            "_source_file",
            "_ingested_at",
        )
    )
    logger.info("TRANSFORM done: projected columns: event_ts, flight_id, callsign, source_system, _source_file, _ingested_at")

    # Load to bronze table
    if output_table:
        target_table = output_table
    else:
        if not (catalog and schema):
            raise click.ClickException(
                "Provide --output-table or both --catalog and --schema to derive target table"
            )
        target_table = f"{catalog}.{schema}.{source_system}_raw"
    logger.info("LOAD start: appending to table '%s'", target_table)
    df_out.write.format("delta").mode("append").saveAsTable(target_table)
    logger.info("LOAD done: wrote to table '%s'", target_table)

    # Passing params downstream
    try:
        from pyspark.dbutils import DBUtils  # type: ignore
        dbutils = DBUtils(spark)
        dbutils.jobs.taskValues.set(key="batch_id", value=resolved_batch_id)
        dbutils.jobs.taskValues.set(key="run_date_utc", value=resolved_run_date_utc)
        logger.info(
            "TASK VALUES set: batch_id=%s, run_date_utc=%s",
            resolved_batch_id,
            resolved_run_date_utc,
        )
    except Exception:
        # Ignore if DBUtils is not available (e.g., local run)
        logger.debug("DBUtils not available; skipping taskValues propagation")


@click.command(help="ETL Job Runner")
@click.option(
    "--run-date",
    "run_date",
    required=False,
    default=None,
    help="Run date to process (YYYY-MM-DD). Defaults to control table or today's date",
)
@click.option(
    "--env",
    "env",
    required=False,
    default=os.getenv("ENV", "dev"),
    show_default=True,
    help="Environment key (default from ENV var or 'dev')",
)
@click.option(
    "--input-filename",
    "input_filename",
    required=False,
    default=None,
    help="Name of the input file inside the raw base path (default: input.csv)",
)
@click.option(
    "--output-table",
    "output_table",
    required=False,
    default=None,
    help="Fully-qualified Delta table to append to. If omitted, requires --catalog and --schema",
)
@click.option(
    "--source-system",
    "source_system",
    required=False,
    default="fr24",
    show_default=True,
    help="Source system label to populate in the table",
)
@click.option(
    "--catalog",
    "catalog",
    required=False,
    default=None,
    help="Unity Catalog to use when deriving output table name",
)
@click.option(
    "--schema",
    "schema",
    required=False,
    default=None,
    help="Schema to use when deriving output table name",
)
@click.option(
    "--raw-base-path",
    "raw_base_path",
    required=False,
    default=None,
    help="Base path containing the input files (e.g. /Volumes/<catalog>/<schema>/fr24_bronze_vol)",
)
@click.option(
    "--batch-id",
    "batch_id",
    required=False,
    default=None,
    help="Optional batch identifier to propagate via task values",
)
@click.option(
    "--run-date-utc",
    "run_date_utc",
    required=False,
    default=None,
    help="Optional run date in UTC (YYYY-MM-DD) to propagate via task values",
)
def main(run_date, env, input_filename, output_table, source_system, catalog, schema, raw_base_path, batch_id, run_date_utc):
    spark = SparkSession.builder.getOrCreate()

    # Resolve run_date and input filename using priority: explicit > control table > default
    run_date = get_param(
        spark,
        env=env,
        key="run_date",
        explicit=run_date if run_date else None,
        default=None,
        control_table=CONTROL_PARAM_TABLE,
    )

    input_filename = get_param(
        spark,
        env=env,
        key="input_filename",
        explicit=input_filename if input_filename else None,
        default=None,
        control_table=CONTROL_PARAM_TABLE,
    )

    # Use environment-configured base paths (or defaults) and write to the table
    run_etl(
        run_date,
        raw_base_path=raw_base_path,
        processed_base_path=None,
        input_filename=input_filename,
        output_table=output_table,
        source_system=source_system,
        catalog=catalog,
        schema=schema,
        batch_id=batch_id,
        run_date_utc=run_date_utc,
    )


if __name__ == "__main__":
    # Avoid Click calling sys.exit (raises SystemExit) in Databricks runners
    main(standalone_mode=False)
