import os
import sys
import logging
import argparse
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
    run_date_utc: Optional[str] = None,
    raw_base_path: Optional[str] = None,
    processed_base_path: Optional[str] = None,
    input_filename: Optional[str] = None,
    output_table: Optional[str] = None,
    source_system: Optional[str] = None,
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
    batch_id: Optional[str] = None,
    # run_date_utc: Optional[str] = None,
) -> None:
    spark = SparkSession.builder.getOrCreate()

    # Extract Data From Filepath (input_filename provided)
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
    resolved_run_date_utc = run_date_utc or datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")

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
    target_table = output_table or (
        f"{catalog}.{schema}.{source_system}_raw" if catalog and schema and source_system else None
    )
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


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="ETL Job Runner")
    parser.add_argument(
        "--run-date",
        dest="run_date",
        required=False,
        default=None,
        help="Run date to process (YYYY-MM-DD). Defaults to control table or today's date",
    )
    parser.add_argument(
        "--env",
        dest="env",
        required=False,
        default=os.getenv("ENV", "dev"),
        help="Environment key (default from ENV var or 'dev')",
    )
    parser.add_argument(
        "--input-filename",
        dest="input_filename",
        required=False,
        default=None,
        help="Name of the input file inside the raw base path (default: input.csv)",
    )
    parser.add_argument(
        "--output-table",
        dest="output_table",
        required=False,
        default=None,
        help="Fully-qualified Delta table to append to. If omitted, requires --catalog and --schema",
    )
    parser.add_argument(
        "--source-system",
        dest="source_system",
        required=False,
        default="fr24",
        help="Source system label to populate in the table",
    )
    parser.add_argument(
        "--catalog",
        dest="catalog",
        required=False,
        default=None,
        help="Unity Catalog to use when deriving output table name",
    )
    parser.add_argument(
        "--schema",
        dest="schema",
        required=False,
        default=None,
        help="Schema to use when deriving output table name",
    )
    parser.add_argument(
        "--raw-base-path",
        dest="raw_base_path",
        required=False,
        default=None,
        help="Base path containing the input files (e.g. /Volumes/<catalog>/<schema>/fr24_bronze_vol)",
    )
    parser.add_argument(
        "--batch-id",
        dest="batch_id",
        required=False,
        default=None,
        help="Optional batch identifier to propagate via task values",
    )
    parser.add_argument(
        "--run-date-utc",
        dest="run_date_utc",
        required=False,
        default=None,
        help="Optional run date in UTC (YYYY-MM-DD) to propagate via task values",
    )
    return parser


def main(argv: Optional[list[str]] = None) -> None:
    parser = _build_parser()
    args = parser.parse_args(argv)

    spark = SparkSession.builder.getOrCreate()

    # Resolve run_date and input filename using priority: explicit > control table > default
    run_date_utc = get_param(
        spark,
        env=args.env,
        key="run_date_utc",
        explicit=args.run_date_utc if args.run_date_utc else None,
        default=None,
        control_table=CONTROL_PARAM_TABLE,
    )

    input_filename = get_param(
        spark,
        env=args.env,
        key="input_filename",
        explicit=args.input_filename if args.input_filename else None,
        default=None,
        control_table=CONTROL_PARAM_TABLE,
    )

    # Use environment-configured base paths (or defaults) and write to the table
    run_etl(
        run_date_utc=run_date_utc,
        raw_base_path=args.raw_base_path,
        processed_base_path=None,
        input_filename=input_filename,
        output_table=args.output_table,
        source_system=args.source_system,
        catalog=args.catalog,
        schema=args.schema,
        batch_id=args.batch_id,
        # run_date_utc=args.run_date_utc,
    )


if __name__ == "__main__":
    main()


def fr24_hourly_task() -> None:
    """Console entry point wrapper that delegates to argparse-based main()."""
    main()
