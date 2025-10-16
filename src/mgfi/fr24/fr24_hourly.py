import os
import sys
import logging
import argparse
from datetime import datetime, timezone
from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, to_timestamp, current_timestamp, struct
)

# dynamic sys.path
sys.path.append("../../")
from mgfi.common.param_utils import get_param

if not logging.getLogger().hasHandlers():
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
logger = logging.getLogger("fr24_etl")

DEFAULT_CONTROL_PARAM_TABLE = os.getenv("CONTROL_PARAM_TABLE", "")


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
) -> None:
    spark = SparkSession.builder.getOrCreate()

    # === Extract ===
    input_path = f"{raw_base_path.rstrip('/')}/{input_filename.lstrip('/')}"
    logger.info("EXTRACT start: reading file '%s'", input_path)
    df = spark.read.json(input_path)
    logger.info("EXTRACT done: total rows=%d", df.count())

    # === Transform ===
    logger.info("TRANSFORM start: normalizing FR24 fields")
    df_tr = (
        df
        .withColumn("event_ts", to_timestamp(col("ts")))
        .withColumn("flight_id", col("properties.flightId").cast("long"))
        .withColumn("callsign", col("properties.callsign"))
        .withColumn("origin", col("properties.origin"))
        .withColumn("destination", col("properties.destination"))
        .withColumn("registration", col("properties.registration"))
        .withColumn("model", col("properties.model"))
        .withColumn("latitude", col("properties.lat").cast("double"))
        .withColumn("longitude", col("properties.lon").cast("double"))
        .withColumn("altitude", col("properties.alt").cast("integer"))
        .withColumn("source_system", lit(source_system))
        .withColumn("_source_file", lit(input_path))
        .withColumn("_ingested_at", current_timestamp())
        .select(
            "event_ts",
            "flight_id",
            "callsign",
            "origin",
            "destination",
            "registration",
            "model",
            "latitude",
            "longitude",
            "altitude",
            "source_system",
            "_source_file",
            "_ingested_at",
        )
    )
    logger.info("TRANSFORM done: normalized schema applied")

    # === Load ===
    target_table = output_table or f"{catalog}.{schema}.{source_system}_raw_adv"
    logger.info("LOAD start: writing to table '%s'", target_table)
    df_tr.write.format("delta").mode("append").saveAsTable(target_table)
    logger.info("LOAD done: wrote to table '%s'", target_table)

    # Set task values (Databricks chaining)
    try:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
        resolved_batch_id = batch_id or datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        dbutils.jobs.taskValues.set(key="batch_id", value=resolved_batch_id)
        dbutils.jobs.taskValues.set(key="run_date_utc", value=run_date_utc)
    except Exception:
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
        "--control-param-table",
        dest="control_param_table",
        required=False,
        default=None,
        help="Fully-qualified control parameter table used for fallback lookups",
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


def main(argv=None):
    parser = _build_parser()
    args = parser.parse_args(argv)
    spark = SparkSession.builder.getOrCreate()
    control_param_table = args.control_param_table or DEFAULT_CONTROL_PARAM_TABLE

    # Resolve run_date and input filename using priority: explicit > control table > default
    run_date_utc = get_param(
        spark,
        env=args.env,
        key="run_date_utc",
        explicit=args.run_date_utc if args.run_date_utc else None,
        default=None,
        control_table=control_param_table,
    )

    input_filename = get_param(
        spark,
        env=args.env,
        key="input_filename",
        explicit=args.input_filename if args.input_filename else None,
        default=None,
        control_table=control_param_table,
    )

    run_etl(
        run_date_utc= run_date_utc,
        raw_base_path=args.raw_base_path,
        input_filename= input_filename,
        output_table=args.output_table.strip() if args.output_table else None,
        source_system=args.source_system,
        catalog=args.catalog,
        schema=args.schema,
        batch_id=args.batch_id,
    )


if __name__ == "__main__":
    main()

def fr24_hourly_task() -> None:
    """Console entry point wrapper that delegates to argparse-based main()."""
    main()
