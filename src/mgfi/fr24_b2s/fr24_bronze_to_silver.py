import os
import sys
import logging
import argparse
from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, lit, to_utc_timestamp, upper, trim,
    count, avg, max as spark_max, min as spark_min
)

sys.path.append("../../")
from mgfi.common.param_utils import resolve_run_date_utc, resolve_batch_id

if not logging.getLogger().hasHandlers():
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
logger = logging.getLogger("fr24_bronze_to_silver")

UPSTREAM_TASK_KEY = os.getenv("UPSTREAM_TASK_KEY", "fr24_hourly")
DEFAULT_CONTROL_PARAM_TABLE = os.getenv("CONTROL_PARAM_TABLE", "")


def run_bronze_to_silver(
    env: str,
    run_date_utc: Optional[str] = None,
    input_table: Optional[str] = None,
    output_table: Optional[str] = None,
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
    source_system: Optional[str] = None,
    control_param_table: Optional[str] = None,
):
    spark = SparkSession.builder.getOrCreate()
    control_table = control_param_table or DEFAULT_CONTROL_PARAM_TABLE

    resolved_run_date_utc = resolve_run_date_utc(
        spark, env, run_date_utc, UPSTREAM_TASK_KEY, control_table
    )
    logger.info("Using run_date_utc=%s", resolved_run_date_utc)

    resolved_input_table = input_table or f"{catalog}.{schema}.{source_system}_raw_adv"
    resolved_output_table = output_table or f"{catalog}.{schema}.{source_system}_silver_adv"

    logger.info("Reading bronze table: %s", resolved_input_table)
    df = spark.read.table(resolved_input_table)

    batch_id_value = resolve_batch_id(spark, UPSTREAM_TASK_KEY)

    # Normalize, add UTC event date, filter by run date
    df_proj = (
        df.withColumn("event_dt_utc", to_date(to_utc_timestamp(col("event_ts"), "UTC")))
          .withColumn("callsign_norm", trim(upper(col("callsign"))))
    )

    df_filtered = df_proj.filter(col("event_dt_utc") == to_date(lit(resolved_run_date_utc)))

    if batch_id_value:
        df_filtered = df_filtered.withColumn("batch_id", lit(batch_id_value))
    else:
        df_filtered = df_filtered.withColumn("batch_id", lit(None))

    # === New Transform: aggregate by flight_id/date ===
    logger.info("Aggregating flight segments by flight_id and event_dt_utc")
    df_segmented = (
        df_filtered.groupBy("flight_id", "event_dt_utc")
        .agg(
            count("*").alias("num_points"),
            spark_min("event_ts").alias("first_seen_ts"),
            spark_max("event_ts").alias("last_seen_ts"),
            avg("latitude").alias("avg_lat"),
            avg("longitude").alias("avg_lon"),
            avg("altitude").alias("avg_altitude"),
        )
        .withColumn("run_date_utc", lit(resolved_run_date_utc))
    )

    df_out = df_segmented.select(
        "flight_id",
        "event_dt_utc",
        "first_seen_ts",
        "last_seen_ts",
        "num_points",
        "avg_lat",
        "avg_lon",
        "avg_altitude",
        "run_date_utc",
    )

    count_out = df_out.count()
    logger.info("Writing %d rows to silver table %s", count_out, resolved_output_table)
    df_out.write.format("delta").mode("append").saveAsTable(resolved_output_table)
    logger.info("Write complete: %s", resolved_output_table)


def _build_parser():
    parser = argparse.ArgumentParser(description="FR24 Bronze→Silver transformation")
    parser.add_argument("--env", default=os.getenv("ENV", "dev"))
    parser.add_argument("--run-date-utc", default=None)
    parser.add_argument("--input-table", default=None)
    parser.add_argument("--output-table", default=None)
    parser.add_argument("--catalog", default=None)
    parser.add_argument("--schema", default=None)
    parser.add_argument("--source-system", default="fr24")
    parser.add_argument("--control-param-table", default=None)
    return parser


def main(argv=None):
    parser = _build_parser()
    args = parser.parse_args(argv)
    run_bronze_to_silver(
        env=args.env,
        run_date_utc=args.run_date_utc,
        input_table=args.input_table.strip() if args.input_table else None,
        output_table=args.output_table.strip() if args.output_table else None,
        catalog=args.catalog,
        schema=args.schema,
        source_system=args.source_system,
        control_param_table=args.control_param_table,
    )


if __name__ == "__main__":
    main()

def fr24_bronze_to_silver_task() -> None:
    """Console entry point wrapper that delegates to argparse-based main()."""
    main()

