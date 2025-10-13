import os
import click
from datetime import date, datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, lit, current_timestamp
from param_utils import get_param


# Allow overriding via environment variables; fall back to defaults
RAW_BASE_PATH = os.getenv("RAW_BASE_PATH", "/Volumes/mgfi_catalog_test/sandbox/fr24_bronze_vol")
PROCESSED_BASE_PATH = os.getenv("PROCESSED_BASE_PATH", "/Volumes/mgfi_catalog_test/sandbox")


def run_etl(
    run_date: str,
    raw_base_path: str = RAW_BASE_PATH,
    processed_base_path: str = PROCESSED_BASE_PATH,
    input_filename: str = "",
    output_table: str = "mgfi_catalog_test.sandbox.fr24_raw",
    source_system: str = "fr24",
    catalog: str = None,
    batch_id: str = None,
    run_date_utc: str = None,
) -> None:
    """Execute the ETL flow for the provided parameters.

    Reads the input file from `raw_base_path/input_filename`, shapes it to the
    `fr24_raw` table schema, and appends to `output_table`.
    """
    spark = SparkSession.builder.getOrCreate()

    if not input_filename or str(input_filename).strip() == "":
        raise ValueError("input_filename must be provided (non-empty)")

    input_path = f"{raw_base_path}/{input_filename}"

    # Reader selection based on file extension (default to CSV with header)
    lower_name = input_filename.lower()
    if lower_name.endswith(".json"):
        df = spark.read.json(input_path)
    elif lower_name.endswith(".parquet"):
        df = spark.read.parquet(input_path)
    else:
        df = spark.read.option("header", True).csv(input_path)

    # Directly project the expected FR24 JSON fields
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

    # Resolve output table from catalog/source_system if not explicitly provided
    target_table = output_table if output_table else (
        f"{catalog}.sandbox.{source_system}_raw" if catalog else "mgfi_catalog_test.sandbox.fr24_raw"
    )

    # Append into the managed Delta table
    df_out.write.format("delta").mode("append").saveAsTable(target_table)

    # Expose batch and run date values to downstream tasks when running in Databricks
    try:
        from pyspark.dbutils import DBUtils  # type: ignore
        dbutils = DBUtils(spark)
        dbutils.jobs.taskValues.set(key="batch_id", value=resolved_batch_id)
        dbutils.jobs.taskValues.set(key="run_date_utc", value=resolved_run_date_utc)
    except Exception:
        # Ignore if DBUtils is not available (e.g., local run)
        pass


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
    default="mgfi_catalog_test.sandbox.fr24_raw",
    show_default=True,
    help="Fully-qualified Delta table to append to",
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
    default=os.getenv("CATALOG", "mgfi_catalog_test"),
    show_default=True,
    help="Unity Catalog to use when deriving output table name",
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
def main(run_date, env, input_filename, output_table, source_system, catalog, batch_id, run_date_utc):
    spark = SparkSession.builder.getOrCreate()

    # Resolve run_date and input filename using priority: explicit > control table > default
    run_date = get_param(
        spark,
        env=env,
        key="run_date",
        explicit=run_date if run_date else None,
        default=None,
    )

    input_filename = get_param(
        spark,
        env=env,
        key="input_filename",
        explicit=input_filename if input_filename else None,
        default=None,
    )

    # Use environment-configured base paths (or defaults) and write to the table
    run_etl(
        run_date,
        RAW_BASE_PATH,
        PROCESSED_BASE_PATH,
        input_filename,
        output_table=output_table,
        source_system=source_system,
        catalog=catalog,
        batch_id=batch_id,
        run_date_utc=run_date_utc,
    )


if __name__ == "__main__":
    # Avoid Click calling sys.exit (raises SystemExit) in Databricks runners
    main(standalone_mode=False)
