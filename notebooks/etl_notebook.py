# Databricks notebook source
# COMMAND ----------
"""
Databricks Notebook Template: ETL driver using control-parameter resolver.

If using Databricks Repos, this file can be referenced as a notebook task.
This version performs light processing on input.csv and writes a Delta output.
"""
# COMMAND ----------

from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Ensure bundled source modules (src/) are importable when running as a notebook task
import sys
try:
    if "dbutils" in globals():
        _nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        # _nb_path looks like: /Workspace/.../.bundle/<bundle>/<target>/files/notebooks/etl_notebook
        if "/notebooks" in _nb_path:
            _files_root = "/Workspace" + _nb_path.split("/notebooks", 1)[0]
            _src_dir = _files_root + "/src"
            if _src_dir not in sys.path:
                sys.path.insert(0, _src_dir)
except Exception:
    # Non-fatal: fall back to default sys.path
    pass

from param_utils import get_param
from etl import run_etl, RAW_BASE_PATH, PROCESSED_BASE_PATH


def _get_widget_or_none(name: str):
    try:
        if "dbutils" in globals():
            return dbutils.widgets.get(name)
    except Exception:
        return None
    return None


# Use active Spark session when run inside Databricks
spark = SparkSession.builder.getOrCreate()

# Resolve parameters, allowing explicit widget overrides
env = _get_widget_or_none("env") or "dev"
explicit_run_date = _get_widget_or_none("run_date")
explicit_raw_base = _get_widget_or_none("raw_base_path")
explicit_processed_base = _get_widget_or_none("processed_base_path")
explicit_input_filename = _get_widget_or_none("input_filename") or "input.csv"

run_date = get_param(
    spark,
    env=env,
    key="run_date",
    explicit=explicit_run_date,
    default=date.today().strftime("%Y-%m-%d"),
)

raw_base_path = get_param(
    spark,
    env=env,
    key="raw_base_path",
    explicit=explicit_raw_base,
    default=RAW_BASE_PATH,
)
processed_base_path = get_param(
    spark,
    env=env,
    key="processed_base_path",
    explicit=explicit_processed_base,
    default=PROCESSED_BASE_PATH,
)

print(f"Notebook ETL env={env}, run_date={run_date}")
print(f"raw_base_path={raw_base_path}, processed_base_path={processed_base_path}, input={explicit_input_filename}")
# COMMAND ----------

# Perform a bit of processing on input.csv
input_path = f"{raw_base_path}/{explicit_input_filename}"
# Write into the date folder alongside the Python output
output_path = f"{processed_base_path}/{run_date}/Notebook"

df = (
    spark.read.option("header", True).csv(input_path)
)

# Simple transformations: keep only string-like values in 'value' (non-numeric)
# This avoids casting and tolerates rows like 'hello' safely
numeric_regex = r"^[-+]?\d*\.?\d+(e[-+]?\d+)?$"
df_processed = (
    df.filter(col("value").isNotNull())
      .filter(~col("value").rlike(numeric_regex))
)

print(f"Read {df.count()} rows; writing {df_processed.count()} string-only rows to {output_path}")

# If output path exists but isn't a Delta table (no _delta_log), remove it to avoid Delta write errors
try:
    if "dbutils" in globals():
        try:
            files = dbutils.fs.ls(output_path)
            has_delta_log = any(f.name.rstrip("/") == "_delta_log" for f in files)
            if not has_delta_log and files:
                print(f"Output path exists without Delta log; removing {output_path}")
                dbutils.fs.rm(output_path, True)
        except Exception:
            pass
except Exception:
    pass

df_processed.write.format("delta").mode("overwrite").save(output_path)

# Optionally, also call the shared run_etl to produce the canonical output
# run_etl(run_date)
