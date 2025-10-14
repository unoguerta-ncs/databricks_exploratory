FR24 ETL (Databricks Bundle + Python wheels)

Overview
- Runs a two-step ETL on Databricks via a Databricks Asset Bundle (DAB).
- Tasks are packaged as a Python wheel and executed with python_wheel_task.
- Job definition: resources/example_etl.job.yml

Prerequisites
- Databricks CLI configured with a workspace profile that has permission to create/update jobs.
- Python 3.10+ on your machine.

Build The Wheel
- Install the build tool: `python -m pip install --upgrade build`
- Build the wheel: `python -m build --wheel`
- Output is created in `dist/` (ignored by git). The bundle syncs wheels from `dist/` to the workspace during deploy.

Deploy The Bundle
- Validate: `databricks bundle validate`
- Deploy to dev: `databricks bundle deploy --target dev`

Run The Job
- Trigger both tasks (bronze then silver):
  - `databricks bundle run fr24_bronze_to_silver --target dev --params input_filename=sample_fr24.json --params run_date_utc=2025-10-06`
- You can also run with only parameter overrides you need; unspecified values fall back to defaults or control table lookups (see below).

Job Parameters (resources/example_etl.job.yml)
- `run_date` (string, default empty)
  - Used by the bronze step to resolve which business date to process. If omitted, it can be resolved from the control table.
- `env` (string, default `${bundle.target}`)
  - Environment key used when resolving parameters from the control table.
- `input_filename` (string, default empty)
  - File name inside the raw base path to read (e.g., `sample_fr24.json`).
- `run_date_utc` (string, default empty)
  - UTC date used by the silver step to filter data. If not provided, the code can try to read an upstream value/control table, but passing it explicitly is recommended.
- `source_system` (string, default `fr24`)
  - Label added to output data and used to derive table names.
- `catalog` (string, default `mgfi_catalog_test`)
  - Unity Catalog name used to form fully-qualified table names.
- `schema` (string, default `sandbox`)
  - Target schema used to form fully-qualified table names.
- `raw_base_path` (string, default `/Volumes/mgfi_catalog_test/sandbox/fr24_bronze_vol`)
  - Base path holding the raw input files for the bronze step.

Tasks
- `fr24_hourly` (bronze)
  - Type: `python_wheel_task`
  - Entry point: `fr24_hourly` (console script from the wheel)
  - Reads raw JSON from `raw_base_path/input_filename`, transforms, and appends to `${catalog}.${schema}.${source_system}_raw` unless `--output-table` is provided.
- `fr24_bronze_to_silver` (silver)
  - Type: `python_wheel_task`, depends on `fr24_hourly`
  - Entry point: `fr24_bronze_to_silver` (console script from the wheel)
  - Reads from bronze table, filters by `run_date_utc`, derives columns, and appends to `${catalog}.${schema}.${source_system}_silver` unless `--output-table` is provided.

Local Usage (optional)
- You can install and run the console scripts locally (requires pyspark and a compatible environment):
  - `python -m pip install dist/mgfi_fr24_etl-<version>-py3-none-any.whl`
  - `fr24_hourly --help`
  - `fr24_bronze_to_silver --help`

Notes & Tips
- When you change code, rebuild the wheel before deploy: `python -m build --wheel`.
- If a cluster appears to keep using an old wheel, bump the version in `pyproject.toml` and update the wheel path in the job YAML, then redeploy.
- The sample job YAML references an existing interactive cluster via `existing_cluster_id`. Replace it with your own or uncomment the `job_clusters` section to let the job create its own cluster.
