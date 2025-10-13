import os
from typing import Optional


def _is_non_empty(value: Optional[str]) -> bool:
    return value is not None and str(value).strip() != ""


def get_param(
    spark,
    env: str,
    key: str,
    explicit: Optional[str] = None,
    default: Optional[str] = None,
    control_table: Optional[str] = "",
):
    # 1) If the caller provided an explicit value, use it
    if _is_non_empty(explicit):
        return explicit

    # 2) Otherwise try fetch from the shared control table (best-effort)
    try:
        if _is_non_empty(control_table):
            df = spark.read.table(control_table)
            row = df.filter((df.env == env) & (df.key == key)).select("value").first()
            if row:
                return row.value
    except Exception:
        # If table doesn't exist or is unreadable, fall through to default
        pass

    # 3) Fallback to the supplied default
    return default



def resolve_run_date_utc(
    spark,
    env: str,
    explicit_run_date_utc: Optional[str] = None,
    upstream_task_key: Optional[str] = None,
    control_table: Optional[str] = "",
) -> str:
    """Resolve run_date_utc with priority order:

    1) Explicit argument (if non-empty)
    2) Databricks Jobs taskValues from the upstream task (default key 'etl_task')
    3) Control table via get_param(key='run_date_utc')

    Raises click.ClickException if not resolved (no default).
    """
    # 1) explicit override
    if _is_non_empty(explicit_run_date_utc):
        return explicit_run_date_utc  # type: ignore[return-value]

    # 2) try Jobs task values
    task_key = upstream_task_key or os.getenv("UPSTREAM_TASK_KEY", "etl_task")
    try:
        from pyspark.dbutils import DBUtils  # type: ignore

        dbutils = DBUtils(spark)
        value = dbutils.jobs.taskValues.get(
            taskKey=task_key, key="run_date_utc", default=None
        )
        if _is_non_empty(value):
            return value  # type: ignore[return-value]
    except Exception:
        # Not running in Jobs or no value set
        pass

    # 3) control table fallback
    value = get_param(
        spark,
        env=env,
        key="run_date_utc",
        explicit=None,
        default=None,
        control_table=control_table,
    )
    if _is_non_empty(value):
        return value  # type: ignore[return-value]

    import click  # local import to avoid hard dependency for callers not using click

    raise click.ClickException(
        "run_date_utc is required and was not provided via CLI, task values, or control table"
    )


def resolve_task_value(
    spark,
    key: str,
    upstream_task_key: Optional[str] = None,
) -> Optional[str]:
    """Fetch a value from Databricks Jobs taskValues for a given key.

    Returns None when not running in Jobs or when the key is missing/empty.
    """
    task_key = upstream_task_key or os.getenv("UPSTREAM_TASK_KEY", "etl_task")
    try:
        from pyspark.dbutils import DBUtils  # type: ignore

        dbutils = DBUtils(spark)
        value = dbutils.jobs.taskValues.get(taskKey=task_key, key=key, default=None)
        return value if _is_non_empty(value) else None
    except Exception:
        return None


def resolve_batch_id(
    spark,
    upstream_task_key: Optional[str] = None,
) -> Optional[str]:
    """Convenience helper to fetch 'batch_id' from upstream task values."""
    return resolve_task_value(spark, key="batch_id", upstream_task_key=upstream_task_key)
