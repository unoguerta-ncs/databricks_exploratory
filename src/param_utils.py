from datetime import date
from typing import Optional


def _is_unresolved_template(value: Optional[str]) -> bool:
    """Return True if the provided string looks like an unresolved template (e.g., "{{ ... }}")."""
    if value is None:
        return False
    s = str(value).strip()
    return s.startswith("{{") and s.endswith("}}")


def get_param(spark, env: str, key: str, explicit: Optional[str] = None, default: Optional[str] = None):
    """
    Resolve a parameter with priority:
    1) Explicitly passed value (unless looks like an unresolved template)
    2) Control table (workspace.default.control_parameters)
    3) Provided default

    The control table is expected to have schema: env STRING, key STRING, value STRING.
    """
    # Treat unresolved template-looking strings as not provided
    if explicit and not _is_unresolved_template(explicit):
        return explicit

    # Attempt to read from control table; if table does not exist, fall back to default
    try:
        df = spark.read.table("workspace.default.control_parameters")
        row = df.filter((df.env == env) & (df.key == key)).select("value").first()
        if row:
            return row.value
    except Exception:
        # Silently fall back if table unavailable; logging can be added by callers if desired
        pass

    return default



def _hadoop_rm_recursive(spark, path: str) -> None:
    """Remove a path recursively using Hadoop FS (works without dbutils)."""
    try:
        jvm = spark._jvm  # type: ignore[attr-defined]
        jsc = spark._jsc  # type: ignore[attr-defined]
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(jsc.hadoopConfiguration())
        fs.delete(jvm.org.apache.hadoop.fs.Path(path), True)
    except Exception:
        # Best-effort removal; callers want to be resilient if cleanup isn't possible
        pass


def prepare_parent_date_dir(spark, parent_date_dir: str) -> None:
    """
    Ensure the date directory can hold subfolders (e.g., Python, Notebook).

    If a previous run wrote a Delta table directly at `parent_date_dir`, that
    directory will contain a `_delta_log` and block creating subfolders.
    In that case, delete `parent_date_dir` so the job can create subdirectories.

    This uses Delta Lake detection and Hadoop FS deletion to avoid relying on dbutils.
    """
    try:
        try:
            from delta.tables import DeltaTable  # type: ignore

            is_delta = bool(DeltaTable.isDeltaTable(spark, parent_date_dir))
        except Exception:
            # Fallback: detect _delta_log with Hadoop FS
            jvm = spark._jvm  # type: ignore[attr-defined]
            jsc = spark._jsc  # type: ignore[attr-defined]
            fs = jvm.org.apache.hadoop.fs.FileSystem.get(jsc.hadoopConfiguration())
            path = jvm.org.apache.hadoop.fs.Path(parent_date_dir + "/_delta_log")
            is_delta = fs.exists(path)

        if is_delta:
            _hadoop_rm_recursive(spark, parent_date_dir)
    except Exception:
        # Swallow exceptions to keep ETL resilient; logging can be added by caller
        pass
