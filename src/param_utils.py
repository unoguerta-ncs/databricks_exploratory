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

