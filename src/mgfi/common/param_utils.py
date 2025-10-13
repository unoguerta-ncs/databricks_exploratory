from typing import Optional


def get_param(
    spark,
    env: str,
    key: str,
    explicit: Optional[str] = None,
    default: Optional[str] = None,
):
    # 1) If the caller provided an explicit value, use it
    if explicit is not None and str(explicit).strip() != "":
        return explicit

    # 2) Otherwise try fetch from the shared control table (best-effort)
    try:
        df = spark.read.table("mgfi_catalog_test.sandbox.control_parameters")
        row = df.filter((df.env == env) & (df.key == key)).select("value").first()
        if row:
            return row.value
    except Exception:
        # If table doesn't exist or is unreadable, fall through to default
        pass

    # 3) Fallback to the supplied default
    return default

