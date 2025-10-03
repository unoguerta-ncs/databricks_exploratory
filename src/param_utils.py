from datetime import date
from typing import Optional

def get_param(spark, env: str, key: str, explicit: Optional[str] = None, default: Optional[str] = None):
    try:
        df = spark.read.table("workspace.default.control_parameters")
        row = df.filter((df.env == env) & (df.key == key)).select("value").first()
        if row:
            return row.value
    except Exception:
        pass

    return default

