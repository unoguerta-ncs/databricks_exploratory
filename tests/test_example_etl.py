import subprocess
import sys
import json
from datetime import date, datetime
from pathlib import Path


def run_cmd(cmd: str):
    print(f"\n🚀 Running: {cmd}")
    try:
        start = datetime.now()
        result = subprocess.run(
            cmd,
            shell=True,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        duration = (datetime.now() - start).total_seconds()

        # Try parsing JSON output
        try:
            output_json = json.loads(result.stdout)
            run_id = output_json.get("run_id") or output_json.get("id", "N/A")
            job_id = output_json.get("job_id", "N/A")
            state = output_json.get("state", {}).get("life_cycle_state", "UNKNOWN")
            result_state = output_json.get("state", {}).get("result_state", "UNKNOWN")

            print(f"✅ SUCCESS — Job ID: {job_id}, Run ID: {run_id}")
            print(f"⏱ Duration: {duration:.2f}s")
            print(f"📊 Final State: {state} / {result_state}\n")

        except json.JSONDecodeError:
            print("✅ SUCCESS (non-JSON output)")
            print(result.stdout)
            print(f"⏱ Duration: {duration:.2f}s\n")

    except subprocess.CalledProcessError as e:
        print("❌ FAILED")
        print("STDOUT:\n", e.stdout)
        print("STDERR:\n", e.stderr)


def main():
    today = date.today().strftime("%Y-%m-%d")
    project_root = Path(__file__).resolve().parent.parent

    commands = [
        # 1️⃣ Basic bundle run (no params)
        "databricks bundle run example_etl",

        # 2️⃣ Bundle run with explicit params
        f"databricks bundle run example_etl --target dev --params run_date={today} --params input_filename=input_prd.csv",

        # 3️⃣ Direct job run-now with job_id
        "databricks jobs run-now 568231396830094",

        # 4️⃣ JSON-based run-now call (relative to project root)
        f"databricks jobs run-now --json @{project_root / 'run-now.json'}",
    ]

    for cmd in commands:
        run_cmd(cmd)


if __name__ == "__main__":
    print(f"🧪 Databricks Job Test Runner ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")
    if sys.platform.startswith("win"):
        print("🪟 Detected Windows environment")
    else:
        print("🐧 Detected non-Windows environment")
    main()
