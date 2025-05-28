import subprocess
from pathlib import Path

HERE = Path(__file__).resolve().parent

SCRIPTS_DIR = HERE / "data_fetch"

for script in [
    "ratings_dataset.py",
    "build_yields_dataset.py",
    "sql_alchemy.py",
]:
    script_path = SCRIPTS_DIR / script
    print(f"\n--- Running {script} ---")
    subprocess.run(
        ["python", str(script_path)],
        check=True
    )

