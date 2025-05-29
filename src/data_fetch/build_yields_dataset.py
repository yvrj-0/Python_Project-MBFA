
import sys
import pandas as pd
from pathlib import Path
from datetime import date

from src.helpers.path_utils import add_project_root_to_path
add_project_root_to_path(__file__, levels_up=2)

from src.helpers.config_utils import load_config
from src.helpers.yield_utils import compile_all_rates, to_long_format
from src.helpers.io_utils import save_csv

def build_yields_dataset(
    start: str = "2020-01-01",
    end: str | None = None,
    out_csv: Path | None = None
) -> pd.DataFrame:
    """
    Génère un jeu de données de rendements entre deux dates,
    l'enregistre au format CSV et renvoie le DataFrame au format long.
    """
    cfg = load_config()
    if end is None:
        end = date.today().isoformat()
    if out_csv is None:
        root = Path(__file__).resolve().parents[2]
        out_csv = root / "data" / "yields.csv"
        out_csv.parent.mkdir(parents=True, exist_ok=True)
    df = compile_all_rates(cfg["yields"]["us_map"], cfg["yields"]["euro_map"], start, end)
    df_long = to_long_format(df)
    df_long["Country"] = df_long["Country"].replace("^TNX", "US")
    save_csv(df_long, out_csv)
    print(f"✓ Yields saved from {start} to {end} → {out_csv}")
    return df_long

if __name__ == "__main__":
    build_yields_dataset()
