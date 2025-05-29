# src/helpers/io_utils.py

from pathlib import Path
import pandas as pd

def save_csv(df: pd.DataFrame, out_path: Path) -> None:
    """
    Crée le dossier parent si besoin et écrit df au format CSV (sans index).
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
