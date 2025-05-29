import sys
from pathlib import Path
from datetime import date
from typing import Optional
import pandas as pd
import logging
from helpers.path_utils import add_project_root_to_path
from helpers.config_utils import load_config
from helpers.logging_utils import setup_logger
from helpers.scraping_utils import HEADERS, fetch_all_ratings
from helpers.io_utils import save_csv

add_project_root_to_path(__file__, levels_up=2)

def build_ratings_dataset(
    start: str = "2020-01-01",
    end: Optional[str] = None,
    out_csv: Optional[Path] = None
) -> pd.DataFrame:
    """
    Récupère les notes journalières pour les slugs configurés entre deux dates,
    les enregistre au format CSV et renvoie le DataFrame.
    """
    cfg = load_config()
    logger = setup_logger(__name__)
    slugs = cfg["scraper"]["slugs"]
    cty_map = cfg["scraper"]["cty_map"]
    code_map = cfg["scraper"]["country_map"]
    end = date.today().isoformat()

    df = fetch_all_ratings(
        slugs=slugs,
        cty_map=cty_map,
        code_map=code_map,
        start=start,
        end=end,
        headers=HEADERS,
        logger=logger
    )

    if out_csv is None:
        root = Path(__file__).resolve().parents[2]
        out_csv = root / "data" / "ratings_daily.csv"
        out_csv.parent.mkdir(parents=True, exist_ok=True)

    save_csv(df, out_csv)
    logger.info("✓ Ratings saved from %s to %s → %s", start, end, out_csv)
    return df

if __name__ == "__main__":
    build_ratings_dataset()
