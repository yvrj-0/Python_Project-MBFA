import time
import random
import logging
from typing import List, Dict, Optional
from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup

from .logging_utils import setup_logger

HEADERS: Dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
    "Connection": "keep-alive"
}

def fetch_rating_history(
    slug: str,
    cty_map: Dict[str, str],
    start: str,
    headers: Dict[str, str] = HEADERS,
    logger: Optional[logging.Logger] = None
) -> pd.DataFrame:
    """
    Pour un slug, va chercher sur countryeconomy.com,
    extrait les 3 tableaux Moody's/S&P/Fitch,
    ne garde que les dates >= start, et renvoie un DataFrame.
    """
    if logger is None:
        logger = setup_logger(__name__)

    key = cty_map.get(slug, slug)
    url = f"https://countryeconomy.com/ratings/{key}"
    time.sleep(random.uniform(2, 4))

    try:
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        records: List[Dict[str, object]] = []
        agencies = ["Moody's", "S&P", "Fitch"]
        tables = soup.find_all("div", class_="table-responsive")[:3]

        for idx, table in enumerate(tables):
            agency = agencies[idx]
            for row in table.find_all("tr")[1:]:
                cols = row.find_all("td")
                if len(cols) < 2:
                    continue
                date_txt = cols[0].get_text(strip=True)
                rating = cols[1].get_text(strip=True)
                try:
                    dt = pd.to_datetime(date_txt)
                except ValueError:
                    logger.warning("Date invalide %s pour %s", date_txt, slug)
                    continue
                if dt < pd.to_datetime(start):
                    continue
                records.append({
                    "Date": dt,
                    "Country": slug.replace("-", " ").title(),
                    "Agency": agency,
                    "Rating": rating,
                })

        if not records:
            logger.warning("Aucune donnée pour %s", slug)
            return pd.DataFrame(columns=["Date", "Country", "Agency", "Rating"])

        df = pd.DataFrame(records)
        df.sort_values("Date", ascending=False, inplace=True)
        return df

    except requests.RequestException as e:
        logger.error("Erreur HTTP pour %s : %s", slug, e)
        return pd.DataFrame(columns=["Date", "Country", "Agency", "Rating"])


def fetch_all_ratings(
    slugs: List[str],
    cty_map: Dict[str, str],
    code_map: Dict[str, str],
    start: str,
    end: str,
    headers: Dict[str, str] = HEADERS,
    logger: Optional[logging.Logger] = None
) -> pd.DataFrame:
    """
    Itère sur les slugs, concatène les DataFrames non vides,
    reindexe sur jours ouvrés et complète l’historique.
    """
    if logger is None:
        logger = setup_logger(__name__)

    dfs: List[pd.DataFrame] = []
    for slug in slugs:
        logger.info("▶ scraping %s", slug)
        df = fetch_rating_history(slug, cty_map, start, headers, logger)
        if df.empty:
            logger.warning("Omission de %s", slug)
        else:
            dfs.append(df)

    if not dfs:
        return pd.DataFrame(columns=["Date", "Country", "Agency", "Rating"])

    events = pd.concat(dfs, ignore_index=True)
    events = events[events["Date"] >= pd.to_datetime(start)]
    events["Country"] = events["Country"].map(code_map)

    bd = pd.date_range(start=start, end=end, freq="B")
    parts: List[pd.DataFrame] = []
    for code, grp in events.groupby("Country"):
        grp = grp.sort_values("Date")[["Date", "Agency", "Rating"]]
        df0 = pd.DataFrame(index=bd).rename_axis("Date")
        df0["Country"] = code
        merged = pd.merge_asof(
            df0.reset_index(), grp, on="Date", direction="backward"
        ).set_index("Date")
        first = grp["Date"].min()
        merged = merged.loc[first:]
        merged["Agency"] = merged["Agency"].ffill()
        merged["Rating"] = merged["Rating"].ffill()
        merged["PrevRating"] = merged["Rating"].shift(1)
        merged["RatingChanged"] = (merged["Rating"] != merged["PrevRating"]).fillna(False)
        parts.append(merged.reset_index())

    return pd.concat(parts, ignore_index=True)
