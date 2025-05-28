from pathlib import Path
import logging
import random
import time
from typing import List, Dict

import pandas as pd
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


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

COUNTRY_MAP: Dict[str, str] = {
    "united-states": "united-states",
    "france":        "france",
    "germany":       "germany",
    "italy":         "italy"
}

def fetch_rating_history(slug: str) -> pd.DataFrame:
    key = COUNTRY_MAP.get(slug, slug)
    url = f"https://countryeconomy.com/ratings/{key}"
    try:
        time.sleep(random.uniform(2, 4))
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        dom = BeautifulSoup(resp.text, "html.parser")

        records: List[Dict[str, str]] = []
        agencies = ["Moody's", "S&P", "Fitch"]
        tables = dom.find_all("div", class_="table-responsive")[:3]

        for idx, table in enumerate(tables):
            agency = agencies[idx]
            for row in table.find_all("tr")[1:]:
                cols = row.find_all("td")
                if len(cols) < 2:
                    continue
                date_text = cols[0].get_text(strip=True)
                rating = cols[1].get_text(strip=True)
                try:
                    date = pd.to_datetime(date_text)
                except ValueError:
                    logger.warning("Date invalide %s pour %s", date_text, slug)
                    continue
                if date.year < 2020:
                    continue
                records.append({
                    "Date": date,
                    "Country": slug.replace("-", " ").title(),
                    "Agency": agency,
                    "Rating": rating
                })

        if not records:
            logger.warning("Aucune donnée pour %s", slug)
            return pd.DataFrame(columns=["Date", "Country", "Agency", "Rating"])

        df = pd.DataFrame(records)
        df.sort_values("Date", ascending=False, inplace=True)
        return df

    except requests.RequestException as e:
        logger.error("Erreur pour %s : %s", slug, e)
        return pd.DataFrame(columns=["Date", "Country", "Agency", "Rating"])

def fetch_all_ratings(slugs: List[str]) -> pd.DataFrame:
    dfs: List[pd.DataFrame] = []
    for slug in slugs:
        logger.info("Traitement de %s", slug)
        df = fetch_rating_history(slug)
        if not df.empty:
            dfs.append(df)
        else:
            logger.warning("Omission de %s", slug)

    if not dfs:
        return pd.DataFrame(columns=["Date", "Country", "Agency", "Rating"])

    result = pd.concat(dfs, ignore_index=True)
    result.sort_values("Date", ascending=False, inplace=True)
    result["PrevRating"] = result.groupby(["Country", "Agency"])["Rating"].shift(1)
    result["RatingChanged"] = (result["Rating"] != result["PrevRating"]).fillna(False)
    return result

if __name__ == "__main__":
    slugs = ["united-states", "france", "germany", "italy"]
    df = fetch_all_ratings(slugs)

    out_dir = Path(__file__).parents[2] / "data"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_csv = out_dir / "ratings.csv"

    df.to_csv(out_csv, index=False)
    print(f"Dataset saved to {out_csv}")
    print(df.head(10).to_string(index=False))
