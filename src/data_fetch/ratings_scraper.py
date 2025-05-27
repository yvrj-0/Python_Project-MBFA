from config import cfg
import logging
import random
import time
from typing import List, Dict

import pandas as pd
import requests
from bs4 import BeautifulSoup

REQ_HEADERS: Dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;"
        "q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8"
    ),
    "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1"
}

CTY_MAP: Dict[str,str] = cfg["scraper"]["cty_map"]

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def retrieve_rating_history(code: str) -> pd.DataFrame:
    key = CTY_MAP.get(code, code)
    url = f"https://countryeconomy.com/ratings/{key}"
    try:
        time.sleep(random.uniform(2, 4))
        resp = requests.get(url, headers=REQ_HEADERS, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        entries: List[Dict[str, str]] = []
        agencies = ["Moody's", "S&P", "Fitch"]
        blocks = soup.find_all("div", class_="table-responsive")[:3]
        for idx, block in enumerate(blocks):
            agency = agencies[idx]
            for row in block.find_all("tr")[1:]:
                cols = row.find_all("td")
                if len(cols) < 2:
                    continue
                dt = cols[0].get_text(strip=True)
                rt = cols[1].get_text(strip=True)
                try:
                    date = pd.to_datetime(dt)
                except ValueError:
                    log.warning("Invalid date %s for %s", dt, code)
                    continue
                if date.year < 2020:
                    continue
                entries.append({
                    "Date": date,
                    "Country": code.replace("-", " ").title(),
                    "Agency": agency,
                    "Rating": rt
                })
        if not entries:
            log.warning("No data for %s", code)
            return pd.DataFrame(columns=["Date", "Country", "Agency", "Rating"])
        df = pd.DataFrame(entries)
        df.sort_values("Date", ascending=False, inplace=True)
        return df
    except requests.RequestException as err:
        log.error("Error for %s: %s", code, err)
        return pd.DataFrame(columns=["Date", "Country", "Agency", "Rating"])

def collect_multiple_ratings(codes: List[str]) -> pd.DataFrame:
    dfs: List[pd.DataFrame] = []
    for code in codes:
        log.info("Fetching %s", code)
        df = retrieve_rating_history(code)
        if not df.empty:
            dfs.append(df)
        else:
            log.warning("Skipping %s", code)
    if not dfs:
        return pd.DataFrame(columns=["Date", "Country", "Agency", "Rating"])
    combined = pd.concat(dfs, ignore_index=True)
    combined.sort_values("Date", ascending=False, inplace=True)
    return combined

if __name__ == "__main__":
    slugs = cfg["scraper"]["slugs"]
    df_all = collect_multiple_ratings(slugs)
    print("Head of combined ratings:")
    print(df_all.head(10).to_string(index=False))
    print("\nCounts by Country:")
    print(df_all["Country"].value_counts())
    print("\nCounts by Agency:")
    print(df_all["Agency"].value_counts())
