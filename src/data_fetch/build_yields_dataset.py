from pathlib import Path
from datetime import datetime
from typing import Mapping
import yaml
import pandas as pd
import yfinance as yf
from pandas_datareader import data as pdr

BASE_DIR = Path(__file__).resolve().parents[2]
CONFIG_FILE = BASE_DIR / "src" / "config.yaml"
DATA_DIR = BASE_DIR / "data"
OUTPUT_FILE = DATA_DIR / "yields.csv"

with CONFIG_FILE.open("r", encoding="utf-8") as f:
    config = yaml.safe_load(f)

START = "2020-01-01"
END = datetime.today().strftime("%Y-%m-%d")

def fetch_us_rate(symbol: str, start: str, end: str) -> pd.Series:
    df = yf.download(symbol, start=start, end=end, progress=False)
    series = df["Close"] if "Close" in df else df["Adj Close"]
    series.name = symbol
    return series

def fetch_euro_rates(mapping: Mapping[str, str], start: str, end: str) -> pd.DataFrame:
    start_dt = datetime.fromisoformat(start)
    end_dt = datetime.fromisoformat(end)
    df = pd.DataFrame()
    for country, code in mapping.items():
        try:
            s = pdr.DataReader(code, "fred", start_dt, end_dt)
            s.columns = [country]
            df = pd.concat([df, s], axis=1)
        except Exception as e:
            print(f"FRED error for {country}: {e}")
    return df

def compile_rates(
    us_map: Mapping[str, str],
    euro_map: Mapping[str, str],
    start: str,
    end: str
) -> pd.DataFrame:
    parts = []
    for label, sym in us_map.items():
        try:
            s = fetch_us_rate(sym, start, end)
            s.name = label
            parts.append(s)
        except Exception as e:
            print(f"US error for {label}: {e}")
    parts.append(fetch_euro_rates(euro_map, start, end))
    df = pd.concat(parts, axis=1)
    idx = pd.date_range(start=start, end=end, freq="B")
    df = df.reindex(idx).ffill()
    df.index.name = "Date"
    return df

def build_yields_dataset() -> pd.DataFrame:
    df = compile_rates(
        config["yields"]["us_map"],
        config["yields"]["euro_map"],
        START,
        END
    )
    df_long = (
        df.reset_index()
          .melt(id_vars="Date", var_name="Country", value_name="Yield")
          .dropna(subset=["Yield"])
    )
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    df_long.to_csv(OUTPUT_FILE, index=False)
    print(f"Yields dataset ({START}→{END}) saved to {OUTPUT_FILE}")
    return df_long

if __name__ == "__main__":
    build_yields_dataset()
