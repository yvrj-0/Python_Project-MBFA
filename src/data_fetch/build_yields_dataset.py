import os
import yaml
import yfinance as yf
import pandas as pd
from pandas_datareader import data as pdr
from datetime import datetime, date
from typing import Mapping

HERE         = os.path.abspath(os.path.dirname(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(HERE, os.pardir, os.pardir))
cfg = yaml.safe_load(open(os.path.join(PROJECT_ROOT, "src", "config.yaml"), encoding="utf-8"))

def get_us_ten_year_rate(symbol: str, start: str, end: str) -> pd.Series:
    data = yf.download(symbol, start=start, end=end, progress=False)
    serie = data["Close"] if "Close" in data else data["Adj Close"]
    serie.name = "US_10Y"
    return serie

def get_foreign_rates(series_map: Mapping[str, str], start: str, end: str) -> pd.DataFrame:
    debut = datetime.fromisoformat(start)
    fin = datetime.fromisoformat(end)
    tableau = pd.DataFrame()
    for pays, code in series_map.items():
        try:
            s = pdr.DataReader(code, "fred", debut, fin)
            s.columns = [pays]
            tableau = pd.concat([tableau, s], axis=1)
        except Exception as err:
            print(f"Erreur pour {pays} ({code}) : {err}")
    return tableau

def compile_all_rates(us_config: Mapping[str, str], fred_config: Mapping[str, str], start: str, end: str) -> pd.DataFrame:
    listes = []
    for label, symb in us_config.items():
        try:
            listes.append(get_us_ten_year_rate(symb, start, end))
        except Exception as err:
            print(f"Problème US ({label}) : {err}")
    autres = get_foreign_rates(fred_config, start, end)
    tout = pd.concat(listes + [autres], axis=1)
    jours = pd.date_range(start=start, end=end, freq="B")
    tout = tout.reindex(jours)
    tout.index.name = "Date"
    return tout.ffill()

def build_yields_dataset(start="2020-01-01", end=None, out_csv=None) -> pd.DataFrame:
    if end is None:
        end = date.today().isoformat()
    if out_csv is None:
        out_csv = os.path.join(PROJECT_ROOT, "data", "yields.csv")

    df = compile_all_rates(
        cfg["yields"]["us_map"],
        cfg["yields"]["euro_map"],
        start, end
    )

    df_long = df.reset_index().melt(
        id_vars="Date", var_name="Country", value_name="Yield"
    ).dropna(subset=["Yield"])

    inv_us = {v: k for k, v in cfg["yields"]["us_map"].items()}
    df_long["Country"] = df_long["Country"].replace(inv_us)

    os.makedirs(os.path.dirname(out_csv), exist_ok=True)
    df_long.to_csv(out_csv, index=False)
    print(f"✓ Yields saved from {start} to {end} → {out_csv}")
    return df_long

if __name__ == "__main__":
    build_yields_dataset()
