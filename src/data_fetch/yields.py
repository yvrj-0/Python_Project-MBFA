import yfinance as yf
import pandas as pd
from pandas_datareader import data as pdr
from datetime import datetime
from typing import Mapping
from config import cfg

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
    return tout.ffill()

if __name__ == "__main__":
    us_args = cfg["args"]["us"]
    euro_args = cfg["args"]["euro"]
    debut = "2015-01-01"
    fin = datetime.today().strftime("%Y-%m-%d")
    df = compile_all_rates(us_args, euro_args, debut, fin)
    print(df.head(10))
