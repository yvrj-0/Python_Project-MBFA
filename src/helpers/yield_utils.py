import pandas as pd
import yfinance as yf
from pandas_datareader import data as pdr
from datetime import datetime
from typing import Mapping

def get_us_ten_year_rate(symbol: str, start: str, end: str) -> pd.Series:
    df = yf.download(symbol, start=start, end=end, progress=False)
    serie = df.get("Close", df.get("Adj Close"))
    serie.name = symbol
    return serie

def get_foreign_rates(series_map: Mapping[str, str], start: str, end: str) -> pd.DataFrame:
    debut = datetime.fromisoformat(start)
    fin   = datetime.fromisoformat(end)
    out   = pd.DataFrame()
    for label, code in series_map.items():
        try:
            s = pdr.DataReader(code, "fred", debut, fin)
            s.columns = [label]
            out = pd.concat([out, s], axis=1)
        except Exception as e:
            print(f"[WARN] FRED {label} impossible : {e}")
    return out

def compile_all_rates(
    us_map: Mapping[str, str],
    fred_map: Mapping[str, str],
    start: str,
    end: str
) -> pd.DataFrame:
    series = []
    for label, symb in us_map.items():
        try:
            s = get_us_ten_year_rate(symb, start, end)
            s.name = label
            series.append(s)
        except Exception as e:
            print(f"[WARN] US {label} impossible : {e}")

    foreign = get_foreign_rates(fred_map, start, end)
    df_all  = pd.concat(series + [foreign], axis=1)
    bd      = pd.date_range(start=start, end=end, freq="B")
    df_all  = df_all.reindex(bd).ffill()
    df_all.index.name = "Date"
    return df_all

def to_long_format(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df
        .reset_index()
        .melt(id_vars="Date", var_name="Country", value_name="Yield")
        .dropna(subset=["Yield"])
    )
