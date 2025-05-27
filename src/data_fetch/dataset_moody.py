import sys
import os
from datetime import datetime

import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))

from yields import compile_all_rates
from ratings_scraper import collect_multiple_ratings

def assemble_moodys_dataset(
    start_date: str = "2023-01-01",
    end_date: str = "2023-12-31",
    slugs: list = None
) -> pd.DataFrame:
    us_map = {"US": "^TNX"}
    euro_map = {
        "DE": "IRLTLT01DEM156N",
        "FR": "IRLTLT01FRA156N",
        "IT": "IRLTLT01ITA156N",
    }
    df_yields = compile_all_rates(us_map, euro_map, start_date, end_date)
    df_yields.index.name = "Date"

    if slugs is None:
        slugs = ["united-states", "france", "germany", "italy"]
    df_all = collect_multiple_ratings(slugs)
    df_moodys = df_all[df_all["Agency"] == "Moody's"].copy()
    df_moodys["Country"] = df_moodys["Country"].map({
        "United States": "US",
        "France": "FR",
        "Germany": "DE",
        "Italy": "IT"
    })
    df_moodys = df_moodys[
        (df_moodys["Date"] >= pd.to_datetime(start_date)) &
        (df_moodys["Date"] <= pd.to_datetime(end_date))
    ]

    ratings_pivot = (
        df_moodys
        .pivot(index="Date", columns="Country", values="Rating")
        .reindex(df_yields.index)
        .ffill()
        .reset_index()
        .melt(id_vars="Date", var_name="Country", value_name="Rating")
    )

    ratings_pivot["Prev"] = ratings_pivot.groupby("Country")["Rating"].shift(1)
    ratings_pivot["Change"] = (ratings_pivot["Rating"] != ratings_pivot["Prev"]).fillna(False)

    yields_long = (
        df_yields
        .reset_index()
        .melt(id_vars="Date", var_name="Country", value_name="Yield")
        .dropna(subset=["Yield"])
    )

    return yields_long.merge(ratings_pivot, on=["Date", "Country"], how="left").sort_values(["Country", "Date"])

if __name__ == "__main__":
    df = assemble_moodys_dataset()
    print(df.head(15).to_string(index=False))
    print("\nTotal rows:", len(df))
    changes = df[df["Change"] == True]
    print("\nRating-change events by country:")
    print(changes.groupby("Country").size())
