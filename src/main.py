from datetime import datetime
import pandas as pd

from data_fetch.yields import compile_all_rates
from data_fetch.ratings_scraper import collect_multiple_ratings

def build_2023_dataset():
    # 1) Récupérer les rendements pour 2023
    us_args = {"US": "^TNX"}
    eur_args = {
        "DE": "IRLTLT01DEM156N",
        "FR": "IRLTLT01FRA156N",
        "IT": "IRLTLT01ITA156N",
    }
    start = "2023-01-01"
    end = "2023-12-31"
    df_yields = compile_all_rates(us_args, eur_args, start, end)

    # 2) Récupérer les notations pour 2023
    slugs = ["united-states", "france", "germany", "italy"]
    df_ratings = collect_multiple_ratings(slugs)

    # 3) Filtrer les notations 2023 et préparer pivot Moody's
    df_ratings = df_ratings[(df_ratings["Date"] >= pd.to_datetime(start)) &
                            (df_ratings["Date"] <= pd.to_datetime(end))]
    df_moodys = df_ratings[df_ratings["Agency"] == "Moody's"]
    df_pivot = (
        df_moodys
        .pivot(index="Date", columns="Country", values="Rating")
        .reindex(df_yields.index)
        .ffill()
        .rename(columns={
            "United States": "US",
            "Germany": "DE",
            "France": "FR",
            "Italy": "IT"
        })
    )

    # 4) Fusionner yields et notations
    df_dataset = pd.concat([df_yields, df_pivot], axis=1)

    # 5) Retourner le dataset final
    return df_dataset

if __name__ == "__main__":
    df_2023 = build_2023_dataset()
    print(df_2023.head())
    df_2023.to_csv("dataset_2023.csv", index=True)

