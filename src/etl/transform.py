import pandas as pd
from etl.models import FinalDataset, RatingDaily, Yield
from etl.session import DATA_DIR


def build_final(session):
    print("[DEBUG] → Construction du jeu final…")
    df_r = pd.read_sql(session.query(RatingDaily).statement, session.bind, parse_dates=["Date"])
    df_y = pd.read_sql(session.query(Yield).statement,        session.bind, parse_dates=["Date"])
    print(f"[DEBUG]   {len(df_y)} rendements, {len(df_r)} notations récupérées.")

    df = (
        df_y
        .merge(df_r, how="left", on=["Date","Country"])
        .sort_values(["Country","Date"])
        .dropna(subset=["Yield"])
        .drop_duplicates(subset=["Date","Country","Yield","Agency","Rating","PrevRating","RatingChanged"])
    )
    print(f"[DEBUG]   {len(df)} lignes après fusion/nettoyage.")

    objs = [
        FinalDataset(
            Date          = row.Date,
            Country       = row.Country,
            Agency        = row.Agency,
            Rating        = row.Rating,
            PrevRating    = row.PrevRating,
            RatingChanged = bool(row.RatingChanged) if row.RatingChanged is not None else False,
            Yield         = float(row.Yield)
        )
        for row in df.itertuples(index=False)
    ]
    session.bulk_save_objects(objs)
    session.commit()
    print(f"[DEBUG]   {len(objs)} enregistrements finaux insérés en base.")

    out_csv = DATA_DIR / "final_dataset.csv"
    df[["Date","Country","Yield","Agency","Rating","PrevRating","RatingChanged"]].to_csv(out_csv, index=False)
    print(f"[DEBUG]   Export CSV final sous : {out_csv}")
    df = df.drop(columns=["id_x", "id_y"])
    return df