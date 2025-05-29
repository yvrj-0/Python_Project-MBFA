import pandas as pd
import sys
from etl.models import RatingDaily, Yield
from sqlalchemy.orm import Session

def ingest_csv(table_name, csv_path, session):
    print(f"[DEBUG] → Ingestion de '{table_name}' depuis : {csv_path}")
    if not csv_path.exists():
        print(f"[ERROR] Fichier introuvable : {csv_path}", file=sys.stderr)
        return
    df = pd.read_csv(csv_path, parse_dates=["Date"])
    df["Date"] = df["Date"].dt.date
    print(f"[DEBUG]   {len(df)} lignes lues.")

    if table_name == "ratings_daily":
        objs = [
            RatingDaily(
                Date          = row.Date,
                Country       = row.Country,
                Agency        = row.Agency,
                Rating        = row.Rating,
                PrevRating    = row.PrevRating,
                RatingChanged = bool(row.RatingChanged)
            )
            for row in df.itertuples(index=False)
        ]
    elif table_name == "yields":

        objs = [
            Yield(
                Date=row.Date,
                Country=row.Country,
                Yield=row.Yield,
            )
            for row in df.itertuples(index=False)
        ]

    else:
        objs = []
    session.bulk_save_objects(objs)
    session.commit()
    print(f"[DEBUG]   {len(objs)} enregistrements insérés en base.")
