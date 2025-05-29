from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Float, Date, Boolean
from sqlalchemy.orm import declarative_base, sessionmaker
import sys

# Répertoires
HERE     = Path(__file__).resolve().parent        # …/Python_Project-MBFA/src
PROJECT  = HERE.parent                            # …/Python_Project-MBFA
DATA_DIR = PROJECT / "data"
DATA_DIR.mkdir(exist_ok=True)

print(f"[DEBUG] __file__       = {__file__}")
print(f"[DEBUG] HERE           = {HERE}")
print(f"[DEBUG] PROJECT        = {PROJECT}")
print(f"[DEBUG] DATA_DIR       = {DATA_DIR.resolve()}")

# Base de données
DB_PATH = DATA_DIR / "mbfa.db"
ENGINE  = create_engine(f"sqlite:///{DB_PATH}", future=True)
Session = sessionmaker(bind=ENGINE, future=True)
Base    = declarative_base()

class RatingDaily(Base):
    __tablename__ = "ratings_daily"
    id            = Column(Integer, primary_key=True)
    Date          = Column(Date,    nullable=False, index=True)
    Country       = Column(String,  nullable=False, index=True)
    Agency        = Column(String,  nullable=False)
    Rating        = Column(String,  nullable=False)
    PrevRating    = Column(String)
    RatingChanged = Column(Boolean, nullable=False)

class Yield(Base):
    __tablename__ = "yields"
    id      = Column(Integer, primary_key=True)
    Date    = Column(Date,    nullable=False, index=True)
    Country = Column(String,  nullable=False, index=True)
    Yield   = Column(Float,   nullable=False)

class FinalDataset(Base):
    __tablename__ = "final_dataset"
    id            = Column(Integer, primary_key=True)
    Date          = Column(Date,    nullable=False, index=True)
    Country       = Column(String,  nullable=False, index=True)
    Agency        = Column(String)
    Rating        = Column(String)
    PrevRating    = Column(String)
    RatingChanged = Column(Boolean)
    Yield         = Column(Float)

def init_db():
    print("[DEBUG] Création des tables en base si nécessaire…")
    Base.metadata.create_all(ENGINE)
    print("[DEBUG] Tables prêtes.")

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
    else:
        objs = [
            Yield(Date=row.Date, Country=row.Country, Yield=row.Yield)
            for row in df.itertuples(index=False)
        ]

    session.bulk_save_objects(objs)
    session.commit()
    print(f"[DEBUG]   {len(objs)} enregistrements insérés en base.")

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

def main():
    try:
        init_db()
        sess = Session()
        ingest_csv("ratings_daily", DATA_DIR / "ratings_daily.csv", sess)
        ingest_csv("yields",        DATA_DIR / "yields.csv",        sess)
        build_final(sess)
        sess.close()
        print("[DEBUG] Traitement terminé avec succès.")
    except Exception as e:
        print(f"[EXCEPTION] {e}", file=sys.stderr)
        raise

if __name__ == "__main__":
    main()
