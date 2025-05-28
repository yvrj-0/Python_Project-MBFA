from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Boolean
from sqlalchemy.orm import declarative_base, sessionmaker

HERE        = Path(__file__).resolve().parent
PROJECT     = HERE.parent.parent
DATA        = PROJECT / "data"
DATA.mkdir(parents=True, exist_ok=True)

DB_FILE     = DATA / "mbfa.db"
RATINGS_CSV = DATA / "ratings.csv"
YIELDS_CSV  = DATA / "yields.csv"
FINAL_CSV   = DATA / "final_dataset.csv"

engine  = create_engine(f"sqlite:///{DB_FILE}", future=True)
Session = sessionmaker(bind=engine, future=True)
Base    = declarative_base()

class Rating(Base):
    __tablename__ = "ratings"
    id      = Column(Integer, primary_key=True)
    Date    = Column(DateTime, nullable=False, index=True)
    Country = Column(String, nullable=False, index=True)
    Agency  = Column(String, nullable=False)
    Rating  = Column(String, nullable=False)
    Prev    = Column(String)
    Change  = Column(Boolean)

class Yield(Base):
    __tablename__ = "yields"
    id      = Column(Integer, primary_key=True)
    Date    = Column(DateTime, nullable=False, index=True)
    Country = Column(String, nullable=False, index=True)
    Yield   = Column(Float, nullable=False)

class FinalDataset(Base):
    __tablename__ = "final_dataset"
    id            = Column(Integer, primary_key=True)
    Date          = Column(DateTime, nullable=False, index=True)
    Country       = Column(String, nullable=False, index=True)
    Agency        = Column(String)
    Yield         = Column(Float)
    Rating        = Column(String)
    PrevRating    = Column(String)
    RatingChanged = Column(Boolean)
    YieldPrev     = Column(Float)
    YieldNext     = Column(Float)

def init_db():
    Base.metadata.create_all(engine)

def ingest_csv(table, path, session):
    df = pd.read_csv(path, parse_dates=["Date"]).dropna(subset=["Date"])
    if table == "ratings":
        df.sort_values(["Country","Agency","Date"], inplace=True)
        df["Prev"]   = df.groupby(["Country","Agency"])["Rating"].shift(1)
        df["Change"] = (df["Rating"] != df["Prev"]).fillna(False)
        df = df.where(pd.notna(df), None)
        records = [
            Rating(Date=row.Date,
                   Country=row.Country,
                   Agency=row.Agency,
                   Rating=row.Rating,
                   Prev=row.Prev,
                   Change=bool(row.Change))
            for row in df.itertuples(index=False)
        ]
    elif table == "yields":
        records = [
            Yield(Date=row.Date,
                  Country=row.Country,
                  Yield=row.Yield)
            for row in df.itertuples(index=False)
        ]
    else:
        raise ValueError(f"Table '{table}' inconnue")
    session.bulk_save_objects(records)
    session.commit()
    print(f"{len(records):,} lignes insérées dans '{table}'")

def build_final_table(session):
    df_r = pd.read_sql(session.query(Rating).statement, session.bind, parse_dates=["Date"])
    df_y = pd.read_sql(session.query(Yield).statement,   session.bind, parse_dates=["Date"])
    merged = (
        df_y
        .merge(df_r, on=["Date","Country"], how="left")
        .sort_values(["Country","Date"])
    )
    merged["YieldPrev"]     = merged.groupby("Country")["Yield"].shift(1)
    merged["YieldNext"]     = merged.groupby("Country")["Yield"].shift(-1)
    merged["PrevRating"]    = merged["Prev"]
    merged["RatingChanged"] = merged["Change"].fillna(False).astype(bool)
    final = merged[[
        "Date","Country","Agency","Yield","Rating",
        "PrevRating","RatingChanged","YieldPrev","YieldNext"
    ]].where(pd.notna(merged), None)
    session.bulk_save_objects(
        [FinalDataset(**row._asdict()) for row in final.itertuples(index=False)]
    )
    session.commit()
    print(f"`final_dataset` créé avec {len(final):,} lignes")
    return final

def main():
    init_db()
    session = Session()
    ingest_csv("ratings", RATINGS_CSV, session)
    ingest_csv("yields",  YIELDS_CSV,  session)
    df_final = build_final_table(session)
    df_final.to_csv(FINAL_CSV, index=False)
    session.close()

if __name__ == "__main__":
    main()
