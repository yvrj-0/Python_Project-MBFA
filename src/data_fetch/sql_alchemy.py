from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Float, Date, Boolean
from sqlalchemy.orm import declarative_base, sessionmaker

HERE     = Path(__file__).resolve().parent
PROJECT  = HERE.parent.parent
DATA_DIR = PROJECT / "data"
DATA_DIR.mkdir(exist_ok=True)

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
    Base.metadata.create_all(ENGINE)

def ingest_csv(table_name, csv_path, session):
    df = pd.read_csv(csv_path, parse_dates=["Date"])
    df["Date"] = df["Date"].dt.date
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

def build_final(session):
    df_r = pd.read_sql(session.query(RatingDaily).statement, session.bind, parse_dates=["Date"])
    df_y = pd.read_sql(session.query(Yield).statement,        session.bind, parse_dates=["Date"])
    df = (
        df_y
        .merge(df_r, how="left", on=["Date","Country"])
        .sort_values(["Country","Date"])
        .dropna(subset=["Yield"])
        .drop_duplicates(subset=["Date","Country","Yield","Agency","Rating","PrevRating","RatingChanged"])
    )
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
    df[["Date","Country","Yield","Agency","Rating","PrevRating","RatingChanged"]].to_csv(
        DATA_DIR/"final_dataset.csv", index=False
    )

def main():
    init_db()
    sess = Session()
    ingest_csv("ratings_daily", DATA_DIR/"ratings_daily.csv", sess)
    ingest_csv("yields",        DATA_DIR/"yields.csv",        sess)
    build_final(sess)
    sess.close()

if __name__ == "__main__":
    main()
