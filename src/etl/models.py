from sqlalchemy import Column, Integer, String, Float, Date, Boolean
from etl.session import Base, Session

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