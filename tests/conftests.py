import pytest
import pandas as pd
from src.sql_alchemy import build_final, Session

@pytest.fixture(scope="module")
def final_df():
    session = Session()
    df = build_final(session)
    session.close()
    return df

@pytest.fixture(scope="module")
def yields_csv():
    return pd.read_csv("data/yields.csv", parse_dates=["Date"])

@pytest.fixture(scope="module")
def ratings_csv():
    return pd.read_csv("data/ratings.csv", parse_dates=["Date"])
