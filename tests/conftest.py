import pytest
import pandas as pd
from pathlib import Path

BASE = Path(__file__).parent.parent

@pytest.fixture(scope="module")
def yields_csv():
    return pd.read_csv(BASE / "data" / "yields.csv", parse_dates=["Date"])


@pytest.fixture(scope="module")
def final_df():
    return pd.read_csv(BASE / "data" / "final_dataset.csv", parse_dates=["Date"])
