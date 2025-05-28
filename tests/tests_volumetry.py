
def test_yields_vs_final_count(final_df, yields_csv):
    expected    = len(yields_csv)
    actual      = len(final_df)
    assert actual == expected, f"Expected {expected}, got {actual}"

def test_no_duplicate_dates_per_country(final_df):
    dup_count = final_df.duplicated(subset=["Country","Date"]).sum()
    assert dup_count == 0, f"Found {dup_count} duplicate Country+Date pairs"

def test_same_unique_dates(final_df, yields_csv):
    expected = set(yields_csv["Date"].unique())
    actual   = set(final_df["Date"].unique())
    assert actual == expected, f"Dates mismatch: only in yields {expected-actual}, only in final {actual-expected}"

def test_no_missing_yields(final_df):
    nulls = final_df["Yield"].isna().sum()
    assert nulls == 0, f"Found {nulls} rows with missing yields"

def test_date_bounds(final_df, yields_csv):
    min_y, max_y = yields_csv["Date"].min(), yields_csv["Date"].max()
    min_f, max_f = final_df["Date"].min(),   final_df["Date"].max()
    assert (min_f, max_f) == (min_y, max_y), f"Date range mismatch: yields {min_y}–{max_y}, final {min_f}–{max_f}"

if __name__ == "__main__":
    import pytest
    pytest.main([__file__])
