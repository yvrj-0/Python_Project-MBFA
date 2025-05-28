
def test_yields_vs_final_count(final_df, yields_csv):
    n_countries = final_df["Country"].nunique()
    expected    = len(yields_csv) * n_countries
    actual      = len(final_df)
    assert actual == expected, f"Expected {expected}, got {actual}"

def test_no_duplicate_dates_per_country(final_df):
    dup_count = final_df.duplicated(subset=["Country","Date"]).sum()
    assert dup_count == 0, f"Found {dup_count} duplicate Country+Date pairs"