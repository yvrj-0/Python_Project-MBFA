
def test_yields_vs_final_count(final_df, yields_csv):
    """
    Vérifie que la longueur du dataset final est la même que celle du
    dataset des rendements.

    Parameters
    ----------
    final_df : pandas.DataFrame
        DataFrame final issu du pipeline ETL, contenant
        les colonnes "Country", "Yield" et "Date".
    yields_csv : pandas.DataFrame
        DataFrame des rendements, contenant
        les colonnes "Country", "Yield" et "Date".

    Returns
    -------
    None
        Le test lève une AssertionError si les longueurs
        ne sont pas égales.
    """
    expected    = len(yields_csv)
    actual      = len(final_df)
    assert actual == expected, f"Expected {expected}, got {actual}"

def test_no_duplicate_dates_per_country(final_df):
    """
    Vérifie qu'il n'y a pas de lignes dupliquées par pays.

    Parameters
    ----------
    final_df : pandas.DataFrame
        DataFrame final issu du pipeline ETL, contenant
        les colonnes "Country" et "Yield".

    Returns
    -------
    None
        Le test lève une AssertionError si il y a au moins
        une journée dupliquée.
    """
    dup_count = final_df.duplicated(subset=["Country","Date"]).sum()
    assert dup_count == 0, f"Found {dup_count} duplicate Country+Date pairs"

def test_same_unique_dates(final_df, yields_csv):
    """
    Vérifie que les dates des deux datasets sont les mêmes.

    Parameters
    ----------
    final_df : pandas.DataFrame
        DataFrame final issu du pipeline ETL, contenant
        les colonnes "Country" et "Date".
    yields_csv : pandas.DataFrame
        DataFrame des rendements, contenant
        les colonnes "Country" et "Date".

    Returns
    -------
    None
        Le test lève une AssertionError si il manque au moins une
        journée dans un des deux datasets.
    """
    expected = set(yields_csv["Date"].unique())
    actual   = set(final_df["Date"].unique())
    assert actual == expected, f"Dates mismatch: only in yields {expected-actual}, only in final {actual-expected}"

def test_no_missing_yields(final_df):
    """
    Vérifie que la colonne des rendements n'a pas de valeur nulle.

    Parameters
    ----------
    final_df : pandas.DataFrame
        DataFrame final issu du pipeline ETL, contenant
        les colonnes "Country" et "Yield".

    Returns
    -------
    None
        Le test lève une AssertionError si il y au moins une valeur
        "yield" nulle.
    """
    nulls = final_df["Yield"].isna().sum()
    assert nulls == 0, f"Found {nulls} rows with missing yields"

def test_date_bounds(final_df, yields_csv):
    """
    Vérifie que les deux deux datasets ont la même fenêtre temporelle.

    Parameters
    ----------
    final_df : pandas.DataFrame
        DataFrame final issu du pipeline ETL, contenant
        les colonnes "Country", "Yield" et "Date".
    yields_csv : pandas.DataFrame
        DataFrame des rendements, contenant
        les colonnes "Country", "Yield" et "Date".

    Returns
    -------
    None
        Le test lève une AssertionError si les dates de début/fin des datasets
        ne sont pas égales.
    """
    min_y, max_y = yields_csv["Date"].min(), yields_csv["Date"].max()
    min_f, max_f = final_df["Date"].min(),   final_df["Date"].max()
    assert (min_f, max_f) == (min_y, max_y), f"Date range mismatch: yields {min_y}–{max_y}, final {min_f}–{max_f}"

if __name__ == "__main__":
    import pytest
    pytest.main([__file__])
