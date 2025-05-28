def test_yield_jump_limits(final_df):
    diffs = final_df.groupby('Country')['Yield'].diff().abs()
    max_jump = diffs.max()
    assert max_jump < 3.0, f"Yield jump too large: {max_jump}"
    print(max_jump)


def test_no_false_negatives_on_rating_change(final_df):
    mask_no_change = final_df["RatingChanged"] == False
    diffs = final_df.loc[mask_no_change, "Rating"] != final_df.loc[mask_no_change, "PrevRating"]
    bad = diffs.sum()
    assert bad == 0, f"{bad} rows flagged unchanged but Rating != PrevRating"

if __name__ == "__main__":
    import pytest
    pytest.main([__file__])
