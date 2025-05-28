def test_yield_jump_limits(final_df):
    diffs = final_df.groupby('Country')['Yield'].diff().abs()
    max_jump = diffs.max()
    assert max_jump < 3.0, f"Yield jump too large: {max_jump}"
    print(max_jump)

def test_rating_flag_consistency(final_df):
    actual_flags = (
        final_df
        .groupby('Country')['Rating']
        .transform(lambda s: s != s.shift(1))
        .fillna(False)
    )
    mismatches = (final_df['RatingChanged'] != actual_flags).sum()
    assert mismatches == 0, f"{mismatches} inconsistent RatingChanged flags"

def test_no_false_negatives_on_rating_change(final_df):
    mask_no_change = final_df["RatingChanged"] == False
    diffs = final_df.loc[mask_no_change, "Rating"] != final_df.loc[mask_no_change, "PrevRating"]
    bad = diffs.sum()
    assert bad == 0, f"{bad} rows flagged unchanged but Rating != PrevRating"
