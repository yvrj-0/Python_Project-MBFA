import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt

@st.cache_data
def load_data():
    """
    lis le csv et fait un traitement préliminaire (date/fillna).

    Returns
    -------
    pandas.DataFrame
    """
    df = pd.read_csv('data/final_dataset.csv', parse_dates=['Date'])
    df['Date'] = pd.to_datetime(df['Date'])
    df['RatingChanged'] = df['RatingChanged'].fillna(False).astype(bool)

    def direction(row):
        """
        Pour chaque changement de rating, détermine s'il sagit d'un downgrade
        ou d'un upgrade.
        :param row = ligne du dataset
        """
        if not row['RatingChanged'] or pd.isna(row['PrevRating']):
            return None
        return 'Upgrade' if row['Rating'] > row['PrevRating'] else 'Downgrade'

    df['Direction'] = df.apply(direction, axis=1)
    return df

def plot_post_event_paths(direction: str, df: pd.DataFrame, window: int = 100) :
    """
    Récupère l’historique des notations pour un pays donné depuis countryeconomy.com.

    Parameters
    ----------
    direction : upgrade/downgrade de la notation
    df : dataframe utilisé
    window : nombre de journées après le changement de notation que l'on veut considérer

    Returns
        Figure Matplotlib
    -------

    """
    fig, ax = plt.subplots(figsize=(10, 6))
    events = df[df['Direction'] == direction]
    for idx, row in events.iterrows():
        country = row['Country']
        event_date = row['Date']
        mask = (df['Country'] == country) & \
               (df['Date'] >= event_date) & \
               (df['Date'] < event_date + pd.Timedelta(days=window))
        series = df.loc[mask, 'Yield']
        if len(series) > 0:
            norm = series / series.iloc[0] * 100
            ax.plot(norm.index.map(lambda i: (df.loc[i, 'Date'] - event_date).days),
                    norm,
                    label=f"{country} @ {event_date.date()}"
                   )
    ax.set_title(f"variation des rendements {window} jours après {direction} de la notation")
    ax.set_xlabel("Jours depuis l'événement")
    ax.set_ylabel("Rendement normalisé (base = 100)")
    ax.legend(fontsize="small", ncol=2)
    ax.grid(True)
    plt.tight_layout()
    return fig, ax
