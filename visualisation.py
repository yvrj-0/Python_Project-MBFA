import streamlit as st
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

st.set_page_config(page_title='Impact des changements de notation', layout='wide')

@st.cache_data
def load_data():
    df = pd.read_csv('data/final_dataset.csv', parse_dates=['Date'])
    df['Date'] = pd.to_datetime(df['Date'])
    df['RatingChanged'] = df['RatingChanged'].fillna(False).astype(bool)

    def direction(row):
        if not row['RatingChanged'] or pd.isna(row['PrevRating']):
            return None
        return 'Upgrade' if row['Rating'] > row['PrevRating'] else 'Downgrade'

    df['Direction'] = df.apply(direction, axis=1)
    return df

df = load_data()

st.title("Impact des changements de notation sur les rendements obligataires")

countries = sorted(df['Country'].unique())
selected_country = st.selectbox('Sélectionnez un pays', countries)
df_country = df[df['Country'] == selected_country]

st.subheader(f"Série temporelle des rendements pour {selected_country}")
fig, ax = plt.subplots(figsize=(12, 6))
sns.lineplot(data=df_country, x='Date', y='Yield', ax=ax, label='Yield', color='blue')
up = df_country[df_country['Direction'] == 'Upgrade']
dl = df_country[df_country['Direction'] == 'Downgrade']
ax.scatter(up['Date'], up['Yield'], color='green', label='Upgrade', zorder=5)
ax.scatter(dl['Date'], dl['Yield'], color='red', label='Downgrade', zorder=5)
ax.set_xlabel('Date')
ax.set_ylabel('Rendement (%)')
ax.legend()
st.pyplot(fig)

st.subheader('Tableau des statistiques par pays')
st.markdown('Moyenne, variance et écart-type du rendement pour chaque pays')
stats = (
    df.groupby('Country')['Yield']
      .agg(['mean', 'var', 'std'])
      .round(4)
      .rename(columns={'mean':'Moyenne', 'var':'Variance', 'std':'Écart-type'})
)
st.dataframe(stats)

st.markdown("### Aperçu des données")
st.write("**Dimension (lignes, colonnes) :**", df.shape)
st.dataframe(df.head(-1))

descr = df.describe(include="all").round(4)
descr = descr.drop(index=["top", "freq"], errors="ignore")
dtypes = df.dtypes.astype(str).to_frame(name="dtype")
descr = pd.concat([dtypes, descr])
st.markdown("### Statistiques descriptives")
st.dataframe(descr)

st.subheader("Densité estimée des rendements")
fig, ax = plt.subplots()
sns.kdeplot(df["Yield"], fill=True, ax=ax)
ax.set_xlabel("Yield (%)")
st.pyplot(fig)

st.subheader("Boxplots des rendements par pays")
fig, ax = plt.subplots(figsize=(12,6))
sns.boxplot(data=df, x="Country", y="Yield", ax=ax)
ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha="right")
st.pyplot(fig)

st.subheader("Violin plots des rendements par pays")
fig, ax = plt.subplots(figsize=(12,6))
sns.violinplot(data=df, x="Country", y="Yield", inner="quartile", ax=ax)
ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha="right")
st.pyplot(fig)

def plot_post_event_paths(direction: str, df: pd.DataFrame, window: int = 100):
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

st.title("Impact des changements de notation – Analyse post-événement")
direction = st.radio("Type d'événement", ("Upgrade", "Downgrade"))
fig, ax = plot_post_event_paths(direction, df, window=100)
st.pyplot(fig)
