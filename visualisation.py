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
selected_country = st.sidebar.selectbox('Pays', countries)
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
