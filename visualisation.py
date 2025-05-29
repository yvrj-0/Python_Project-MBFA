import streamlit as st
import seaborn as sns
import matplotlib.pyplot as plt
from src.helpers.visualisation_utils import load_data
from src.helpers.visualisation_utils import plot_post_event_paths

st.set_page_config(page_title='Impact des changements de notation', layout='centered')

df = load_data()

#Graphique d'évolution des rendements

st.title("Evolution des rendements obligataires par pays")
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

#Tableau de statistique des rendements par pays

st.subheader('Tableau des statistiques par pays')
st.markdown('Moyenne, variance et écart-type du rendement pour chaque pays')
stats = (
    df.groupby('Country')['Yield']
      .agg(['mean', 'var', 'std'])
      .round(4)
      .rename(columns={'mean':'Moyenne', 'var':'Variance', 'std':'Écart-type'})
)
st.dataframe(stats)

# Aperçu des données

st.markdown("### Aperçu des données")
st.write("**Dimension (lignes, colonnes) :**", df.shape)
st.dataframe(df.head(-1))

# Tableau statistique détaillé

st.markdown("### Statistiques descriptives")
descr = df.describe(include="all").round(4)
descr = descr.drop(index=["top", "freq"], errors="ignore")
st.dataframe(descr)

# Graphique de densité des rendements

st.subheader("Densité estimée des rendements")
fig, ax = plt.subplots()
sns.kdeplot(df["Yield"], fill=True, ax=ax)
ax.set_xlabel("Yield (%)")
st.pyplot(fig)

#Graphique boxplot des rendements

st.subheader("Boxplots des rendements par pays")
fig, ax = plt.subplots(figsize=(12,6))
sns.boxplot(data=df, x="Country", y="Yield", ax=ax)
ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha="right")
st.pyplot(fig)

# Graphique violin plot des rendements

st.subheader("Violin plots des rendements par pays")
fig, ax = plt.subplots(figsize=(12,6))
sns.violinplot(data=df, x="Country", y="Yield", inner="quartile", ax=ax)
ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha="right")
st.pyplot(fig)

# Graphique évolution des rendements après changement de notation

st.title("Impact des changements de notation – Analyse post-événement")
direction = st.radio("Type d'événement", ("Upgrade", "Downgrade"))
fig, ax = plot_post_event_paths(direction, df, window=100)
st.pyplot(fig)
