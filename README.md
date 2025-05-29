# Evolution du rendement d'obligations souveraines selon les changements de notation

Extraction et jointure de plusieurs datasets : rendements d'obligations souveraines de la France, Allemagne, Italie, Etats-Unis, depuis 2020, puis extraction d'un dataset de l'évolution des notations de ces pays selon trois agences depuis 2020, et jointure via SQALchemy. Présentation des résultats et d'une analyse graphique sur Streamlit.

lien github du projet : https://github.com/yvrj-0/Python_Project-MBFA

## Arborescence du projet

data : stocke les datasets extraits, puis le dataset final crée

src : data fetch : fait le scraping, va chercher les données sur internet, produit un dataset pour les rendements et un pour les notations

src : etl : jointure des deux datasets via SQALchemy et création du dataset final

src : helpers : fonctions généralistes utilisées dans tout le projet.

src :config.py / config.yaml : configuration yaml

src : main : direction de l'ensemble du projet.

tests : tests de volumétrie et de variance pour s'assurer de la conformité du dataset final.

visualisation.py : mise en place de la page streamlit et production de graphiques.


## Utilisation

```bash
python -m venv .venv
source .venv/bin/activate     # Windows : .venv\Scripts\activate
pip install -r requirements.txt # installer les packages nécessaires
python src/main.py #lancer le programme principal
pytest -q #lancer les tests de vérification du dataset final
streamlit run visualisation.py #lancer le tableau streamlit avec les résultats
```

Ressources utilisées :
les cours du père galan
https://app.datacamp.com/learn/courses/joining-data-with-pandas 
https://app.datacamp.com/learn/courses/introduction-to-data-visualization-with-matplotlib 
https://app.datacamp.com/learn/courses/introduction-to-git
https://app.datacamp.com/learn/courses/object-oriented-programming-in-python
https://app.datacamp.com/learn/courses/cleaning-data-in-python
https://app.datacamp.com/learn/courses/introduction-to-relational-databases-in-python 
https://app.datacamp.com/learn/courses/supervised-learning-with-scikit-learn