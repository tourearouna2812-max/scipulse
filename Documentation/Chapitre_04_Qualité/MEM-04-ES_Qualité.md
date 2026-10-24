# TP J2 après-midi — Qualité des données et Great Expectations

---

## 1. `great_expectations` (alias `gx`) — Framework de validation

### `gx.get_context()`

Crée un contexte Great Expectations en mémoire (mode programmatique, sans fichiers YAML). Le contexte est l'objet central qui gère les datasources, les suites et les checkpoints.

```python
import great_expectations as gx
context = gx.get_context()
```

### `context.sources.add_or_update_pandas(name)`

Enregistre une source de données Pandas dans le contexte. Le nom est un identifiant libre.

```python
datasource = context.sources.add_or_update_pandas(name="arxiv_pandas")
```

### `datasource.add_dataframe_asset(name)`

Déclare un « asset » (jeu de données) dans la datasource. Un asset pointe vers un DataFrame concret.

```python
data_asset = datasource.add_dataframe_asset(name="arxiv_data")
```

### `data_asset.build_batch_request(dataframe)`

Crée une requête de lot qui connecte un DataFrame Pandas réel à l'asset déclaré.

```python
batch_request = data_asset.build_batch_request(dataframe=df)
```

### `context.add_or_update_expectation_suite(name)`

Crée ou met à jour une suite d'expectations. Le nom est un identifiant libre qui servira de référence.

```python
suite = context.add_or_update_expectation_suite("arxiv_quality_suite")
```

### `context.get_validator(batch_request, expectation_suite_name)`

Crée un validateur — le pont entre un jeu de données et une suite. C'est sur le validateur qu'on appelle les méthodes `expect_*`.

```python
validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name="arxiv_quality_suite",
)
```

### Expectations du validateur

Chaque expectation vérifie une propriété des données. Le paramètre `mostly` (0.0 à 1.0) définit le seuil de tolérance.

| Méthode | Ce qu'elle vérifie |
|---------|-------------------|
| `validator.expect_column_values_to_not_be_null("col", mostly=0.99)` | Au moins 99% des valeurs ne sont pas null |
| `validator.expect_column_values_to_be_unique("col")` | Aucun doublon dans la colonne |
| `validator.expect_column_values_to_be_in_set("col", value_set=[...], mostly=0.9)` | 90%+ des valeurs dans l'ensemble donné |
| `validator.expect_column_values_to_be_between("col", min_value=0, max_value=100)` | Valeurs dans l'intervalle [0, 100] |
| `validator.expect_column_value_lengths_to_be_between("col", min_value=50)` | Longueur de chaque valeur ≥ 50 caractères |
| `validator.expect_table_row_count_to_be_between(min_value=5000, max_value=150000)` | Nombre de lignes dans l'intervalle |
| `validator.expect_table_columns_to_include_column_set(column_set=[...])` | Toutes les colonnes listées sont présentes |

### `validator.save_expectation_suite(discard_failed_expectations)`

Sauvegarde la suite dans le contexte. Avec `discard_failed_expectations=False`, on garde toutes les expectations même si certaines ont échoué à la dernière exécution.

```python
validator.save_expectation_suite(discard_failed_expectations=False)
```

### `context.add_or_update_checkpoint(name, validations)`

Crée un checkpoint — l'objet qui exécute une suite sur un jeu de données.

```python
checkpoint = context.add_or_update_checkpoint(
    name="arxiv_checkpoint",
    validations=[{
        "batch_request": batch_request,
        "expectation_suite_name": "arxiv_quality_suite",
    }],
)
```

### `checkpoint.run()`

Exécute le checkpoint. Retourne un objet `CheckpointResult` contenant le succès/échec de chaque expectation.

```python
result = checkpoint.run()
print(result.success)  # True si toutes les expectations passent
```

### `context.build_data_docs()`

Génère le rapport HTML (Data Docs) avec les résultats de toutes les validations.

```python
context.build_data_docs()
# → Rapport dans gx/uncommitted/data_docs/local_site/index.html
```

### Naviguer dans les résultats

```python
run_results = list(result.run_results.values())
validation_result = run_results[0]["validation_result"]

for r in validation_result["results"]:
    success = r["success"]                                    # True/False
    exp_type = r["expectation_config"]["expectation_type"]    # Nom de l'expectation
    column = r["expectation_config"]["kwargs"].get("column")  # Colonne testée
    unexpected = r["result"].get("unexpected_count", 0)       # Valeurs non conformes
    pct = r["result"].get("unexpected_percent", 0)            # Pourcentage non conforme
```

---

## 2. `pandas` — Manipulation de DataFrames

### `pd.DataFrame(list_of_dicts)`

Crée un DataFrame à partir d'une liste de dictionnaires. Chaque dict devient une ligne, chaque clé une colonne.

```python
import pandas as pd
df = pd.DataFrame([{"arxiv_id": "2301.07041", "title": "Mon article"}, ...])
```

### `df.info()`

Affiche un résumé du DataFrame : nombre de lignes, colonnes, types, compteur de non-nulls. Premier outil de diagnostic de qualité.

```python
df.info()
# → RangeIndex: 10000 entries, 0 to 9999
# → abstract    9987 non-null  object     ← 13 nulls !
```

### `df.memory_usage(deep=True).sum()`

Retourne la consommation mémoire totale en octets. `deep=True` mesure la taille réelle des objets (chaînes).

```python
size_mb = df.memory_usage(deep=True).sum() / 1024 / 1024
```

### `df.isnull().sum()` / `df.isna().sum()`

Compte les valeurs nulles par colonne. Les deux méthodes sont synonymes.

```python
nulls_per_column = df.isnull().sum()
```

### `df.duplicated(subset)`

Retourne un booléen par ligne indiquant si c'est un doublon (True) ou non. `subset` limite la vérification à certaines colonnes.

```python
n_duplicates = df["arxiv_id"].duplicated().sum()
```

### `df.drop_duplicates(subset, keep)`

Supprime les doublons. `keep="first"` garde la première occurrence.

```python
df = df.drop_duplicates(subset="arxiv_id", keep="first")
```

### `df.dropna(subset)`

Supprime les lignes qui ont des nulls dans les colonnes spécifiées.

```python
df_clean = df.dropna(subset=["abstract"])
```

### `df.fillna(value)`

Remplace les nulls par une valeur donnée.

```python
df["abstract"] = df["abstract"].fillna("")
```

### `df["col"].str.len()` / `.str.contains()` / `.str.replace()`

Opérations vectorisées sur les chaînes. Appliquent une opération à chaque élément de la colonne.

```python
# Longueur de chaque abstract
lengths = df["abstract"].str.len()

# Abstracts qui contiennent des espaces multiples
has_multi = df["title"].str.contains(r"\s{2,}", regex=True)

# Remplacer les espaces multiples
df["title"] = df["title"].str.replace(r"\s{2,}", " ", regex=True)
```

### `df["col"].value_counts()`

Compte les occurrences de chaque valeur dans la colonne. Trié par fréquence décroissante.

```python
df["primary_category"].value_counts().head(10)
# → cs.LG    15234
# → cs.AI    12876
# → ...
```

### `df["col"].nunique()`

Nombre de valeurs distinctes dans la colonne.

```python
n_categories = df["primary_category"].nunique()   # → 42
```

### `df["col"].quantile(q)`

Calcule le quantile q (entre 0 et 1).

```python
p5  = lengths.quantile(0.05)    # 5ème percentile
p95 = lengths.quantile(0.95)    # 95ème percentile
```

### `df["col"].apply(func)`

Applique une fonction à chaque élément de la colonne. Utilisé pour les transformations complexes.

```python
df.loc[mask, "primary_category"] = df.loc[mask, "categories"].apply(lambda x: x[0])
```

### `df.to_dict()` / `row.to_dict()`

Convertit un DataFrame ou une ligne en dictionnaire.

```python
for _, row in df.iterrows():
    doc = row.to_dict()
```

---

## 3. `elasticsearch.helpers.scan` — Parcours complet d'un index

### `helpers.scan(es, index, query, size)`

Parcourt un index entier de manière efficace en utilisant l'API scroll d'ES. Retourne un itérateur de hits. Contrairement à `es.search()` qui est limité à 10 000 résultats, `scan` peut parcourir des millions de documents.

```python
from elasticsearch import helpers

for hit in helpers.scan(es, index="arxiv-papers", query={"query": {"match_all": {}}}, size=1000):
    doc = hit["_source"]
    # Traiter le document...
```

### `re` — Expressions régulières (bibliothèque standard)

Utilisé dans `clean_arxiv.py` pour le nettoyage de texte.

```python
import re
# Vérifier qu'un nom ne commence pas par un chiffre
df = df[~df["author_name"].str.match(r"^\d")]
```

---
---

# TP J3 matin — Transformation dbt et enrichissement Python

---

## 1. `psycopg2` — Client PostgreSQL

### `psycopg2.connect(dsn)`

Crée une connexion à PostgreSQL.

```python
import psycopg2
conn = psycopg2.connect("host=localhost port=5432 dbname=scipulse user=scipulse password=scipulse")
```

### `conn.cursor()`

Crée un curseur pour exécuter des requêtes SQL.

```python
cur = conn.cursor()
```

### `cur.execute(sql)`

Exécute une requête SQL (DDL ou DML).

```python
cur.execute("DROP TABLE IF EXISTS public.arxiv_papers_raw CASCADE;")
cur.execute("CREATE TABLE public.arxiv_papers_raw (arxiv_id TEXT PRIMARY KEY, title TEXT, ...);")
conn.commit()    # N'oubliez pas le commit !
```

### `cur.fetchone()` / `cur.fetchall()`

Récupère les résultats d'un SELECT.

```python
cur.execute("SELECT COUNT(*) FROM arxiv_papers_raw;")
count = cur.fetchone()[0]   # → 100000

cur.execute("SELECT primary_category, COUNT(*) FROM arxiv_papers_raw GROUP BY 1 ORDER BY 2 DESC LIMIT 5;")
for row in cur.fetchall():
    print(f"{row[0]} : {row[1]}")
```

### `psycopg2.extras.execute_values(cur, sql, values, page_size)`

Insère des lignes en masse (beaucoup plus rapide qu'un INSERT par ligne). Le `%s` dans le SQL est remplacé par chaque tuple de `values`.

```python
from psycopg2.extras import execute_values

values = [("2301.07041", "Mon article", "Mon abstract", ...), ...]

execute_values(
    cur,
    "INSERT INTO arxiv_papers_raw (arxiv_id, title, abstract, ...) VALUES %s ON CONFLICT (arxiv_id) DO NOTHING",
    values,
    page_size=1000,
)
conn.commit()
```

### `conn.close()` / `cur.close()`

Ferme la connexion et le curseur. Toujours fermer dans un `finally` ou avec `with`.

---

## 2. `sklearn.feature_extraction.text.TfidfVectorizer` — Extraction de mots-clés

### `TfidfVectorizer(**params)`

Crée un vectoriseur TF-IDF qui transforme des textes en matrice numérique.

```python
from sklearn.feature_extraction.text import TfidfVectorizer

vectorizer = TfidfVectorizer(
    max_features=5000,       # Garder les 5000 termes les plus importants
    stop_words="english",    # Supprimer les stopwords anglais
    min_df=5,                # Ignorer les termes dans < 5 documents
    max_df=0.7,              # Ignorer les termes dans > 70% des documents
    ngram_range=(1, 2),      # Unigrammes et bigrammes ("neural", "neural network")
    token_pattern=r"(?u)\b[a-zA-Z][a-zA-Z]+\b",  # Mots de 2+ lettres
)
```

### `vectorizer.fit_transform(texts)`

Apprend le vocabulaire ET transforme les textes en matrice TF-IDF. Retourne une matrice sparse (scipy).

```python
tfidf_matrix = vectorizer.fit_transform(abstracts)   # shape: (n_docs, n_features)
```

### `vectorizer.get_feature_names_out()`

Retourne la liste des termes du vocabulaire (correspondant aux colonnes de la matrice).

```python
feature_names = vectorizer.get_feature_names_out()   # ["algorithm", "attention", "cnn", ...]
```

### Extraire les top-N mots-clés par document

```python
import numpy as np

for i in range(len(abstracts)):
    row = tfidf_matrix[i].toarray().flatten()     # Score TF-IDF de chaque terme pour ce doc
    top_indices = row.argsort()[-8:][::-1]         # 8 meilleurs scores, décroissant
    keywords = [feature_names[j] for j in top_indices if row[j] > 0]
```

---

## 3. `numpy` — Opérations numériques

### `ndarray.argsort()`

Retourne les indices qui trieraient le tableau. Utilisé pour trouver les N plus grandes valeurs.

```python
import numpy as np
row = np.array([0.1, 0.8, 0.3, 0.9])
top_2 = row.argsort()[-2:][::-1]   # → [3, 1] (indices des valeurs 0.9 et 0.8)
```

### `ndarray.flatten()`

Aplatit une matrice en vecteur 1D. Nécessaire après `.toarray()` sur une matrice sparse.

```python
row = tfidf_matrix[i].toarray().flatten()
```

---

## 4. dbt — Fonctions Jinja et SQL PostgreSQL

### `{{ ref('model_name') }}`

Référence un autre modèle dbt. dbt résout automatiquement les dépendances et exécute les modèles dans le bon ordre.

```sql
select * from {{ ref('stg_arxiv_papers') }}
```

### `{{ source('source_name', 'table_name') }}`

Référence une table brute (non gérée par dbt). Déclarée dans `schema.yml`.

```sql
select * from {{ source('raw', 'arxiv_papers_raw') }}
```

### Fonctions SQL PostgreSQL utilisées

| Fonction | Rôle | Exemple |
|----------|------|---------|
| `trim(col)` | Supprimer les espaces en début/fin | `trim(title) as title` |
| `length(col)` | Longueur d'une chaîne | `length(abstract) as abstract_length` |
| `split_part(col, sep, n)` | Extraire la n-ème partie d'une chaîne | `split_part(primary_category, '.', 1) as domain` |
| `coalesce(a, b)` | Retourne la première valeur non-null | `coalesce(primary_category, split_part(categories, ' ', 1))` |
| `string_to_array(col, sep)` | Chaîne → tableau | `string_to_array(categories, ' ')` |
| `array_length(arr, dim)` | Nombre d'éléments d'un tableau | `array_length(string_to_array(categories, ' '), 1)` |
| `unnest(array)` | Éclate un tableau en lignes | `unnest(string_to_array(authors_flat, ','))` |
| `date_trunc('month', date)` | Tronquer au mois | `date_trunc('month', date_updated::date)::date as mois` |
| `count(distinct col)` | Comptage de valeurs uniques | `count(distinct arxiv_id) as nb_publications` |
| `mode() within group (order by col)` | Valeur la plus fréquente (PostgreSQL) | `mode() within group (order by domain) as domaine_principal` |
| `col::date` / `col::int` | Conversion de type (cast) | `date_updated::date` |

### Commandes dbt CLI

| Commande | Rôle |
|----------|------|
| `dbt debug` | Vérifier la connexion PostgreSQL |
| `dbt run` | Exécuter tous les modèles (staging → marts) |
| `dbt test` | Lancer les tests déclarés dans `schema.yml` |
| `dbt docs generate` | Générer la documentation HTML |
| `dbt docs serve --port 8081` | Serveur web pour la documentation |
| `dbt run --profiles-dir .` | Chercher `profiles.yml` dans le dossier courant |

---

## 5. API Elasticsearch — Mise à jour partielle

### `_op_type: "update"` dans les actions bulk

Met à jour uniquement les champs spécifiés dans `doc`, sans écraser le reste du document. Différent de `_op_type: "index"` qui remplace le document entier.

```python
yield {
    "_op_type": "update",
    "_index": "arxiv-papers",
    "_id": arxiv_id,
    "doc": {"keywords": ["transformer", "attention"]},   # Seul ce champ est modifié
    "doc_as_upsert": False,   # Ne pas créer le document s'il n'existe pas
}
```
