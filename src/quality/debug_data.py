import pandas as pd
from elasticsearch import Elasticsearch
from src.quality.extract_for_validation import extract_arxiv

es = Elasticsearch("http://localhost:9200")
# On récupère un échantillon pour analyser
df = extract_arxiv(es, max_docs=10000)

print("\n--- ANALYSE DES ANOMALIES ---")

# 1. Analyse des abstracts courts (< 50 caractères)
short_abstracts = df[df["abstract"].str.len() < 50]
print(f"\n🔍 {len(short_abstracts)} abstracts suspects (< 50 chars) :")
if not short_abstracts.empty:
    print(short_abstracts[["arxiv_id", "abstract"]].head(10))
else:
    print("Aucun abstract suspect trouvé.")

# 2. Analyse des catégories "exotiques" 
# (Celles qui ne sont pas CS, MATH, STAT ou PHYSICS)
pattern = r"^(cs\.|stat\.|math\.|physics\.)"
exotic_cats = df[~df["primary_category"].str.contains(pattern, regex=True, na=False)]
print(f"\n🔍 {len(exotic_cats)} catégories hors-standards :")
if not exotic_cats.empty:
    print(exotic_cats["primary_category"].value_counts().head(10))