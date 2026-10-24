import sys
import os
import webbrowser
import great_expectations as gx
import pandas as pd
from elasticsearch import Elasticsearch, helpers

# 1. Connexion à Elasticsearch
es = Elasticsearch("http://localhost:9200")

# 2. Extraction des données depuis l'index hn-items
print("📥 Extraction d'un échantillon depuis 'hn-items'...")
query = {"query": {"match_all": {}}}
try:
    res = helpers.scan(es, index="hn-items", query=query, size=1000, preserve_order=False)
    data = [hit["_source"] for hit in res]
    df = pd.DataFrame(data)
except Exception as e:
    print(f"❌ Erreur lors de l'extraction : {e}")
    sys.exit(1)

if df.empty:
    print("⚠️ L'index 'hn-items' est vide. Lancez d'abord le poller HN (src.ingestion.hn_poller).")
    sys.exit(1)

print(f"✅ Extrait {len(df)} items Hacker News.")

# 3. Initialisation du contexte Great Expectations (Version 1.x)
context = gx.get_context(mode="ephemeral")
data_source = context.data_sources.add_pandas("hn_source")
data_asset = data_source.add_dataframe_asset("hn_data")
batch_definition = data_asset.add_batch_definition_whole_dataframe("hn_batch")
batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

# 4. Création de la Suite de Validation
suite = context.suites.add(gx.ExpectationSuite(name="hn_quality_suite"))

# 5. Définition des 5+ Expectations (Règles imposées par le TP)
# E1 : L'id de l'item ne doit pas être nul
suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="id"))

# E2 : L'id doit être unique
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column="id"))

# E3 : Le type de l'item doit être dans la liste officielle de HN
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
    column="type", 
    value_set=["story", "comment", "poll", "pollopt", "job"]
))

# E4 : Le nom du contributeur ('by') ne doit pas être nul
suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="by"))

# E5 : Si la colonne 'score' existe, elle doit être supérieure ou égale à 0
if "score" in df.columns:
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="score", min_value=0))

# 6. Exécution de la Validation via une ValidationDefinition
validation_definition = context.validation_definitions.add(
    gx.ValidationDefinition(
        name="hn_validation",
        data=batch_definition,
        suite=suite
    )
)

# On lance la validation en passant notre dataframe actuel
results = validation_definition.run(batch_parameters={"dataframe": df})

# 7. Affichage des résultats dans la console
print("\n" + "═"*60)
print("   RÉSULTATS DE LA VALIDATION HACKER NEWS")
print("═"*60)
all_success = True

# En 1.x, on parcourt les résultats via results.results
for res in results.results:
    statut = "✅" if res.success else "❌"
    exp_type = res.expectation_config.type.split(".")[-1]
    kwargs = res.expectation_config.kwargs
    col = kwargs.get("column", "—table—")
    print(f"   {statut} {col:<20} {exp_type}")
    if not res.success:
        all_success = False

print("─"*60)
# Calcul du score global
success_count = sum(1 for res in results.results if res.success)
total_count = len(results.results)
score = (success_count / total_count) * 100
print(f"📊 Score : {score:.0f}% ({success_count}/{total_count})")

# 8. Finalisation propre sans crash
if all_success:
    print("🎉 Toutes les validations HN ont réussi (100%) !")
else:
    print("⚠️ Certaines validations HN ont échoué.")