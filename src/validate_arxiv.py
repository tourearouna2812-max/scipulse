"""
SciPulse — Suite de validation Great Expectations pour les données ArXiv
"""

import sys
import great_expectations as gx
import pandas as pd
from elasticsearch import Elasticsearch
from src.quality.extract_for_validation import extract_arxiv

ES_HOST = "http://localhost:9200"

def validate_arxiv_data(df: pd.DataFrame):
    """
    Valide un DataFrame d'articles ArXiv avec Great Expectations.
    """

    # ── 1. Créer un contexte GX en mémoire ──────
    context = gx.get_context()

    # ── 2. Connecter le DataFrame (Syntaxe sécurisée) ─────────────
    try:
        datasource = context.data_sources.add_or_update_pandas(name="arxiv_pandas")
    except AttributeError:
        datasource = context.sources.add_or_update_pandas(name="arxiv_pandas")
        
    data_asset = datasource.add_dataframe_asset(name="arxiv_data")
    batch_request = data_asset.build_batch_request(options={"dataframe": df})
    
    # ── 3. Créer la suite d'expectations ──────────────────────────────
    suite_name = "arxiv_quality_suite"
    context.suites.add_or_update(gx.ExpectationSuite(name=suite_name))
    #context.add_or_update_expectation_suite(expectation_suite_name=suite_name)

    # ── 4. Obtenir un validateur ──────────────────────────────────────
    validator = context.get_validator(
        batch_request=batch_request, 
        expectation_suite_name=suite_name
    )

    # ══════════════════════════════════════════════════════════════
    #  EXPECTATIONS
    # ══════════════════════════════════════════════════════════════

    # Complétude
    validator.expect_column_values_to_not_be_null("arxiv_id")
    validator.expect_column_values_to_not_be_null("title")
    validator.expect_column_values_to_not_be_null("abstract", mostly=0.99)
    validator.expect_column_values_to_not_be_null("primary_category")

    # Unicité
    validator.expect_column_values_to_be_unique("arxiv_id")

    # Validité
    validator.expect_column_value_lengths_to_be_between("abstract", min_value=50)
    validator.expect_column_value_lengths_to_be_between("title", min_value=10)
    
    known_cs_categories = ["cs.AI", "cs.LG", "cs.CV", "cs.CL", "cs.NE", "stat.ML"]
    validator.expect_column_values_to_be_in_set("primary_category", known_cs_categories, mostly=0.80)

    # Volume
    validator.expect_table_row_count_to_be_between(min_value=5000, max_value=15000)
    expected_columns = ["arxiv_id", "title", "abstract", "primary_category", "date_published"]
    validator.expect_table_columns_to_match_set(column_set=expected_columns, exact_match=False)

    # ══════════════════════════════════════════════════════════════
    #  SAUVEGARDER ET EXÉCUTER
    # ══════════════════════════════════════════════════════════════
    #validator.save_expectation_suite(discard_failed_expectations=False)
    #context.suites.add_or_update(validator.get_expectation_suite())
    #checkpoint = context.add_or_update_checkpoint(
        #name="arxiv_checkpoint",
        #validations=[
            #{
                #"batch_request": batch_request,
                #"expectation_suite_name": suite_name,
            #}
        #],
    #)

    #result = checkpoint.run()
    #return context, result
   # ── SAUVEGARDER LA SUITE ET EXÉCUTER ──────────────────────────
    # ── SAUVEGARDER LA SUITE ET EXÉCUTER ──────────────────────────
    context.suites.add_or_update(validator.get_expectation_suite())

    # 1. Créer une définition de lot (Batch Definition) - REQUIS en 1.x
    batch_def = data_asset.add_batch_definition_whole_dataframe("all_data_batch")

    # 2. Créer la définition de validation
    validation_def = gx.ValidationDefinition(
        name="arxiv_validation_def",
        data=batch_def, # On donne le batch_def ici
        suite=context.suites.get(name=suite_name)
    )

    # 3. Créer et exécuter le checkpoint
    checkpoint = context.checkpoints.add_or_update(
        gx.Checkpoint(
            name="arxiv_checkpoint",
            validation_definitions=[validation_def]
        )
    )

    result = checkpoint.run(batch_parameters={"dataframe": df})
    return context, result



def print_results(result):
    """Affiche les résultats de manière lisible dans le terminal."""
    print("\n" + "═" * 60)
    print("   RÉSULTATS DE LA VALIDATION ARXIV")
    print("═" * 60)

    # En 1.x, on accède directement aux résultats via .results du premier run
    run_results = list(result.run_results.values())
    if not run_results:
        print("   ⚠️ Aucun résultat")
        return

    # Accès direct aux résultats de validation
    validation_result = run_results[0]
    results_list = validation_result.results

    success_count = 0
    failure_count = 0

    for r in results_list:
        success = r.success
        expectation_type = r.expectation_config.type
        #expectation_type = r.expectation_config.expectation_type
        kwargs = r.expectation_config.kwargs
        column = kwargs.get("column", "—table—")

        if success:
            success_count += 1
            icon = "✅"
        else:
            failure_count += 1
            icon = "❌"

        short_name = expectation_type.replace("expect_column_values_to_", "").replace("expect_column_", "").replace("expect_table_", "table:")
        
        # Statistiques
        res_detail = r.result
        u_count = res_detail.get("unexpected_count", 0)
        u_pct = res_detail.get("unexpected_percent")

        stats = ""
        if u_count > 0:
            stats = f" ({u_count} non-conformes"
            if u_pct is not None:
                stats += f", {u_pct:.2f}%"
            stats += ")"

        print(f"   {icon} {column:<22} {short_name:<35} {stats}")

    print(f"\n   {'─' * 56}")
    print(f"   ✅ Succès  : {success_count}")
    print(f"   ❌ Échecs  : {failure_count}")
    total = success_count + failure_count
    pct = success_count / total * 100 if total > 0 else 0
    print(f"   📊 Score   : {pct:.0f}% ({success_count}/{total})")
    print()
if __name__ == "__main__":
    print()
    print("╔══════════════════════════════════════════════════════════╗")
    print("║   SciPulse — Validation qualité ArXiv                    ║")
    print("╚══════════════════════════════════════════════════════════╝")

    es = Elasticsearch(ES_HOST)
    
    print("\n📥 Extraction d'un échantillon depuis Elasticsearch...\n")
    df_arxiv = extract_arxiv(es, max_docs=10000)
    
    print("\n🔍 Exécution de la suite de validation...\n")
    context, result = validate_arxiv_data(df_arxiv)
    
    print_results(result)

    print("📊 Génération des Data Docs...")
    context.build_data_docs()
    context.open_data_docs()
    
    if not result.success:
        print("⚠️ Certaines validations ont échoué.")
        sys.exit(1)
    else:
        print("🎉 Toutes les validations ont réussi !")