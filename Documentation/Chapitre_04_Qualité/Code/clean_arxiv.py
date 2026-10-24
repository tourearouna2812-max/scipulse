"""
SciPulse — Nettoyage des données ArXiv
Guidé par les résultats des validations Great Expectations.

Ce script suit le cycle : charger → diagnostiquer → nettoyer →
diagnostiquer à nouveau → (optionnel) réindexer.

Corrections appliquées :
  1. Suppression des doublons (sur arxiv_id)
  2. Nettoyage des abstracts (espaces, retours à la ligne)
  3. Nettoyage des titres (espaces multiples)
  4. Normalisation des noms d'auteurs
  5. Reconstruction des catégories primaires manquantes

Usage :
    python -m src.quality.clean_arxiv              # Diagnostic + nettoyage + réindexation
    python -m src.quality.clean_arxiv --dry-run     # Diagnostic + nettoyage sans réindexation
"""

import argparse
import re
import sys
import time

import pandas as pd
from elasticsearch import Elasticsearch, helpers

ES_HOST    = "http://localhost:9200"
INDEX      = "arxiv-papers"
BATCH_SIZE = 500


# ═════════════════════════════════════════════════════════════════════════════
#  CHARGEMENT
# ═════════════════════════════════════════════════════════════════════════════

def load_all_articles(es: Elasticsearch) -> pd.DataFrame:
    """
    Charge tous les articles depuis Elasticsearch dans un DataFrame.
    Conserve le champ _id pour la réindexation.
    """
    docs = []
    count = 0

    for hit in helpers.scan(es, index=INDEX, query={"query": {"match_all": {}}}, size=1000):
        doc = hit["_source"]
        doc["_id"] = hit["_id"]
        docs.append(doc)
        count += 1
        if count % 25_000 == 0:
            print(f"   {count:>7,} chargés...")

    df = pd.DataFrame(docs)
    print(f"   ✅ {len(df):,} articles chargés depuis '{INDEX}'")
    return df


# ═════════════════════════════════════════════════════════════════════════════
#  DIAGNOSTIC
# ═════════════════════════════════════════════════════════════════════════════

def diagnose(df: pd.DataFrame) -> dict:
    """
    Analyse les problèmes de qualité dans le DataFrame.
    Retourne un dictionnaire de métriques.
    """
    report = {}

    # ── Complétude ─────────────────────────────────────────────────
    report["null_arxiv_id"]   = df["arxiv_id"].isna().sum() if "arxiv_id" in df.columns else 0
    report["null_abstracts"]  = df["abstract"].isna().sum() if "abstract" in df.columns else 0
    report["null_titles"]     = df["title"].isna().sum() if "title" in df.columns else 0
    report["null_categories"] = df["primary_category"].isna().sum() if "primary_category" in df.columns else 0

    # ── Validité ───────────────────────────────────────────────────
    if "abstract" in df.columns:
        abstracts = df["abstract"].dropna()
        report["short_abstracts"] = (abstracts.str.len() < 50).sum()
        report["empty_abstracts"] = (abstracts.str.strip() == "").sum()
    else:
        report["short_abstracts"] = 0
        report["empty_abstracts"] = 0

    if "title" in df.columns:
        titles = df["title"].dropna()
        report["short_titles"] = (titles.str.len() < 10).sum()
        report["multi_space_titles"] = titles.str.contains(r"\s{2,}", regex=True).sum()
    else:
        report["short_titles"] = 0
        report["multi_space_titles"] = 0

    # ── Unicité ────────────────────────────────────────────────────
    report["duplicates"] = df["arxiv_id"].duplicated().sum() if "arxiv_id" in df.columns else 0

    # ── Cohérence catégories ───────────────────────────────────────
    if "primary_category" in df.columns:
        known_prefixes = ("cs.", "stat.", "math.", "eess.", "quant-ph", "physics.", "q-bio.", "nlin.", "hep-", "cond-mat", "astro-ph", "gr-qc")
        non_null_cats = df["primary_category"].dropna()
        unknown_mask = ~non_null_cats.str.startswith(known_prefixes)
        report["unknown_categories"] = unknown_mask.sum()
    else:
        report["unknown_categories"] = 0

    return report


def print_diagnosis(report: dict, label: str):
    """Affiche le diagnostic de manière lisible."""
    print(f"\n  {'─' * 54}")
    print(f"  {label}")
    print(f"  {'─' * 54}")
    print(f"  {'Métrique':<32} {'Valeur':>8}")
    print(f"  {'─' * 32} {'─' * 8}")
    print(f"  arxiv_id nulls                 {report['null_arxiv_id']:>8,}")
    print(f"  Abstracts nulls                {report['null_abstracts']:>8,}")
    print(f"  Abstracts vides                {report['empty_abstracts']:>8,}")
    print(f"  Abstracts < 50 car.            {report['short_abstracts']:>8,}")
    print(f"  Titres nulls                   {report['null_titles']:>8,}")
    print(f"  Titres < 10 car.               {report['short_titles']:>8,}")
    print(f"  Titres avec espaces multiples  {report['multi_space_titles']:>8,}")
    print(f"  Doublons (arxiv_id)            {report['duplicates']:>8,}")
    print(f"  Catégories primaires nulles    {report['null_categories']:>8,}")
    print(f"  Catégories inconnues           {report['unknown_categories']:>8,}")


# ═════════════════════════════════════════════════════════════════════════════
#  NETTOYAGE
# ═════════════════════════════════════════════════════════════════════════════

def clean(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applique les corrections de qualité.
    Retourne un nouveau DataFrame nettoyé (l'original n'est pas modifié).
    """
    original_len = len(df)
    df = df.copy()
    print(f"\n  🧹 Nettoyage de {len(df):,} articles...\n")

    # ── Correction 1 : Supprimer les doublons ──────────────────────
    # On garde la première occurrence. Si le pipeline a ingéré le même
    # article deux fois, on ne garde qu'une copie.
    before = len(df)
    df = df.drop_duplicates(subset="arxiv_id", keep="first")
    deduped = before - len(df)
    print(f"  [1/5] Déduplication        : {deduped:>5,} doublons supprimés")

    # ── Correction 2 : Nettoyer les abstracts ──────────────────────
    # - Remplacer les nulls par des chaînes vides (pour éviter les NaN)
    # - Remplacer les séquences d'espaces par un seul espace
    # - Supprimer les espaces en début/fin
    df["abstract"] = (
        df["abstract"]
        .fillna("")
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )
    # Marquer les abstracts trop courts pour suivi (sans les supprimer)
    short_count = (df["abstract"].str.len() < 50).sum()
    print(f"  [2/5] Nettoyage abstracts  : {short_count:>5,} marqués comme courts")

    # ── Correction 3 : Nettoyer les titres ─────────────────────────
    df["title"] = (
        df["title"]
        .fillna("")
        .str.replace(r"\s{2,}", " ", regex=True)
        .str.strip()
    )
    print(f"  [3/5] Nettoyage titres     : espaces multiples corrigés")

    # ── Correction 4 : Normaliser les auteurs ──────────────────────
    if "authors_flat" in df.columns:
        df["authors_flat"] = (
            df["authors_flat"]
            .fillna("")
            .str.replace(r"\s{2,}", " ", regex=True)
            .str.strip()
        )
    print(f"  [4/5] Normalisation auteurs: espaces normalisés")

    # ── Correction 5 : Reconstruire les catégories primaires ───────
    # Si primary_category est null mais que categories (le tableau)
    # contient des valeurs, on prend la première.
    if "categories" in df.columns:
        mask = df["primary_category"].isna() & df["categories"].apply(
            lambda x: isinstance(x, list) and len(x) > 0
        )
        df.loc[mask, "primary_category"] = df.loc[mask, "categories"].apply(lambda x: x[0])
        fixed = mask.sum()
    else:
        fixed = 0
    print(f"  [5/5] Catégories primaires : {fixed:>5,} reconstruites")

    removed = original_len - len(df)
    print(f"\n  ✅ Nettoyage terminé")
    print(f"     Avant : {original_len:>7,} articles")
    print(f"     Après : {len(df):>7,} articles ({removed:>+,})")

    return df


# ═════════════════════════════════════════════════════════════════════════════
#  RÉINDEXATION
# ═════════════════════════════════════════════════════════════════════════════

def reindex_cleaned(es: Elasticsearch, df: pd.DataFrame):
    """
    Réindexe les données nettoyées dans Elasticsearch.
    Les documents existants sont écrasés grâce à l'_id.
    """
    print(f"\n🔄 Réindexation de {len(df):,} articles nettoyés...")

    def generate_actions():
        for _, row in df.iterrows():
            doc = row.to_dict()
            doc_id = doc.pop("_id", doc.get("arxiv_id"))
            yield {
                "_index": INDEX,
                "_id": doc_id,
                "_source": doc,
            }

    start = time.time()
    success = 0
    errors_list = []

    for ok, result in helpers.streaming_bulk(
        es,
        generate_actions(),
        chunk_size=BATCH_SIZE,
        raise_on_error=False,
    ):
        if ok:
            success += 1
        else:
            errors_list.append(result)

        if success % 25_000 == 0 and success > 0:
            elapsed = time.time() - start
            print(f"   {success:>7,} réindexés ({success / elapsed:,.0f} docs/sec)")

    elapsed = time.time() - start
    es.indices.refresh(index=INDEX)

    print(f"\n  ✅ {success:,} documents réindexés en {elapsed:.1f}s")
    if errors_list:
        print(f"  ❌ {len(errors_list)} erreurs")
        for e in errors_list[:3]:
            print(f"     {e}")


# ═════════════════════════════════════════════════════════════════════════════
#  POINT D'ENTRÉE
# ═════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="SciPulse — Nettoyage des données ArXiv",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples :
  python -m src.quality.clean_arxiv               # Nettoyage complet
  python -m src.quality.clean_arxiv --dry-run      # Diagnostic seul
        """,
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Diagnostiquer et simuler le nettoyage sans réindexer",
    )
    args = parser.parse_args()

    print()
    print("╔══════════════════════════════════════════════════════════╗")
    print("║  SciPulse — Nettoyage des données ArXiv                 ║")
    print("╚══════════════════════════════════════════════════════════╝")

    es = Elasticsearch(ES_HOST)
    try:
        es.info()
    except Exception as e:
        print(f"\n❌ Elasticsearch non accessible : {e}")
        sys.exit(1)

    # 1. Charger
    print("\n📥 Chargement depuis Elasticsearch...")
    df = load_all_articles(es)

    # 2. Diagnostiquer AVANT
    report_before = diagnose(df)
    print_diagnosis(report_before, "DIAGNOSTIC AVANT NETTOYAGE")

    # 3. Nettoyer
    df_clean = clean(df)

    # 4. Diagnostiquer APRÈS
    report_after = diagnose(df_clean)
    print_diagnosis(report_after, "DIAGNOSTIC APRÈS NETTOYAGE")

    # 5. Comparer
    print(f"\n  {'─' * 54}")
    print(f"  BILAN DES AMÉLIORATIONS")
    print(f"  {'─' * 54}")

    any_improvement = False
    for key in report_before:
        before = report_before[key]
        after = report_after[key]
        delta = before - after
        if delta > 0:
            any_improvement = True
            print(f"  📉 {key:<32} {before:>6,} → {after:>6,}  ({delta:>+,})")
        elif before > 0 and delta == 0:
            print(f"  ── {key:<32} {before:>6,} → {after:>6,}  (inchangé)")

    if not any_improvement:
        print(f"  ✨ Les données étaient déjà propres — aucune correction nécessaire.")

    # 6. Réindexer
    if args.dry_run:
        print(f"\n🏃 Mode dry-run — aucune modification dans Elasticsearch.")
        print(f"   Pour appliquer les corrections : python -m src.quality.clean_arxiv")
    else:
        reindex_cleaned(es, df_clean)
        print(f"\n💡 Relancez la validation pour constater l'amélioration :")
        print(f"   python -m src.quality.validate_arxiv")

    print()


if __name__ == "__main__":
    main()
