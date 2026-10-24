"""
SciPulse Insights — Validation Great Expectations (v1.x) des citations Silver

Lit la table s3://citations-clean/openalex.parquet depuis MinIO, applique
une suite de 7 expectations (le brief en exige au moins 3) et génère les
Data Docs, comme validate_arxiv.py / validate_hn.py au module 1.

API Great Expectations 1.x (celle qui a provoqué les erreurs
"gx.Validator" et "ValidationDefinition: data field required") :
  data source pandas → asset dataframe → batch definition
  → suite → ValidationDefinition(data=…, suite=…, name=…) → run()

Usage (depuis Windows, dans ton venv) :
    python -m src.quality.validate_citations
Code de sortie 1 si une expectation échoue (exploitable par Airflow).
"""

import io
import json
import logging
import os
import sys
from datetime import date
from pathlib import Path

import boto3
import great_expectations as gx
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

BUCKET = "citations-clean"
PREFIX = "openalex.parquet/"
REPORT_PATH = Path("reports/gx_citations_results.json")


def load_silver_citations() -> pd.DataFrame:
    """Télécharge les fichiers Parquet écrits par Spark (un dossier de part-*.parquet)."""
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
    )
    paginator = s3.get_paginator("list_objects_v2")
    frames = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=PREFIX):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                body = s3.get_object(Bucket=BUCKET, Key=obj["Key"])["Body"].read()
                frames.append(pd.read_parquet(io.BytesIO(body)))
    if not frames:
        raise FileNotFoundError(f"Aucun Parquet dans s3://{BUCKET}/{PREFIX} — lance build_silver --source citations")

    df = pd.concat(frames, ignore_index=True)
    # Les colonnes imbriquées (counts_by_year) ne sont pas utiles aux expectations
    return df.drop(columns=[c for c in ["counts_by_year"] if c in df.columns])


def build_suite() -> gx.ExpectationSuite:
    annee_max = date.today().year + 1
    suite = gx.ExpectationSuite(name="citations_quality_suite")
    expectations = [
        # Identité de l'œuvre
        gx.expectations.ExpectColumnValuesToNotBeNull(column="openalex_id"),
        gx.expectations.ExpectColumnValuesToBeUnique(column="openalex_id"),
        gx.expectations.ExpectColumnValuesToMatchRegex(column="openalex_id", regex=r"^W\d+$"),
        # Métrique de citation exploitable
        gx.expectations.ExpectColumnValuesToNotBeNull(column="cited_by_count"),
        gx.expectations.ExpectColumnValuesToBeBetween(column="cited_by_count", min_value=0, max_value=1_000_000),
        # Clés de jointure correctement normalisées (DOI sans préfixe URL, en minuscules)
        gx.expectations.ExpectColumnValuesToMatchRegex(column="doi_clean", regex=r"^10\.\d{4,9}/\S+$", mostly=0.99),
        # Cohérence temporelle
        gx.expectations.ExpectColumnValuesToBeBetween(column="oa_publication_year", min_value=1990,
                                                      max_value=annee_max, mostly=0.99),
    ]
    for exp in expectations:
        suite.add_expectation(exp)
    return suite


def main():
    logger.info("📥 Chargement des citations Silver depuis MinIO...")
    df = load_silver_citations()
    logger.info(f"   {len(df):,} œuvres chargées")

    context = gx.get_context(mode="ephemeral")
    data_source = context.data_sources.add_pandas(name="citations_pandas")
    asset = data_source.add_dataframe_asset(name="citations_data")
    batch_definition = asset.add_batch_definition_whole_dataframe("citations_batch")

    suite = context.suites.add(build_suite())
    validation = context.validation_definitions.add(
        gx.ValidationDefinition(name="citations_validation", data=batch_definition, suite=suite)
    )
    results = validation.run(batch_parameters={"dataframe": df})

    # ── Affichage console (même présentation qu'au module 1) ──
    print("\n" + "═" * 70)
    print("  RÉSULTATS DE LA VALIDATION CITATIONS (OpenAlex Silver)")
    print("═" * 70)
    rapport = []
    for r in results.results:
        cfg = r.expectation_config
        colonne = cfg.kwargs.get("column", "-table-")
        pct = r.result.get("unexpected_percent")
        detail = f"({pct:.2f} % non conformes)" if pct else ""
        print(f"  {'✅' if r.success else '❌'} {colonne:<22} {cfg.type:<45} {detail}")
        rapport.append({"column": colonne, "expectation": cfg.type, "success": r.success,
                        "unexpected_percent": pct})

    succes = sum(1 for x in rapport if x["success"])
    score = 100 * succes / len(rapport)
    print("─" * 70)
    print(f"  Score : {score:.0f} % ({succes}/{len(rapport)})")
    print("═" * 70)

    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(json.dumps({"rows": len(df), "score": score, "results": rapport},
                                      indent=2, ensure_ascii=False), encoding="utf-8")
    logger.info(f"📝 Rapport JSON : {REPORT_PATH}")

    context.build_data_docs()
    if os.environ.get("GX_OPEN_DOCS", "1") == "1":   # mettre 0 dans Airflow (pas de navigateur)
        context.open_data_docs()
    logger.info("📊 Data Docs générés")

    sys.exit(0 if results.success else 1)


if __name__ == "__main__":
    main()
