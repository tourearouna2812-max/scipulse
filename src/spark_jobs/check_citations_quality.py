"""
SciPulse Insights — Contrôle qualité des citations Silver, version Spark (pour Airflow)

Mêmes 7 règles que la suite Great Expectations (src/quality/validate_citations.py),
recalculées en Spark. Pourquoi un doublon :
  - Great Expectations 1.x exige Python >= 3.9 ; l'image Spark 3.5.0 embarque Python 3.8,
    et l'image Airflow n'a ni GX ni accès à MinIO configuré pour pandas.
  - Dans le DAG, ce script sert de "quality gate" : code de sortie 1 si une règle échoue,
    ce qui stoppe le pipeline AVANT l'écriture dans la couche Gold.
  - La suite GX reste la validation de référence (Data Docs, rapport JSON), lancée à la main.

Usage :
    .\\scripts\\spark.ps1 check_citations_quality
"""

from __future__ import annotations

import logging
import sys
from datetime import date

from pyspark.sql import functions as F

from src.spark_jobs import config
from src.spark_jobs.spark_session import get_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def run_checks(df) -> list[dict]:
    total = df.count()
    annee_max = date.today().year + 1

    def taux_invalide(condition_invalide) -> float:
        return df.filter(condition_invalide).count() / max(total, 1)

    regles = [
        ("openalex_id non nul", taux_invalide(F.col("openalex_id").isNull()), 0.0),
        ("openalex_id unique", 1 - df.select("openalex_id").distinct().count() / max(total, 1), 0.0),
        ("openalex_id au format W<n>", taux_invalide(~F.col("openalex_id").rlike(r"^W\d+$")), 0.0),
        ("cited_by_count non nul", taux_invalide(F.col("cited_by_count").isNull()), 0.0),
        ("cited_by_count dans [0, 1 000 000]",
         taux_invalide((F.col("cited_by_count") < 0) | (F.col("cited_by_count") > 1_000_000)), 0.0),
        ("doi_clean normalisé (10.xxxx/...)",
         taux_invalide(F.col("doi_clean").isNotNull() & ~F.col("doi_clean").rlike(r"^10\.\d{4,9}/\S+$")), 0.01),
        (f"oa_publication_year dans [1990, {annee_max}]",
         taux_invalide((F.col("oa_publication_year") < 1990) | (F.col("oa_publication_year") > annee_max)), 0.01),
    ]
    return [{"regle": nom, "taux_invalide": taux, "tolerance": tol, "succes": taux <= tol}
            for nom, taux, tol in regles]


def main():
    spark = get_spark_session("SciPulse — Quality gate citations")
    df = spark.read.parquet(config.CITATIONS_CLEAN).cache()
    logger.info(f"📂 {df.count():,} œuvres OpenAlex en Silver")

    resultats = run_checks(df)
    print("\n" + "═" * 70)
    print("  QUALITY GATE — citations Silver (7 règles, miroir de la suite GX)")
    print("═" * 70)
    for r in resultats:
        print(f"  {'✅' if r['succes'] else '❌'} {r['regle']:<40} {100 * r['taux_invalide']:6.2f} % invalides")
    nb_ok = sum(r["succes"] for r in resultats)
    print("─" * 70)
    print(f"  Score : {100 * nb_ok / len(resultats):.0f} % ({nb_ok}/{len(resultats)})")
    print("═" * 70)
    spark.stop()

    if nb_ok < len(resultats):
        logger.error("❌ Quality gate en échec — la couche Gold ne sera pas mise à jour")
        sys.exit(1)


if __name__ == "__main__":
    main()
