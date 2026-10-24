"""
SciPulse Insights — Diagnostic d'une jointure lente (sort-merge vs broadcast)

Compare, sur les tables Silver, la jointure ArXiv ⨝ OpenAlex par DOI :
  1. broadcast automatique désactivé → SortMergeJoin (shuffle des DEUX côtés)
  2. broadcast hint explicite        → BroadcastHashJoin (la grande table ne bouge pas)

Pour chaque stratégie : plan physique, présence d'un Exchange, temps mesuré.
Les résultats sont à reporter dans docs/MODULE2_PIPELINE_SPARK.md (tableau J2).
"""

import time

from pyspark.sql import functions as F

from src.spark_jobs import config
from src.spark_jobs.spark_session import get_spark_session


def plan_text(df) -> str:
    return df._jdf.queryExecution().executedPlan().toString()


def run(df, label: str):
    plan = plan_text(df)
    operateur = ("BroadcastHashJoin" if "BroadcastHashJoin" in plan
                 else "SortMergeJoin" if "SortMergeJoin" in plan else "autre")
    nb_exchange = plan.count("Exchange")
    print(f"\n=== {label} ===\n{plan}")
    start = time.time()
    n = df.count()
    elapsed = time.time() - start
    return operateur, nb_exchange, n, elapsed


def main():
    spark = get_spark_session("Diagnose slow join")

    arxiv = spark.read.parquet(config.ARXIV_CLEAN).filter(F.col("doi_clean").isNotNull()).select("arxiv_id", "doi_clean")
    cit = spark.read.parquet(config.CITATIONS_CLEAN).filter(F.col("doi_clean").isNotNull()).select("openalex_id", "doi_clean")

    # 1. Broadcast interdit → sort-merge join forcé
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    slow = run(arxiv.join(cit, on="doi_clean", how="inner"), "SANS broadcast (sort-merge forcé)")

    # 2. Broadcast explicite de la petite table
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")
    fast = run(arxiv.join(F.broadcast(cit), on="doi_clean", how="inner"), "AVEC broadcast hint")

    print("\n" + "═" * 72)
    print(f"  {'':<22}{'Sort-merge join':>24}{'Broadcast join':>24}")
    print(f"  {'Opérateur':<22}{slow[0]:>24}{fast[0]:>24}")
    print(f"  {'Nb d’Exchange':<22}{slow[1]:>24}{fast[1]:>24}")
    print(f"  {'Lignes jointes':<22}{slow[2]:>24,}{fast[2]:>24,}")
    print(f"  {'Temps':<22}{slow[3]:>23.2f}s{fast[3]:>23.2f}s")
    if fast[3] > 0:
        print(f"  Gain du broadcast : x{slow[3] / fast[3]:.1f}")
    print("═" * 72)

    spark.stop()


if __name__ == "__main__":
    main()
