"""
SciPulse Insights — Benchmark Pandas vs Spark (J1)

Même opération métier (comptage des articles par catégorie principale) :
  - Pandas sur un sous-ensemble local (fichier JSON Lines)
  - Spark sur ce même sous-ensemble
  - Spark sur le dump complet dans MinIO
Objectif : observer comment le temps évolue avec le volume, pas "prouver"
que Spark est plus rapide (sur petit volume, Pandas gagne).

Le sous-ensemble est lu dans ./data (monté dans le conteneur sur
/opt/spark-apps/data). Chemin modifiable : ARXIV_SUBSET_PATH.
"""

from __future__ import annotations

import os
import time

import pandas as pd
from pyspark.sql import functions as F

from src.spark_jobs import config
from src.spark_jobs.spark_session import get_spark_session

SUBSET_PATH = os.getenv("ARXIV_SUBSET_PATH", "/opt/spark-apps/data/arxiv-metadata-100k.json")


def bench_pandas() -> tuple[float, int]:
    start = time.time()
    df = pd.read_json(SUBSET_PATH, lines=True)
    df["primary_category"] = df["categories"].str.split().str[0]
    df.groupby("primary_category").size().sort_values(ascending=False)
    return time.time() - start, len(df)


def bench_spark(spark, path: str) -> tuple[float, int]:
    start = time.time()
    df = spark.read.json(path)
    df = df.withColumn("primary_category", F.split("categories", " ")[0])
    df.groupBy("primary_category").count().orderBy(F.desc("count")).collect()
    elapsed = time.time() - start
    return elapsed, df.count()


def main():
    t_pandas, n_subset = bench_pandas()
    spark = get_spark_session("Benchmark Pandas vs Spark")
    t_spark_subset, _ = bench_spark(spark, f"file://{SUBSET_PATH}")
    t_spark_full, n_full = bench_spark(spark, config.ARXIV_RAW)
    spark.stop()

    print("\n" + "═" * 72)
    print(f"  {'Outil':<10}{'Volume':>14}{'Temps':>12}{'µs / article':>16}")
    for outil, n, t in [("Pandas", n_subset, t_pandas), ("Spark", n_subset, t_spark_subset),
                        ("Spark", n_full, t_spark_full)]:
        print(f"  {outil:<10}{n:>14,}{t:>11.2f}s{1e6 * t / max(n, 1):>16.1f}")
    print("═" * 72)
    ratio_volume = n_full / max(n_subset, 1)
    print(f"  Volume x{ratio_volume:.1f} → temps Spark x{t_spark_full / t_spark_subset:.1f} "
          f"(une dégradation linéaire donnerait x{ratio_volume:.1f})")


if __name__ == "__main__":
    main()
