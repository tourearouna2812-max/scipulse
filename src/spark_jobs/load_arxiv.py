"""
SciPulse Insights — Premier chargement du dump ArXiv avec Spark (J1)

Lit le dump depuis MinIO, affiche le schéma inféré et le plan
d'exécution, puis illustre la lazy evaluation : rien ne s'exécute
avant la première action (count).
"""

import time

from pyspark.sql import functions as F

from src.spark_jobs import config
from src.spark_jobs.spark_session import get_spark_session


def main():
    spark = get_spark_session("SciPulse — Load ArXiv")

    print(f"📂 Lecture de {config.ARXIV_RAW} (lazy)...")
    df = spark.read.json(config.ARXIV_RAW)
    df.printSchema()

    print("\n📐 Plan d'exécution AVANT toute action :")
    df.explain()

    start = time.time()
    total = df.count()
    print(f"\n⏱  count() total : {total:,} documents en {time.time() - start:.1f}s")

    df_cs_lg = df.filter(F.col("categories").contains("cs.LG"))
    start = time.time()
    n = df_cs_lg.count()
    print(f"⏱  count() filtré cs.LG : {n:,} articles en {time.time() - start:.1f}s")
    print(f"📊 Partitions : {df.rdd.getNumPartitions()} — cœurs : {spark.sparkContext.defaultParallelism}")

    print("\n📐 Plan détaillé du filtre (chercher PushedFilters / Filter) :")
    df_cs_lg.explain(True)
    spark.stop()


if __name__ == "__main__":
    main()
