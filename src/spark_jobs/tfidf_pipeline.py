"""
SciPulse Insights — TF-IDF distribué (Spark MLlib) sur les abstracts ArXiv

Pipeline : RegexTokenizer → StopWordsRemover → CountVectorizer → IDF

Choix par rapport au TP (Tokenizer + HashingTF) :
  - RegexTokenizer : découpe sur tout ce qui n'est pas une lettre, ignore
    les tokens de moins de 3 caractères (formules, numéros, "et", "al").
  - CountVectorizer au lieu de HashingTF : le vocabulaire est conservé,
    ce qui permet de retrouver les MOTS les plus pondérés de chaque
    article (mots-clés exposés dans Kibana). HashingTF est plus économe en
    mémoire mais irréversible (on ne sait pas quel mot est derrière un
    index), et sujet aux collisions.
  - Stop words scientifiques ajoutés ("propose", "results", "paper"…),
    trop fréquents dans les abstracts pour être discriminants.

Écrit :
  - s3a://results/tfidf_vectors.parquet   (arxiv_id, title, tfidf)
  - s3a://results/arxiv_keywords.parquet  (arxiv_id, keywords[5])
"""

from __future__ import annotations

import logging
import time

from pyspark.ml import Pipeline
from pyspark.ml.feature import IDF, CountVectorizer, RegexTokenizer, StopWordsRemover
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType

from src.spark_jobs import config
from src.spark_jobs.spark_session import get_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

STOP_WORDS_SCIENTIFIQUES = [
    "paper", "propose", "proposed", "show", "shows", "shown", "results", "result", "using",
    "based", "method", "methods", "approach", "approaches", "also", "use", "used", "new",
    "present", "study", "work", "problem", "problems", "model", "models", "however",
    "well", "two", "one", "first", "can", "may", "via", "thus", "find", "found", "obtain",
    "obtained", "provide", "different", "large", "high", "low", "number", "general",
]


def build_tfidf_pipeline(vocab_size: int = 20_000, min_df: int = 5) -> Pipeline:
    tokenizer = RegexTokenizer(inputCol="abstract", outputCol="mots", pattern="[^a-zA-Z]+", minTokenLength=3)
    remover = StopWordsRemover(inputCol="mots", outputCol="mots_filtres")
    remover.setStopWords(remover.getStopWords() + STOP_WORDS_SCIENTIFIQUES)
    vectorizer = CountVectorizer(inputCol="mots_filtres", outputCol="tf", vocabSize=vocab_size, minDF=min_df)
    idf = IDF(inputCol="tf", outputCol="tfidf")
    return Pipeline(stages=[tokenizer, remover, vectorizer, idf])


def make_top_keywords_udf(vocabulary: list[str], k: int = 5):
    """UDF légitime : extraire les k plus forts poids d'un SparseVector n'a pas d'équivalent natif."""
    def top_keywords(vector):
        if vector is None or vector.numNonzeros() == 0:
            return []
        paires = sorted(zip(vector.indices, vector.values), key=lambda p: -p[1])[:k]
        return [vocabulary[int(i)] for i, _ in paires]
    return F.udf(top_keywords, ArrayType(StringType()))


def main():
    spark = get_spark_session("SciPulse — TF-IDF pipeline")

    df = (spark.read.parquet(config.ARXIV_CLEAN)
          .filter(F.col("abstract").isNotNull() & ~F.col("is_withdrawn"))
          .select("arxiv_id", "title", "abstract"))
    if config.TFIDF_SAMPLE < 1.0:
        df = df.sample(fraction=config.TFIDF_SAMPLE, seed=42)
        logger.warning(f"⚠️  Échantillon de {config.TFIDF_SAMPLE:.0%} du corpus (TFIDF_SAMPLE) — "
                       "à mentionner dans la doc pour interpréter les temps")
    df = df.cache()
    total = df.count()
    logger.info(f"📂 {total:,} abstracts à traiter")

    pipeline = build_tfidf_pipeline()

    start = time.time()
    modele = pipeline.fit(df)   # passe sur TOUT le corpus : vocabulaire + fréquences documentaires (IDF)
    t_fit = time.time() - start
    logger.info(f"⚙️  fit       : {t_fit:.1f}s")

    start = time.time()
    df_tfidf = modele.transform(df).select("arxiv_id", "title", "tfidf").cache()
    df_tfidf.count()
    t_transform = time.time() - start
    logger.info(f"⚙️  transform : {t_transform:.1f}s")

    vocabulaire = modele.stages[2].vocabulary
    logger.info(f"📚 Vocabulaire retenu : {len(vocabulaire):,} mots")

    df_keywords = df_tfidf.select("arxiv_id", make_top_keywords_udf(vocabulaire)("tfidf").alias("keywords"))
    logger.info("=== Exemples de mots-clés extraits ===")
    df_keywords.join(df_tfidf.select("arxiv_id", "title"), "arxiv_id").show(5, truncate=70)

    df_tfidf.write.mode("overwrite").parquet(config.TFIDF_VECTORS)
    df_keywords.write.mode("overwrite").parquet(config.KEYWORDS)

    logger.info("═" * 60)
    logger.info(f"  Abstracts traités : {total:,}")
    logger.info(f"  Temps fit         : {t_fit:.1f}s")
    logger.info(f"  Temps transform   : {t_transform:.1f}s")
    logger.info("═" * 60)
    spark.stop()


if __name__ == "__main__":
    main()
