"""
SciPulse Insights — Normalisation des clés de jointure

Fonctions pures (DataFrame → DataFrame) réutilisées par la couche Silver
et testées dans tests/test_transformations.py. Uniquement des fonctions
natives pyspark.sql.functions : pas d'UDF Python, donc pas de
sérialisation JVM ↔ Python et le Catalyst optimizer reste efficace.

Trois clés permettent de raccorder les sources :
  - arxiv_id   : identifiant ArXiv (ex. 2301.01234 ou hep-th/9901001)
  - doi_clean  : DOI en minuscules, sans préfixe https://doi.org/
  - title_clean: titre normalisé (repli quand aucune clé fiable n'existe)
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

# Identifiant ArXiv dans une URL : nouveau format (YYMM.NNNNN) ou ancien
# format (archive/NNNNNNN). Le suffixe de version (v2) et ".pdf" sont exclus.
ARXIV_URL_REGEX = (
    r"(?i)arxiv\.org/(?:abs|pdf|html|format)/"
    r"(\d{4}\.\d{4,5}|[a-z\-]+(?:\.[a-z]{2})?/\d{7})"
)
ARXIV_DOI_PREFIX = "10.48550/arxiv."


def normalize_doi(col: Column) -> Column:
    """'https://doi.org/10.1103/PhysRevD.76.013009 ' → '10.1103/physrevd.76.013009'.
    Le dump ArXiv contient parfois plusieurs DOIs séparés par un espace : on garde le premier."""
    first = F.split(F.trim(col), r"\s+").getItem(0)
    cleaned = F.lower(F.regexp_replace(first, r"(?i)^(https?://)?(dx\.)?doi\.org/", ""))
    cleaned = F.regexp_replace(cleaned, r"^doi:", "")
    return F.when(col.isNull() | (F.trim(col) == ""), F.lit(None)).otherwise(cleaned)


def normalize_title(col: Column) -> Column:
    """Minuscules, ponctuation retirée, espaces compactés."""
    no_punct = F.regexp_replace(F.lower(col), r"[^\w\s]", " ")
    return F.trim(F.regexp_replace(no_punct, r"\s+", " "))


def extract_arxiv_id_from_url(col: Column) -> Column:
    """URL contenant un lien arXiv → identifiant ArXiv en minuscules, sinon NULL."""
    found = F.lower(F.regexp_extract(F.coalesce(col, F.lit("")), ARXIV_URL_REGEX, 1))
    return F.when(found == "", F.lit(None)).otherwise(found)


def arxiv_id_from_arxiv_doi(doi_clean_col: Column) -> Column:
    """'10.48550/arxiv.2301.01234' → '2301.01234' (DOI DataCite attribué par arXiv)."""
    return F.when(
        doi_clean_col.startswith(ARXIV_DOI_PREFIX),
        F.substring_index(doi_clean_col, ARXIV_DOI_PREFIX, -1),
    )


# ── Compatibilité avec les noms du TP (J2) ────────────────────────────
def add_doi_clean_column(df: DataFrame, doi_col: str) -> DataFrame:
    return df.withColumn("doi_clean", normalize_doi(F.col(doi_col)))


def add_title_clean_column(df: DataFrame, title_col: str) -> DataFrame:
    return df.withColumn("title_clean", normalize_title(F.col(title_col)))
