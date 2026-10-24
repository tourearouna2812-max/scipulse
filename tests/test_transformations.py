"""
SciPulse Insights — Tests unitaires des transformations Spark (jeu de test)

Petits DataFrames construits à la main, dont on connaît le résultat
attendu : normalisation des clés, nettoyage Silver, priorité des méthodes
de jointure, bornes et monotonie du score d'impact.

Lancement (dans le conteneur, aucun accès MinIO/ES nécessaire) :
    docker compose exec spark python3 -m pytest /opt/spark-apps/tests -v
"""

import datetime as dt

import pytest
from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F

from src.spark_jobs.build_silver import clean_arxiv, clean_citations, clean_hn
from src.spark_jobs.citation_velocity import compute_velocity, explode_counts_by_year, velocity_per_article
from src.spark_jobs.clean_join_keys import (
    arxiv_id_from_arxiv_doi,
    extract_arxiv_id_from_url,
    normalize_doi,
    normalize_title,
)
from src.spark_jobs.impact_score import compute_impact_score
from src.spark_jobs.join_sources import aggregate_hn, join_arxiv_citations


@pytest.fixture(scope="session")
def spark():
    # Session minimale : pas de connecteur S3/ES, les tests n'en ont pas besoin
    session = (SparkSession.builder.master("local[1]").appName("tests")
               .config("spark.sql.shuffle.partitions", "1")
               .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def single_col(spark, values, func):
    df = spark.createDataFrame([(v,) for v in values], "v string")
    return [r[0] for r in df.select(func(F.col("v"))).collect()]


# ── Clés de jointure ──────────────────────────────────────────────────
def test_normalize_doi(spark):
    got = single_col(spark, [
        "https://doi.org/10.1103/PhysRevD.76.013009",
        "http://dx.doi.org/10.1000/ABC ",
        "10.1016/j.x.2020.01.001 10.1016/j.y",
        "doi:10.5555/XYZ",
        "",
        None,
    ], normalize_doi)
    assert got == ["10.1103/physrevd.76.013009", "10.1000/abc", "10.1016/j.x.2020.01.001",
                   "10.5555/xyz", None, None]


def test_normalize_title(spark):
    got = single_col(spark, ["  Attention Is   All You Need! ", "Deep-Learning: a survey"], normalize_title)
    assert got == ["attention is all you need", "deep learning a survey"]


def test_extract_arxiv_id_from_url(spark):
    got = single_col(spark, [
        "https://arxiv.org/abs/2301.01234v2",
        "https://arxiv.org/pdf/1706.03762.pdf",
        "http://ARXIV.org/abs/hep-th/9901001v1",
        "https://export.arxiv.org/abs/2610.18773",
        "https://github.com/foo/bar",
        None,
    ], extract_arxiv_id_from_url)
    assert got == ["2301.01234", "1706.03762", "hep-th/9901001", "2610.18773", None, None]


def test_arxiv_id_from_arxiv_doi(spark):
    got = single_col(spark, ["10.48550/arxiv.2301.01234", "10.1103/physrevd.1", None], arxiv_id_from_arxiv_doi)
    assert got == ["2301.01234", None, None]


# ── Couche Silver ─────────────────────────────────────────────────────
def test_clean_arxiv(spark):
    raw = spark.createDataFrame([
        Row(id="2301.01234", title=" A   title ", abstract="A long enough abstract about transformers and data.",
            authors="A. B", categories="cs.LG stat.ML", doi="https://doi.org/10.1000/ABC",
            update_date="2023-02-01", versions=[Row(version="v1", created="Mon, 2 Jan 2023 10:00:00 GMT")]),
        Row(id="2301.01234", title="Doublon", abstract="dup abstract dup abstract dup abstract dup",
            authors="x", categories="cs.LG", doi=None, update_date="2023-02-01",
            versions=[Row(version="v1", created="Mon, 2 Jan 2023 10:00:00 GMT")]),
        Row(id="0704.0001", title="Retired", abstract="This paper has been withdrawn",
            authors="y", categories="hep-th", doi=None, update_date="2008-01-01",
            versions=[Row(version="v1", created="Mon, 2 Apr 2007 19:18:42 GMT")]),
    ])
    out = {r["arxiv_id"]: r for r in clean_arxiv(raw).collect()}
    assert len(out) == 2                                   # doublon retiré
    a = out["2301.01234"]
    assert a["title"] == "A title"
    assert a["primary_category"] == "cs.LG"
    assert a["doi_clean"] == "10.1000/abc"
    assert a["doi_arxiv"] == "10.48550/arxiv.2301.01234"
    assert a["published_date"] == dt.date(2023, 1, 2)
    assert a["is_withdrawn"] is False
    assert out["0704.0001"]["is_withdrawn"] is True
    assert out["0704.0001"]["published_year"] == 2007


def test_clean_hn_extracts_ids_and_dedupes(spark):
    items = spark.createDataFrame([
        (1, "Paper A", "https://arxiv.org/abs/2301.01234", 50, 10, 1700000000, "alice", "algolia"),
        (1, "Paper A", "https://arxiv.org/abs/2301.01234", 80, 12, 1700000000, "alice", "es_poller"),
        (2, "Not arXiv", "https://example.com", 500, 3, 1700000000, "bob", "algolia"),
        (3, "Paper A again", "https://arxiv.org/pdf/2301.01234v3", None, None, 1710000000, None, "algolia"),
    ], "hn_id long, hn_title string, url string, score int, comments int, time long, by string, hn_source string")
    out = {r["hn_id"]: r for r in clean_hn(items).collect()}
    assert set(out) == {1, 3}                              # item sans lien arXiv écarté
    assert out[1]["score"] == 80                           # version au meilleur score conservée
    assert out[3]["score"] == 0 and out[3]["by"] == "[deleted]"

    agg = aggregate_hn(clean_hn(items)).collect()[0]
    assert agg["arxiv_id"] == "2301.01234"
    assert (agg["hn_score"], agg["hn_comments"], agg["hn_posts"]) == (80, 12, 2)


def test_clean_citations_finds_arxiv_id(spark):
    raw = spark.createDataFrame([
        Row(id="https://openalex.org/W1", doi="https://doi.org/10.48550/arXiv.2301.01234",
            display_name="Title One", publication_year=2023, cited_by_count=12,
            locations=[Row(landing_page_url="https://arxiv.org/abs/2301.01234")]),
        Row(id="https://openalex.org/W2", doi="https://doi.org/10.1000/xyz",
            display_name="Title Two", publication_year=2021, cited_by_count=-3,
            locations=[Row(landing_page_url="http://arxiv.org/abs/2105.00001v2")]),
    ])
    out = {r["openalex_id"]: r for r in clean_citations(raw).collect()}
    assert out["W1"]["arxiv_id"] == "2301.01234"           # via le DOI DataCite arXiv
    assert out["W2"]["arxiv_id"] == "2105.00001"           # via l'URL de la location
    assert out["W2"]["cited_by_count"] == 0                # valeur négative ramenée à 0


# ── Couche Gold ───────────────────────────────────────────────────────
@pytest.fixture()
def arxiv_silver(spark):
    return spark.createDataFrame([
        ("a1", "10.1000/abc", "10.48550/arxiv.a1", "a fairly long title number one", "cs.LG"),
        ("a2", None, "10.48550/arxiv.a2", "another fairly long title two", "cs.CV"),
        ("a3", None, "10.48550/arxiv.a3", "an article nobody ever cited", "cs.AI"),
    ], "arxiv_id string, doi_clean string, doi_arxiv string, title_clean string, primary_category string")


@pytest.fixture()
def citations_silver(spark):
    return spark.createDataFrame([
        # W1 matche a1 à la fois par arxiv_id ET par titre : arxiv_id doit l'emporter
        ("W1", "a1", "10.48550/arxiv.a1", "a fairly long title number one", 10),
        ("W9", None, None, "a fairly long title number one", 999),
        # W2 ne matche a2 que par titre
        ("W2", None, None, "another fairly long title two", 5),
    ], "openalex_id string, arxiv_id string, doi_clean string, title_clean string, cited_by_count int") \
        .withColumn("counts_by_year", F.lit(None).cast("array<struct<year:int,cited_by_count:int>>")) \
        .withColumn("topic_name", F.lit("T")).withColumn("subfield_name", F.lit("S")) \
        .withColumn("field_name", F.lit("F")).withColumn("oa_publication_year", F.lit(2023))


def test_join_priority_and_no_duplicates(arxiv_silver, citations_silver):
    out = {r["arxiv_id"]: r for r in join_arxiv_citations(arxiv_silver, citations_silver).collect()}
    assert len(out) == 3                                   # une ligne par article, aucun perdu
    assert (out["a1"]["match_method"], out["a1"]["openalex_id"]) == ("arxiv_id", "W1")
    assert (out["a2"]["match_method"], out["a2"]["openalex_id"]) == ("title_fallback", "W2")
    assert out["a3"]["match_method"] == "none"


def test_velocity(spark):
    df = spark.createDataFrame([
        Row(arxiv_id="a1", primary_category="cs.LG", topic_name="T",
            counts_by_year=[Row(year=2023, cited_by_count=10), Row(year=2024, cited_by_count=30),
                            Row(year=2025, cited_by_count=20)]),
    ])
    v = velocity_per_article(compute_velocity(explode_counts_by_year(df)), ref_year=2025).collect()[0]
    assert v["velocite"] == -10                            # 20 − 30
    assert v["citations_3ans"] == pytest.approx(20.0)      # (10 + 30 + 20) / 3


def test_impact_score_bounds_and_monotonicity(spark):
    today = dt.date.today()
    enriched = spark.createDataFrame([
        ("old_cited", 1000, 300, today.replace(year=today.year - 10), False),
        ("new_viral", 2, 5000, today, False),
        ("nothing", 0, None, today.replace(year=today.year - 5), False),
    ], "arxiv_id string, cited_by_count int, hn_score int, published_date date, is_withdrawn boolean")
    velocity = spark.createDataFrame([("old_cited", 50.0)], "arxiv_id string, citations_3ans double")

    scores = {r["arxiv_id"]: r for r in compute_impact_score(enriched, velocity).collect()}
    for r in scores.values():
        assert 0.0 <= r["score_impact"] <= 100.0
        for comp in ("score_citations", "score_velocite", "score_hn", "score_recence"):
            assert 0.0 <= r[comp] <= 1.0
    assert scores["old_cited"]["score_citations"] == pytest.approx(1.0)
    assert scores["new_viral"]["score_hn"] == pytest.approx(1.0)
    assert scores["new_viral"]["score_recence"] > scores["old_cited"]["score_recence"]
    assert scores["nothing"]["score_impact"] < scores["old_cited"]["score_impact"]
