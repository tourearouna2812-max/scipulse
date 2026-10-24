# SciPulse Insights — Module 2 : Spark, citations et score d'impact

**Auteur :** Toure Arouna · Mastère Data & IA (AIA01) · HETIC

Le module 1 de SciPulse indexait les articles ArXiv et les posts Hacker News
dans Elasticsearch. Le module 2 répond à la question : **quels articles ont
un vrai impact, académique et auprès du public tech ?**

Il croise trois sources, ArXiv (métadonnées), **OpenAlex** (citations) et
**Hacker News** (écho tech), avec **Apache Spark**, calcule un **score
d'impact composite** et publie le résultat dans un index Elasticsearch
`arxiv-papers-enriched`, exploré dans un **dashboard Kibana**.

## Résultats clés

| Indicateur | Valeur |
|---|---|
| Articles traités (Gold) | **100 000** |
| Articles appariés à OpenAlex | **98 501 (98,5 %)** (19,7 % avant l'ingestion ciblée) |
| Articles discutés sur Hacker News | **518** |
| Qualité Silver citations (Great Expectations) | **7/7 expectations (100 %)** |
| Broadcast join vs sort-merge join | **1,93 s vs 9,71 s (× 5,0)** |
| TF-IDF (99 850 abstracts, 20 000 mots) | fit 52,5 s · transform 17,4 s |
| Tests unitaires | **10/10** |
| DAG Airflow complet | **13 min 56 s**, toutes les tâches en succès |
| N° 1 du score d'impact | *Scikit-learn: Machine Learning in Python* (65,49 / 100) |

## Architecture

```
            ┌──────────── BRONZE (MinIO, JSON Lines) ────────────┐
ArXiv dump ─►  arxiv-raw/        (module 1)                        │
OpenAlex   ─►  citations-raw/    fetch_openalex.py (par DOI ArXiv) │
HN Algolia ─►  hn-raw/           fetch_hn_arxiv.py                 │
            └───────────────────────┬────────────────────────────┘
                                    │ build_silver.py (Spark)
            ┌──────────── SILVER (MinIO, Parquet) ───────────────┐
            │ arxiv-clean · citations-clean · hn-clean           │
            │ ✔ Great Expectations (7/7) · quality gate Spark    │
            └───────────────────────┬────────────────────────────┘
                                    │ run_full_pipeline.py (Spark)
            jointures (broadcast) → window functions → TF-IDF → score
                                    │
            ┌──────────── GOLD (Elasticsearch) ──────────────────┐
            │ index arxiv-papers-enriched (mapping strict)       │
            └───────────────────────┬────────────────────────────┘
                                    ▼
                         Dashboard Kibana (7 visualisations)

  Orchestration : DAG Airflow scipulse_insights_spark (hebdomadaire)
```

## Arborescence (fichiers du module 2)

```
docker/spark/Dockerfile                 image Spark 3.5.0 + dépendances Python
docker-compose.yml                      service spark, socket Docker pour Airflow
scripts/spark.ps1                       lance un job Spark dans le conteneur (Windows)
src/ingestion/                          Bronze : OpenAlex, Hacker News (Algolia, export ES)
src/spark_jobs/
  config.py, spark_session.py           configuration centralisée, SparkSession S3A
  clean_join_keys.py                    normalisation DOI / titre / ID ArXiv
  build_silver.py                       Bronze → Silver (3 sources)
  check_citations_quality.py            quality gate Spark (miroir de la suite GX)
  join_sources.py, diagnose_slow_join.py  jointures multi-sources, broadcast vs SMJ
  citation_velocity.py                  window functions (lag, moyenne mobile, rank)
  tfidf_pipeline.py, find_similar_articles.py  TF-IDF MLlib, articles similaires
  impact_score.py                       score composite + analyse de sensibilité
  write_to_elasticsearch.py             écriture Gold
  run_full_pipeline.py                  enchaînement Silver → Gold (utilisé par Airflow)
  benchmark_pandas_vs_spark.py          benchmark J1
src/quality/validate_citations.py       suite Great Expectations 1.x (Data Docs)
src/utils/mappings_spark.py             création de l'index Gold (mapping explicite)
airflow/dags/dag_scipulse_insights.py   DAG d'orchestration
tests/test_transformations.py           10 tests unitaires
docs/MODULE2_PIPELINE_SPARK.md          documentation du pipeline et validation
docs/captures/                          captures Kibana, Airflow, qualité
```

## Prérequis

- Docker Desktop (≥ 12 Go de RAM alloués conseillés)
- Le stack du module 1 (Elasticsearch, Kibana, MinIO, Airflow, PostgreSQL), défini dans `docker-compose.yml`
- Une clé API OpenAlex gratuite (https://openalex.org) — uniquement pour réingérer les citations
- Le dump ArXiv dans MinIO (`arxiv-raw/`), déposé par le DAG batch du module 1

Configuration : copier le modèle puis le compléter (le `.env` n'est jamais commité) :

```powershell
Copy-Item .env.example .env      # Mac / Linux : cp .env.example .env
```

## Vérification rapide (1 minute, sans données ni clé API)

```powershell
docker compose build spark
docker compose up -d spark
docker compose exec spark python3 -m pytest /opt/spark-apps/tests -v     # attendu : 10 passed
```

Les tests utilisent des jeux de données construits à la main : ils vérifient la normalisation des
clés, le nettoyage des 3 sources, la priorité des méthodes de jointure, la vélocité et les bornes du score.

## Importer le dashboard Kibana

Kibana → *Stack Management* → *Saved objects* → *Import* →
`kibana/saved_objects/dashboard_scipulse_insights.ndjson`
(data view, 7 visualisations et dashboard « SciPulse Insights — Dashboard Gold »).

## Lancer le projet (PowerShell, depuis la racine)

> Sur Mac / Linux, `scripts/spark.ps1 <job>` équivaut à :
> `docker compose exec spark /opt/spark/bin/spark-submit --master "local[*]" --driver-memory 4g --conf spark.jars.ivy=/opt/ivy --packages org.apache.hadoop:hadoop-aws:3.3.4,org.elasticsearch:elasticsearch-spark-30_2.12:8.13.4 /opt/spark-apps/src/spark_jobs/<job>.py`

```powershell
# 1. Démarrer le stack et construire l'image Spark
docker compose build spark
docker compose up -d elasticsearch kibana minio minio-init spark

# 2. Tests unitaires
docker compose exec spark python3 -m pytest /opt/spark-apps/tests -v

# 3. Bronze (ingestion) → Silver
.\scripts\spark.ps1 build_silver --source arxiv
docker compose exec spark python3 -m src.ingestion.fetch_openalex --mode arxiv-ids --limit 100000
docker compose exec spark python3 -m src.ingestion.fetch_hn_arxiv --max-items 30000
.\scripts\spark.ps1 build_silver --source citations
.\scripts\spark.ps1 build_silver --source hn

# 4. Qualité
python -m src.quality.validate_citations          # Great Expectations (venv Windows)
.\scripts\spark.ps1 check_citations_quality       # quality gate Spark

# 5. Analyses et Gold
.\scripts\spark.ps1 diagnose_slow_join            # broadcast vs sort-merge
.\scripts\spark.ps1 tfidf_pipeline                # mots-clés (avant le pipeline complet)
docker compose exec spark python3 -m src.utils.mappings_spark
.\scripts\spark.ps1 run_full_pipeline             # jointures, vélocité, score, écriture ES

# 6. Vérification
curl.exe -s "localhost:9200/arxiv-papers-enriched/_count"
```

Kibana : http://localhost:5601 → dashboard **SciPulse Insights — Dashboard Gold**.

### Avec Airflow

```powershell
docker compose up -d airflow-webserver airflow-scheduler
```

http://localhost:8080 (admin / admin) → DAG **scipulse_insights_spark** →
*Trigger DAG w/ config*. Le paramètre `ingest` (décoché par défaut) réutilise
le Bronze déjà présent dans MinIO ; cochez-le pour réingérer OpenAlex et HN.

## Score d'impact

```
score_impact = 100 × (0,40 · citations + 0,25 · vélocité + 0,20 · HN + 0,15 · récence)
```

Chaque composante est passée en `log(1 + x)` puis normalisée par son maximum
(sauf la récence, `2^(−âge/3)`). Justification, analyse de sensibilité et
limites : `docs/MODULE2_PIPELINE_SPARK.md`, section 6.

## Limites connues

- Corpus 2007–2013 : peu d'articles récents, donc la récence départage peu
  et l'écho Hacker News (créé en 2007) reste rare (518 articles).
- TF-IDF sans lemmatisation (« network » et « networks » distincts).
- Moyennes par sous-domaine instables quand un sous-domaine compte peu d'articles.
- Exécution Spark en `local[*]` dans un seul conteneur : démontre l'API
  distribuée, pas un passage à l'échelle sur plusieurs machines.
