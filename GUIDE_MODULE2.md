# SciPulse Insights — Guide d'exécution du module 2 (Windows / PowerShell)

Ordre conseillé pour terminer la deuxième partie du projet. Toutes les
commandes se lancent **depuis la racine de ton projet**
(`C:\Users\DELL PC\Downloads\Projet SciPulse`), dans PowerShell, venv activé.

> **Architecture retenue (brief, Medallion)**
> Bronze (JSON Lines dans MinIO) → Silver (Parquet nettoyé, une table par source)
> → Gold (Spark : jointure × 3 + score) → index Elasticsearch `arxiv-papers-enriched` → Kibana.

---

## 0. Installer les fichiers (10 min)

1. Dézippe l'archive à la racine du projet. Elle ajoute ou remplace :
   - `docker/spark/Dockerfile`
   - `scripts/spark.ps1`
   - `src/spark_jobs/*.py` (remplace les scripts du TP du même nom)
   - `src/ingestion/fetch_openalex.py`, `fetch_hn_arxiv.py`, `export_hn_from_es.py`, `minio_utils.py`
   - `src/quality/validate_citations.py`
   - `src/utils/mappings_spark.py`, `src/utils/mapping_arxiv_papers_enriched.json`
   - `airflow/dags/dag_scipulse_insights.py`
   - `tests/test_transformations.py`
   - `docs/MODULE2_PIPELINE_SPARK.md`

   ⚠️ Si `src/ingestion/__init__.py`, `src/quality/__init__.py` ou `src/utils/__init__.py`
   existent déjà chez toi, garde les tiens (ceux de l'archive sont vides).

2. Ajoute le service `spark` de `docker-compose.spark.yml` dans ton `docker-compose.yml`
   (bloc `spark:` sous `services:` + volume `spark-ivy:` sous `volumes:`).
   Vérifie le nom du réseau avec `docker network ls`.

3. Dans ton fichier `.env` (jamais commité) :
   ```
   OPENALEX_API_KEY=ta_cle
   OPENALEX_MAILTO=ton.email@exemple.com
   ```

4. Dans ton venv Windows :
   ```powershell
   pip install requests boto3 python-dotenv "elasticsearch>=8,<9" pyarrow
   ```

5. Construis et démarre Spark :
   ```powershell
   docker compose build spark
   docker compose up -d spark
   docker compose ps
   ```
   UI Spark Master : http://localhost:8090

6. Vérifie la version d'Elasticsearch :
   ```powershell
   curl.exe -s localhost:9200
   ```
   Si `number` n'est pas en 8.x, définis la bonne version du connecteur
   avant de lancer Spark : `$env:ES_SPARK_VERSION="8.15.0"` (même numéro que ton ES).

7. Vérifie le nom du dump ArXiv dans MinIO :
   ```powershell
   docker compose exec minio mc ls local/arxiv-raw
   ```
   S'il ne s'appelle pas `arxiv-metadata-full.json`, ajoute dans `.env` :
   `ARXIV_RAW_PATH=s3a://arxiv-raw/<nom_du_fichier>`, puis `docker compose up -d spark`.

```powershell
git add docker/ docker-compose.yml scripts/ src/spark_jobs/config.py src/spark_jobs/spark_session.py src/ingestion/minio_utils.py
git commit -m "feat(spark): add Spark 3.5 service, shared config and SparkSession factory"
```

## 1. Tests unitaires (2 min) — valide que le conteneur fonctionne

```powershell
docker compose exec spark python3 -m pytest /opt/spark-apps/tests -v
```
10 tests doivent passer.

## 2. Buckets Medallion

```powershell
python -m src.ingestion.minio_utils
```

## 3. Jour 1 — prise en main (optionnel si déjà fait)

```powershell
.\scripts\spark.ps1 load_arxiv
.\scripts\spark.ps1 benchmark_pandas_vs_spark   # nécessite data\arxiv-metadata-100k.json
```

## 4. Silver ArXiv → ingestion des citations → Silver citations

```powershell
.\scripts\spark.ps1 build_silver --source arxiv
python -m src.ingestion.fetch_openalex --mode arxiv-ids --limit 20000
.\scripts\spark.ps1 build_silver --source citations
python -m src.quality.validate_citations
```
Fais une capture des Data Docs qui s'ouvrent (pour le rendu).

```powershell
git add src/ingestion/fetch_openalex.py src/spark_jobs/build_silver.py src/spark_jobs/clean_join_keys.py src/quality/validate_citations.py
git commit -m "feat(citations): OpenAlex bronze ingestion, silver layer and GX suite"
```

## 5. Hacker News → Silver

```powershell
python -m src.ingestion.fetch_hn_arxiv --max-items 20000
python -m src.ingestion.export_hn_from_es
.\scripts\spark.ps1 build_silver --source hn
```

```powershell
git add src/ingestion/fetch_hn_arxiv.py src/ingestion/export_hn_from_es.py
git commit -m "feat(hn): bronze HN stories linking to arXiv and silver layer"
```

## 6. Jour 2 — jointures

```powershell
.\scripts\spark.ps1 join_sources
.\scripts\spark.ps1 diagnose_slow_join
```
Note le tableau de couverture et le tableau sort-merge / broadcast dans
`docs/MODULE2_PIPELINE_SPARK.md`.

```powershell
git add src/spark_jobs/join_sources.py src/spark_jobs/diagnose_slow_join.py
git commit -m "feat(spark): three-way join with priority matching and broadcast diagnosis"
```

## 7. Jour 3 — analytique

```powershell
.\scripts\spark.ps1 citation_velocity
.\scripts\spark.ps1 tfidf_pipeline
.\scripts\spark.ps1 find_similar_articles --id 1706.03762
.\scripts\spark.ps1 impact_score
```
Si le TF-IDF est trop long : ajoute `TFIDF_SAMPLE=0.1` dans `.env`, puis
`docker compose up -d spark` (une variable `$env:` de PowerShell n'atteint
pas le conteneur), et signale l'échantillonnage dans la doc.

```powershell
git add src/spark_jobs/citation_velocity.py src/spark_jobs/tfidf_pipeline.py src/spark_jobs/find_similar_articles.py src/spark_jobs/impact_score.py
git commit -m "feat(spark): citation velocity, TF-IDF keywords and composite impact score"
```

## 8. Jour 4 — couche Gold

```powershell
python -m src.utils.mappings_spark
.\scripts\spark.ps1 write_to_elasticsearch
curl.exe -s localhost:9200/arxiv-papers-enriched/_count
curl.exe -s "localhost:9200/arxiv-papers-enriched/_search?size=3&sort=score_impact:desc"
```

Pipeline complet en une commande (ce que lance Airflow) :
```powershell
.\scripts\spark.ps1 run_full_pipeline
```

```powershell
git add src/utils/ src/spark_jobs/write_to_elasticsearch.py src/spark_jobs/run_full_pipeline.py
git commit -m "feat(gold): explicit mapping and distributed write to arxiv-papers-enriched"
```

## 9. Airflow

1. Copie `airflow/dags/dag_scipulse_insights.py` dans ton dossier de DAGs.
2. Les conteneurs Airflow doivent pouvoir lancer `docker exec` :
   socket `/var/run/docker.sock` monté et client docker installé
   (sinon : voir la section J4 de la doc pour l'alternative).
3. ```powershell
   docker exec scipulse-airflow-sched airflow dags reserialize
   ```
4. UI http://localhost:8080 → DAG `scipulse_insights_spark` → ▶. Capture d'écran du graphe en vert.

```powershell
git add airflow/dags/dag_scipulse_insights.py
git commit -m "feat(airflow): orchestrate Spark silver-to-gold pipeline"
```

## 10. Kibana

Suis la section **Dashboard Kibana** de `docs/MODULE2_PIPELINE_SPARK.md`
(data view, 6 visualisations, requêtes KQL des cas d'usage). Captures d'écran pour le rendu.

## 11. Documentation finale

Complète tous les `[à compléter]` de `docs/MODULE2_PIPELINE_SPARK.md`
avec tes propres mesures, puis :
```powershell
git add docs/ tests/ GUIDE_MODULE2.md
git commit -m "docs: Spark pipeline documentation, validation procedure and tests"
```

---

## Dépannage

| Symptôme | Cause probable | Solution |
|---|---|---|
| `pull access denied for bitnami/spark` | images Bitnami retirées de Docker Hub | utiliser le Dockerfile fourni (image officielle `spark:3.5.0`) |
| `ClassNotFoundException: S3AFileSystem` | jars non chargés | lancer via `scripts\spark.ps1` (passe `--packages`) |
| `UnknownHostException: minio` | Spark pas sur le même réseau que MinIO | même `networks:` que minio dans docker-compose |
| `Cannot detect ES version` | version du connecteur ≠ ES, ou mauvais hôte | `ES_SPARK_VERSION`, et `ES_NODES=elasticsearch` |
| `strict_dynamic_mapping_exception` | une colonne n'est pas dans le mapping | aligner `COLONNES_GOLD` et le JSON du mapping |
| `index_not_found_exception` | mapping pas créé | `python -m src.utils.mappings_spark` |
| `Path does not exist: s3a://arxiv-raw/...` | nom du dump différent | `ARXIV_RAW_PATH` dans `.env` |
| OpenAlex `HTTP 429` | quota | clé API dans `.env`, réduire `--limit` |
| `.\scripts\spark.ps1 cannot be loaded` | politique d'exécution | `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned` |
| `OutOfMemoryError` | dump complet en local | `$env:SPARK_DRIVER_MEMORY="6g"` ou `TFIDF_SAMPLE=0.1` |
