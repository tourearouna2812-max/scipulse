"""
SciPulse Insights — DAG Airflow du module 2 (Bronze → Silver → Gold, Spark)

  ingest_citations ─► silver_citations ─┐
  ingest_hn_arxiv ──► silver_hn ────────┴─► quality_gate_citations
      ─► create_gold_index ─► spark_enrich_and_score ─► check_gold

Chaque tâche lance une commande DANS le conteneur scipulse-spark, via le
socket Docker et la bibliothèque Python "docker" (équivalent de "docker exec").
Pourquoi pas BashOperator + "docker exec" comme dans le TP J4 : l'image
apache/airflow ne contient pas le binaire docker ; le SDK Python suffit et
s'installe via _PIP_ADDITIONAL_REQUIREMENTS (cf. docker-compose.yml).

Paramètre "ingest" (case à cocher au lancement, "Trigger DAG w/ config") :
  - False (défaut) : on réutilise le Bronze déjà présent dans MinIO
    (évite de reconsommer le quota OpenAlex à chaque démonstration) ;
    les tâches d'ingestion sont marquées "skipped".
  - True : ré-ingestion OpenAlex + Hacker News avant de reconstruire Silver.

La qualité est contrôlée AVANT la Gold : si une règle échoue, le DAG
s'arrête et l'index Elasticsearch n'est pas modifié.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

SPARK_CONTAINER = "scipulse-spark"
ES_URL = "http://elasticsearch:9200"
ES_INDEX = "arxiv-papers-enriched"
PACKAGES = ("org.apache.hadoop:hadoop-aws:3.3.4,"
            "org.elasticsearch:elasticsearch-spark-30_2.12:8.13.4")


def spark_submit(job: str, args: str = "") -> str:
    return (f"/opt/spark/bin/spark-submit --master local[*] --driver-memory 4g "
            f"--conf spark.jars.ivy=/opt/ivy --packages {PACKAGES} "
            f"/opt/spark-apps/src/spark_jobs/{job}.py {args}").strip()


def python_module(module: str, args: str = "") -> str:
    return f"python3 -m {module} {args}".strip()


def run_in_spark(command: str, skip_if_no_ingest: bool = False, **context):
    """Exécute `command` dans le conteneur Spark, diffuse les logs, échoue si code retour ≠ 0."""
    if skip_if_no_ingest and not context["params"].get("ingest", False):
        raise AirflowSkipException("ingest=False : Bronze existant réutilisé")

    import docker  # installé via _PIP_ADDITIONAL_REQUIREMENTS

    client = docker.from_env()
    api = client.api
    print(f"▶ docker exec {SPARK_CONTAINER} {command}")
    exec_id = api.exec_create(SPARK_CONTAINER, ["bash", "-lc", command], workdir="/opt/spark-apps")["Id"]
    for chunk in api.exec_start(exec_id, stream=True):
        print(chunk.decode("utf-8", errors="replace"), end="")
    code = api.exec_inspect(exec_id)["ExitCode"]
    if code != 0:
        raise AirflowException(f"Commande en échec (code {code}) : {command}")


def check_gold_index(**_):
    """Contrôle de la couche Gold : l'index existe, n'est pas vide, et le score est renseigné."""
    import requests

    total = requests.get(f"{ES_URL}/{ES_INDEX}/_count", timeout=30).json()["count"]
    scored = requests.get(f"{ES_URL}/{ES_INDEX}/_count", timeout=30,
                          json={"query": {"exists": {"field": "score_impact"}}}).json()["count"]
    if total == 0:
        raise ValueError(f"Index {ES_INDEX} vide après le pipeline Spark")
    if scored < total:
        raise ValueError(f"{total - scored} documents sans score_impact sur {total}")
    print(f"✅ {ES_INDEX} : {total:,} documents, tous scorés")


def spark_task(task_id: str, command: str, skip_if_no_ingest: bool = False, **kwargs) -> PythonOperator:
    return PythonOperator(
        task_id=task_id,
        python_callable=run_in_spark,
        op_kwargs={"command": command, "skip_if_no_ingest": skip_if_no_ingest},
        **kwargs,
    )


default_args = {
    "owner": "toure-arouna",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="scipulse_insights_spark",
    description="Module 2 : citations OpenAlex + HN → Silver → Spark → Gold Elasticsearch",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="@weekly",
    catchup=False,
    max_active_runs=1,
    params={"ingest": Param(False, type="boolean",
                            description="Ré-ingérer OpenAlex et Hacker News (consomme le quota API)")},
    tags=["scipulse", "spark", "gold"],
) as dag:

    ingest_citations = spark_task(
        "ingest_citations",
        python_module("src.ingestion.fetch_openalex", "--mode arxiv-ids --limit 100000"),
        skip_if_no_ingest=True,
    )
    ingest_hn_arxiv = spark_task(
        "ingest_hn_arxiv",
        python_module("src.ingestion.fetch_hn_arxiv", "--max-items 30000"),
        skip_if_no_ingest=True,
    )

    # NONE_FAILED : la tâche tourne si l'ingestion a réussi OU a été sautée (ingest=False)
    silver_citations = spark_task("silver_citations", spark_submit("build_silver", "--source citations"),
                                  trigger_rule=TriggerRule.NONE_FAILED)
    silver_hn = spark_task("silver_hn", spark_submit("build_silver", "--source hn"),
                           trigger_rule=TriggerRule.NONE_FAILED)

    quality_gate_citations = spark_task("quality_gate_citations", spark_submit("check_citations_quality"))

    create_gold_index = spark_task("create_gold_index", python_module("src.utils.mappings_spark"))

    spark_enrich_and_score = spark_task(
        "spark_enrich_and_score",
        spark_submit("run_full_pipeline"),
        execution_timeout=timedelta(hours=2),
    )

    check_gold = PythonOperator(task_id="check_gold", python_callable=check_gold_index)

    ingest_citations >> silver_citations
    ingest_hn_arxiv >> silver_hn
    [silver_citations, silver_hn] >> quality_gate_citations >> create_gold_index \
        >> spark_enrich_and_score >> check_gold
