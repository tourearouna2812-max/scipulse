# TP 1 : Prise en main de la pile de traitement de donnnées du projet SciPulse

## Objectif du TP

À la fin de cette session, vous aurez :

- Un dépôt Git initialisé avec la structure du projet.
- Une stack complète lancée via Docker Compose (Elasticsearch, Logstash, Kibana, Airflow, PostgreSQL, MinIO).
- Vérifié que chaque service est accessible.
- Téléchargé un subset du dump ArXiv et déposé les données dans MinIO.
- Écrit votre premier script Python d'interaction avec MinIO.

---

## Étape 1 — Initialiser le dépôt Git (20 min)

### 1.1. Cloner le starter kit

Vous pouvez télécharger le fichier de configuration Docker à cette adresse :

```bash
git clone <url-du-repo> scipulse
cd scipulse
```

### 1.2. Explorer la structure

La structure de l'application devrait ressembler à quelque chose comme :
```
scipulse/
├── docker-compose.yml       ← Stack complète
├── requirements.txt         ← Dépendances Python
├── Makefile                 ← Raccourcis (make up, make test…)
├── .env.example             ← Variables d'environnement
├── .gitignore
├── README.md
│
├── docker/                  ← Configs Docker par service
│   ├── logstash/
│   │   ├── config/logstash.yml
│   │   └── pipeline/arxiv.conf
│   └── postgres/
│       └── init.sql
│
├── airflow/                 ← DAGs et plugins Airflow
│   └── dags/
│       ├── dag_arxiv_pipeline.py
│       └── dag_hn_poller.py
│
├── src/                     ← Code source Python
│   ├── ingestion/
│   │   └── hn_poller.py
│   ├── search/
│   │   └── search_service.py
│   └── utils/
│       └── mappings.py
│
├── dbt/                     ← Projet dbt
├── tests/                   ← Tests Pytest
├── data/                    ← Données (non versionnées)
├── notebooks/               ← Exploration Jupyter
└── docs/                    ← Documentation
```

**Question à se poser** : quel pilier DataOps est déjà en action juste avec cette structure ? 

### 1.3. Configurer l'environnement

```bash
# Copier le fichier d'environnement
cp .env.example .env

# Créer un environnement virtuel Python
python -m venv .venv
source .venv/bin/activate    # Linux/macOS
# .venv\Scripts\activate     # Windows

# Installer les dépendances
pip install -r requirements.txt
```

Comme alternative, plus moderne, vous pouvez aussi utiliser le gestionnaire de paquets `uv` :
```bash
# Créer l'environnement virtuel 
uv init

# Ajouter les dépendances
uv add kaggle
# etc.

# Si le fichier `pyproject.toml` existe déjà :
uv sync
# ou
uv pip install -r pyproject.toml

# Exécuter un script Python
uv run python ...
# ou
uv run script.py
```

### 1.4. Créer votre branche de travail

```bash
git checkout -b feature/setup-<votre-nom>
```

**Bonne pratique DataOps** : ne jamais committer directement sur `main`. Toujours travailler sur une branche et faire une pull request. Même en TP, on prend les bonnes habitudes.

---

## Étape 2 — Comprendre le docker-compose.yml (20 min)

Avant de lancer quoi que ce soit, prenons le temps de **lire et comprendre** le fichier `docker-compose.yml`. C'est un exercice DataOps fondamental : on ne lance jamais une stack qu'on ne comprend pas.

### 2.1. Lecture guidée

Ouvrez `docker-compose.yml` dans votre éditeur. Identifions chaque service :

| Service | Image | Ports | Rôle |
|---------|-------|-------|------|
| `elasticsearch` | `elastic/elasticsearch:8.13.4` | 9200 | Moteur de recherche et stockage |
| `logstash` | `elastic/logstash:8.13.4` | — | Ingestion et parsing des données |
| `kibana` | `elastic/kibana:8.13.4` | 5601 | Interface d'exploration |
| `postgres` | `postgres:16-alpine` | 5432 | Backend Airflow + entrepôt dbt |
| `minio` | `minio/minio:latest` | 9000, 9001 | Stockage objet (landing zone) |
| `minio-init` | `minio/mc:latest` | — | Création des buckets au démarrage |
| `airflow-init` | `apache/airflow:2.9.3` | — | Initialisation de la DB Airflow |
| `airflow-webserver` | `apache/airflow:2.9.3` | 8080 | Interface web Airflow |
| `airflow-scheduler` | `apache/airflow:2.9.3` | — | Planificateur de tâches |

### 2.2. Points d'attention à discuter

**Les healthchecks** : Elasticsearch déclare un healthcheck (`curl -f http://localhost:9200/_cluster/health`). Les services qui en dépendent (`depends_on: elasticsearch: condition: service_healthy`) ne démarrent qu'une fois ES prêt. Sans ça, Logstash et Kibana planteraient au démarrage parce qu'ES n'est pas encore prêt à accepter des connexions.

```yaml
healthcheck:
  test: ["CMD-SHELL", "curl -f http://localhost:9200/_cluster/health || exit 1"]
  interval: 10s
  timeout: 5s
  retries: 10
```

**Les volumes nommés** : `es-data`, `pg-data` et `minio-data` sont des volumes Docker persistants. Les données survivent à un `docker compose down` mais pas à un `docker compose down -v` (qui supprime aussi les volumes).

**Le réseau** : tous les services sont sur le même réseau Docker `scipulse`. Ils se voient par leur nom de service (ex. : `http://elasticsearch:9200` depuis Logstash).

**La sécurité désactivée** : `xpack.security.enabled=false` sur ES et Kibana. En production, c'est évidemment interdit ! En TP, ça simplifie considérablement la configuration.

**L'ancre YAML `&airflow-common`** : c'est une technique DRY (Don't Repeat Yourself) pour factoriser la config commune entre les 3 services Airflow. Notez le `<<: *airflow-common` qui « hérite » de l'ancre.

**Le `minio-init`** : ce conteneur éphémère s'exécute une fois au démarrage pour créer les buckets (`arxiv-raw`, `hn-raw`, `processed`), puis s'arrête. C'est un pattern courant pour l'initialisation d'infrastructure.

### 2.3. Le fichier init.sql

PostgreSQL exécute automatiquement les scripts dans `/docker-entrypoint-initdb.d/` au premier démarrage. Notre `init.sql` crée une base `scipulse` séparée de la base `airflow`, avec deux schémas (`staging` et `marts`) pour dbt :

```sql
CREATE USER scipulse WITH PASSWORD 'scipulse';
CREATE DATABASE scipulse OWNER scipulse;

\c scipulse
CREATE SCHEMA IF NOT EXISTS staging AUTHORIZATION scipulse;
CREATE SCHEMA IF NOT EXISTS marts   AUTHORIZATION scipulse;
```

---

## Étape 3 — Lancer la stack (20 min)

### 3.1. Démarrage

```bash
# Depuis la racine du projet
docker compose up -d
```

Le premier lancement est long (téléchargement des images, ~5 Go au total). Les fois suivantes seront quasi-instantanées.

Suivez la progression :

```bash
# Voir l'état de tous les conteneurs
docker compose ps

# Suivre les logs en temps réel (Ctrl+C pour quitter)
docker compose logs -f
```

### 3.2. Vérification de chaque service

Attendez que tous les services soient `healthy` ou `running`, puis vérifiez un par un :

#### Elasticsearch

```bash
# Santé du cluster
curl -s 'http://localhost:9200/_cluster/health?pretty'
```

Vous devez voir :

```json
{
  "cluster_name": "scipulse-cluster",
  "status": "green",
  "number_of_nodes": 1,
  ...
}
```

Le statut doit être `green` (ou `yellow` en single-node, ce qui est normal).

```bash
# Version et infos
curl -s http://localhost:9200
```

#### Kibana

Ouvrez http://localhost:5601 dans votre navigateur. L'interface Kibana doit s'afficher.

Naviguez vers **Dev Tools** (menu gauche > Management > Dev Tools). C'est ici que vous écrirez vos requêtes Elasticsearch :

```
GET _cluster/health
```

Cliquez sur le bouton ▶ pour exécuter. Vous devez obtenir la même réponse que `curl`.

#### MinIO

Ouvrez http://localhost:9001 (console d'administration).

Identifiants : `minioadmin` / `minioadmin`.

Vérifiez que les 3 buckets ont été créés automatiquement :
- `arxiv-raw`
- `hn-raw`
- `processed`

Vous pouvez aussi vérifier en ligne de commande :

```bash
# Depuis le conteneur minio-init (s'il est encore là) ou en installant mc localement
docker run --rm --network=scipulse minio/mc alias set local http://minio:9000 minioadmin minioadmin && mc ls local/
```

#### PostgreSQL

```bash
# Connexion à la base scipulse
docker exec -it scipulse-postgres psql -U scipulse -d scipulse -c "\dn"
```

Vous devez voir les schémas `public`, `staging`, et `marts`.

#### Airflow

Ouvrez http://localhost:8080.

Identifiants : `admin` / `admin`.

Vous devez voir les deux DAGs :
- `scipulse_arxiv_pipeline` (pausé)
- `scipulse_hn_poller` (pausé)

Ne les activez pas encore — nous les utiliserons au Jour 4.

### 3.3. Résumé des URLs

| Service | URL | Identifiants |
|---------|-----|-------------|
| Elasticsearch | http://localhost:9200 | — |
| Kibana | http://localhost:5601 | — |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Airflow | http://localhost:8080 | admin / admin |
| PostgreSQL | localhost:5432 | scipulse / scipulse |

### 3.4. Dépannage courant

| Problème | Cause probable | Solution |
|----------|---------------|----------|
| ES ne démarre pas | RAM insuffisante | Allouer ≥ 8 Go à Docker Desktop |
| `max virtual memory areas` | Limite système Linux | `sudo sysctl -w vm.max_map_count=262144` |
| Port déjà utilisé | Un autre service écoute | `docker compose down` puis vérifier `lsof -i :9200` |
| Airflow init échoue | DB pas encore prête | `docker compose restart airflow-init` |
| MinIO buckets manquants | Init trop rapide | `docker compose restart minio-init` |

---

> ### ⚠️ Où s'exécutent vos commandes ? Hôte vs. conteneur Docker
>
> C'est la source de confusion la plus fréquente du TP. Prenez 3 minutes pour bien comprendre ce point — il vous évitera des heures de débogage.
>
> **Dans ce cours, deux contextes d'exécution coexistent :**
>
> **1. Votre machine hôte** (le terminal de votre laptop) : c'est là que vous exécutez les scripts Python pendant les TPs des Jours 1-3. Depuis l'hôte, les services Docker sont accessibles via `localhost` grâce au mapping de ports :
>
> ```
> Hôte                          Conteneur Docker
> ────                          ────────────────
> localhost:9200     ──────▶    elasticsearch:9200
> localhost:5601     ──────▶    kibana:5601
> localhost:5432     ──────▶    postgres:5432
> localhost:9000     ──────▶    minio:9000
> localhost:8080     ──────▶    airflow-webserver:8080
> ```
>
> Quand vous tapez `python -m src.ingestion.arxiv_bulk_ingest` dans votre terminal, le script utilise `http://localhost:9200` et ça fonctionne — le port est redirigé vers le conteneur Elasticsearch.
>
> **2. L'intérieur d'un conteneur Docker** : c'est là que s'exécutent les tâches Airflow (Jour 4), les pipelines Logstash, et toute commande lancée via `docker exec`. Depuis un conteneur, `localhost` désigne **le conteneur lui-même**, pas votre machine. Pour atteindre Elasticsearch depuis un autre conteneur, il faut utiliser le **nom de service Docker** : `http://elasticsearch:9200`.
>
> ```
> Depuis l'hôte (Jours 1-3)           Depuis un conteneur (Jour 4)
> ─────────────────────────           ────────────────────────────
> http://localhost:9200               http://elasticsearch:9200
> host=localhost (PG)                 host=postgres (PG)
> http://localhost:9000               http://minio:9000
> ```
>
> **C'est pourquoi nos scripts utilisent des variables d'environnement :**
>
> ```python
> import os
> ES_HOST = os.getenv("ES_HOST", "http://localhost:9200")
> ```
>
> - **Depuis l'hôte** : pas de variable définie → valeur par défaut `localhost:9200` → ça fonctionne.
> - **Depuis un conteneur Airflow** : la variable `ES_HOST=http://elasticsearch:9200` est définie dans `docker-compose.yml` → le script utilise le bon hostname.
>
> **Règle pratique** : pendant les Jours 1-3, vous travaillez depuis votre terminal hôte, tout utilise `localhost`. Au Jour 4, les scripts tournent dans Airflow (conteneur Docker), tout utilise les noms de services. Le code est le même — seule la variable d'environnement change.
>
> Si un script échoue avec `ConnectionError: Connection refused`, la première question à se poser est : « suis-je en train d'exécuter cette commande depuis l'hôte ou depuis un conteneur ? » et « est-ce que j'utilise le bon hostname ? ».

---

## Étape 4 — Préparer les données ArXiv (30 min)

### 4.1. Télécharger le dataset

Le dump complet ArXiv fait ~3.5 Go. Pour le TP, nous utiliserons un **subset** de ~100 000 articles filtrés sur les catégories Computer Science (`cs.*`) et Machine Learning (`stat.ML`).

**Option A — Dataset Kaggle (recommandé)** :

Si vous avez un compte Kaggle et le CLI installé :

```bash
pip install kaggle
kaggle datasets download -d Cornell-University/arxiv -p data/
```

**Option B — Subset pré-préparé par le formateur** :

Le formateur met à disposition un fichier `arxiv-cs-subset-100k.json` sur un serveur local ou un drive partagé :

```bash
# Adapter l'URL fournie par le formateur
curl -o data/arxiv-raw/arxiv-cs-subset-100k.json <URL>
```

**Option C — Générer le subset soi-même** :

Si vous avez le dump complet, filtrez-le avec un script Python :

```python
"""Extraction d'un subset CS/ML depuis le dump ArXiv complet."""
import json

INPUT  = "data/arxiv-metadata-oai-snapshot.json"
OUTPUT = "data/arxiv-raw/arxiv-cs-subset-100k.json"
MAX    = 100_000

"""
En ouvrant des fichiers en entrée et en sortie :
  Pour chaque ligne du fichier d'entrée
    Ligne la ligne
    Récupérer la catégorie de l'article
    Si la catégorie commence par "cs." ou "stat.ML" 
      Ecrire la ligne dans lefichier de sortie
      Incrémenter un compteur
    Si le compteur dépasse MAX
      Sortir de la boucle.
""" 
```

### 4.2. Inspecter les données

Avant de charger quoi que ce soit, **regardons les données**. C'est un réflexe DataOps : ne jamais ingérer à l'aveugle.

```bash
# Nombre de lignes
wc -l data/arxiv-raw/arxiv-cs-subset-100k.json

# Premiers articles
head -3 data/arxiv-raw/arxiv-cs-subset-100k.json | python -m json.tool
```

Observez et notez :

- Les champs disponibles.
- Le format du champ `authors` (chaîne brute vs. JSON structuré ?).
- La longueur typique d'un abstract.
- Le format des dates.
- Les catégories (mono vs. multi-valuées).

**Exercice** : Combien de catégories uniques contient le subset ?

```bash
cat data/arxiv-raw/arxiv-cs-subset-100k.json \
  | python -c ''
  """ Code """
  '
```

---

## Étape 5 — Charger les données dans MinIO (30 min)

### 5.1. Pourquoi MinIO ?

En production, les données brutes arrivent rarement directement dans le moteur de traitement. Elles atterrissent d'abord dans une **landing zone** — un stockage objet de type S3. Cela permet :

- De conserver une copie brute (immutabilité).
- De découpler l'ingestion du traitement.
- De rejouer le pipeline depuis les données brutes si nécessaire.

MinIO est un serveur S3-compatible local — parfait pour le TP.

### 5.2. Script Python d'upload

Créez le script `src/ingestion/upload_to_minio.py` :

```python
"""
SciPulse — Upload des données ArXiv vers MinIO
Découpe le dump JSON en chunks et les dépose dans le bucket arxiv-raw.
"""

import json
import os

import boto3
from botocore.client import Config

# ── Configuration MinIO ─────────────────────────────────────────────

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS   = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET   = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BUCKET         = "arxiv-raw"

# ── Connexion ────────────────────────────────────────────────────────

# Création du client S3
# s3 = ...

def upload_file(local_path: str, object_name: str | None = None):
    """Upload un fichier vers MinIO."""
    pass

def split_and_upload(
    source_file: str,
    chunk_size: int = 10_000,
    prefix: str = "chunks/",
):
    """
    Découpe un fichier JSON lines en chunks et les upload vers MinIO.
    Chaque chunk contient `chunk_size` articles.
    """
    pass

def _upload_chunk(lines: list[str], object_name: str):
    """Upload un chunk en mémoire vers MinIO."""
    pass

if __name__ == "__main__":
    import sys

    # Amélioration pour une application réelle :
    # Utiliser le module `argparse`
    source = sys.argv[1] if len(sys.argv) > 1 else "data/arxiv-raw/arxiv-cs-subset-100k.json"

    # 1) Traiter l'erreur si la source n'existe pas

    # 2) Afficher les information de chargement dans le bucket S3
    # -> Amélioration de bonne pratique : enregistrer l'nformation dans un fichier le log

    # 3) Upload du fichier complet

    # 4) Découpage en chunks de 10 000 articles
```

### 5.3. Exécuter le script

```bash
python -m src.ingestion.upload_to_minio data/arxiv-raw/arxiv-cs-subset-100k.json
```

Sortie attendue :

```
📤 Upload de data/arxiv-raw/arxiv-cs-subset-100k.json vers MinIO (bucket: arxiv-raw)
   Endpoint: http://localhost:9000

  ✅ arxiv-cs-subset-100k.json → s3://arxiv-raw/arxiv-cs-subset-100k.json

📦 Découpage en chunks de 10 000 articles...
  ✅ chunks/arxiv-chunk-0000.json (10000 articles)
  ✅ chunks/arxiv-chunk-0001.json (10000 articles)
  ...
  ✅ chunks/arxiv-chunk-0009.json (10000 articles)

🏁 10 chunks uploadés (100000 articles au total)
```

### 5.4. Vérifier dans MinIO Console

Retournez sur http://localhost:9001 et naviguez dans le bucket `arxiv-raw`. Vous devez voir :

- Le fichier complet `arxiv-cs-subset-100k.json`
- Le dossier `chunks/` avec 10 fichiers de 10 000 articles chacun

### 5.5. Vérification programmatique

```python
# Quick check — lister le contenu du bucket
import boto3
from botocore.client import Config

# Création d'unclient S3
# s3 = ...

# 1) Lire les objets du bucket arxiv-raw
# 2) Pour tous les objets lus, récupérer le contenuet  afficher la clef et la taille
```

---

## Étape 6 — Premier contact avec Elasticsearch (20 min)

Avant de configurer Logstash (cet après-midi), familiarisons-nous avec Elasticsearch via Kibana Dev Tools.

### 6.1. Indexer un document manuellement

Dans Kibana > Dev Tools, exécutez :

```
PUT /test-index/_doc/1
{
  "title": "A Comprehensive Survey of AI-Generated Content",
  "abstract": "Recently, AI-generated content (AIGC) has received increasing attention and is becoming a hot topic in both research and industry.",
  "categories": ["cs.AI", "cs.CL"],
  "date_published": "2023-02-07"
}
```

Réponse attendue :

```json
{
  "_index": "test-index",
  "_id": "1",
  "result": "created",
  ...
}
```

### 6.2. Rechercher le document

```
GET /test-index/_search
{
  "query": {
    "match": {
      "abstract": "AI content"
    }
  }
}
```

Observez :

- Le `_score` : c'est le score de pertinence BM25.
- Le `_source` : c'est le document stocké.
- Le `hits.total.value` : nombre de résultats.

### 6.3. Explorer le mapping auto-généré

```
GET /test-index/_mapping
```

Elasticsearch a **deviné** les types de champs. Regardez :

- `title` a été mappé en `text` (recherche full-text possible).
- `categories` a été mappé en `text` aussi (mais on voudrait `keyword` pour les agrégations !).
- `date_published` a été mappé en `date` (ES a détecté le format).

C'est exactement pourquoi nous définirons un **mapping explicite** demain : le mapping automatique fait des choix raisonnables mais rarement optimaux.

### 6.4. Nettoyer

```
DELETE /test-index
```

---

## Étape 7 — Commit et checkpoint (10 min)

Avant de partir en pause, committons notre travail :

```bash
# Ajouter les fichiers créés pendant le TP
git add src/ingestion/upload_to_minio.py
git add .env   # (si vous l'avez modifié)

# Commit
git commit -m "feat: setup environment + upload arxiv data to minio"

# Push (si un remote est configuré)
git push origin feature/setup-<votre-nom>
```

### Checklist de fin de matinée

Avant de passer à l'après-midi, vérifiez que vous avez :

- [ ] Un dépôt Git initialisé avec la structure du projet
- [ ] Docker Compose lancé avec tous les services en `running` ou `healthy`
- [ ] Kibana accessible sur http://localhost:5601
- [ ] MinIO accessible sur http://localhost:9001 avec les 3 buckets
- [ ] Airflow accessible sur http://localhost:8080 avec les 2 DAGs visibles
- [ ] Le dump ArXiv (ou subset) téléchargé dans `data/arxiv-raw/`
- [ ] Les données uploadées dans MinIO (fichier complet + chunks)
- [ ] Un document indexé et recherché manuellement dans ES via Dev Tools
- [ ] Un commit Git propre

---

## Ce qui vient cet après-midi

Cet après-midi, nous attaquons l'**ingestion à grande échelle** : configurer un pipeline Logstash qui parse automatiquement le dump ArXiv et indexe les 100 000 articles dans Elasticsearch. Nous comparerons avec un pipeline Python utilisant `elasticsearch-py` et les `bulk` helpers.

À la fin de la journée, vous aurez 100 000 articles scientifiques indexés dans Elasticsearch et consultables dans Kibana Discover — prêts à être modélisés et fouillés au Jour 2.

---

## Mémo — Commandes utiles

```bash
# Stack Docker
docker compose up -d              # Lancer tout
docker compose down               # Arrêter tout
docker compose down -v            # Arrêter + supprimer les volumes
docker compose logs -f <service>  # Suivre les logs d'un service
docker compose ps                 # État des conteneurs

# Elasticsearch
curl -s localhost:9200/_cluster/health?pretty   # Santé
curl -s localhost:9200/_cat/indices?v            # Liste des index

# MinIO (via Docker)
docker run --rm --network=scipulse minio/mc \
  alias set local http://minio:9000 minioadmin minioadmin && \
  mc ls local/arxiv-raw/

# Raccourcis Makefile
make up                           # docker compose up -d
make status                       # ps + indices ES
make down                         # docker compose down
make clean                        # down -v + nettoyage
```
