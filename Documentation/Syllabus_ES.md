# Ingénierie de Données & DataOps
## Plan de cours — 4,5 jours

---

## Philosophie du cours

Le fil conducteur est le **cycle de vie complet de la donnée** (ingestion → transport → stockage → transformation → indexation → exploration → restitution → monitoring), abordé sous l'angle **DataOps** : versioning, automatisation, tests de qualité, observabilité et déploiement reproductible.

Chaque demi-journée mêle un bloc théorique court (≈ 45 min) et une mise en pratique immédiate sur le projet fil rouge.

---

## Stack technique

### Socle imposé

| Outil | Rôle dans le pipeline |
|---|---|
| **Python** | Scripting, ingestion, transformation, orchestration |
| **Logstash** | Collecte, parsing et routage des flux de données |
| **Elasticsearch** | Stockage, indexation, recherche plein texte et analytique |
| **Kibana** | Exploration, visualisation et dashboarding |

### Outils complémentaires recommandés

| Outil | Rôle | Justification |
|---|---|---|
| **Docker / Docker Compose** | Conteneurisation de toute la stack | Reproductibilité des environnements (pilier DataOps) |
| **Apache Airflow** | Orchestration des pipelines (DAGs) | Planification, retry, lignage, alerting |
| **PostgreSQL** | Base relationnelle source / référentiel | Miroir tabulaire pour transformations dbt |
| **Great Expectations** | Tests de qualité des données | Validation automatisée à chaque étape (DataOps) |
| **dbt (data build tool)** | Transformation SQL versionnée | Modélisation, tests, documentation as code |
| **MinIO** | Stockage objet S3-compatible (local) | Zone de landing / data lake léger |
| **Git + GitHub/GitLab** | Versioning du code ET des configs | Fondation DataOps (IaC, CI/CD) |
| **Filebeat** | Collecte légère de fichiers / logs | Complément à Logstash pour l'ingestion distribuée |
| **Pandas / Polars** | Manipulation tabulaire en Python | Nettoyage, enrichissement, feature engineering |
| **Pytest** | Tests unitaires du code pipeline | Qualité du code (DataOps) |

---

## Projet fil rouge — « SciPulse — Veille scientifique & Tech augmentée »

### Pitch

Les apprenants construisent **SciPulse**, une plateforme de veille intelligente qui croise deux sources de données complémentaires :

- **ArXiv** (source batch) : le dump des métadonnées d'articles scientifiques (2M+ articles, disponible sur Kaggle en JSON). Chaque article contient un identifiant, un titre, un abstract riche, une liste d'auteurs, des catégories hiérarchiques (cs.AI, cs.LG, stat.ML…), et une date de publication.

- **Hacker News** (source streaming) : le flux temps réel des posts et commentaires via l'API Firebase. Chaque item contient un titre, un score, un auteur, un nombre de commentaires, un timestamp, et éventuellement une URL.

Le pipeline ingère les deux sources en parallèle — batch pour ArXiv, micro-batch/polling pour HN —, les nettoie, les enrichit (extraction de topics, détection de liens ArXiv dans les posts HN, scoring de popularité), les indexe dans Elasticsearch, et les expose dans un dashboard Kibana unifié. Le tout est orchestré par Airflow, versionné sous Git, testé par Great Expectations, et déployé via Docker Compose.

### Pourquoi ce sujet ?

**Fouille de texte riche (Elasticsearch) :**
- Les abstracts ArXiv sont des blocs de texte dense et technique → `match`, `multi_match` cross-fields (titre + abstract), `match_phrase` avec slop pour retrouver des expressions proches.
- `more_like_this` : à partir d'un article donné, trouver les 10 publications les plus similaires dans le corpus.
- `significant_terms` : quels termes distinguent les papiers de « cs.AI » de ceux de « cs.CR » (cryptographie) ? Quels mots-clés émergent dans les publications récentes vs. le corpus historique ?
- `highlight` : surligner les termes trouvés dans les abstracts pour afficher des résultats de recherche lisibles.
- `suggest` (did-you-mean) : correction de saisie sur le vocabulaire scientifique (« transformmer » → « transformer »).
- `function_score` : pondérer le score de pertinence textuelle par la fraîcheur de l'article (decay gaussien sur la date) ou par le score HN du post associé.
- Analyseur custom : stemming anglais scientifique, synonymes techniques (« CNN » ↔ « convolutional neural network »), stopwords adaptés.
- `bool` queries complexes : combiner filtre par catégorie (keyword), plage de dates (range), recherche textuelle (match) et boost par popularité HN.

**Double pattern d'ingestion :**
- ArXiv = batch classique (dump JSON → Logstash → ES), illustration du pattern ETL/ELT.
- Hacker News = polling temps réel (API Firebase → script Python → ES), illustration du pattern micro-batch/streaming.
- Les deux flux convergent dans Elasticsearch, ce qui permet de démontrer un pipeline multi-sources réaliste.

**Données structurées + non-structurées :**
- Structuré : catégories, auteurs, dates, scores HN, nombre de commentaires.
- Non-structuré : abstracts, titres, commentaires HN.
- Relation entre les deux sources : un post HN peut contenir un lien vers un article ArXiv → enrichissement croisé.

**Volumétrie et motivation :**
- Le dump ArXiv Kaggle contient 2M+ articles → volumétrie réaliste pour tester les performances ES.
- Sujet prestigieux et motivant : explorer la recherche en IA, en crypto, en physique.
- Les apprenants peuvent personnaliser leur dashboard selon leurs centres d'intérêt scientifiques.

### Architecture du pipeline

```
┌─────────────────────────────────────────────────────────────────────┐
│                        SOURCES DE DONNÉES                          │
├──────────────────────────────┬──────────────────────────────────────┤
│  ArXiv (batch)               │  Hacker News (streaming)            │
│  Dump Kaggle JSON            │  API Firebase (polling)             │
│  ~2M articles                │  ~500 items/heure                   │
└──────────┬───────────────────┴──────────────┬──────────────────────┘
           │                                  │
           ▼                                  ▼
┌──────────────────────┐         ┌──────────────────────────┐
│  MinIO (landing zone)│         │  Script Python            │
│  arxiv-raw/          │         │  hn_poller.py             │
└──────────┬───────────┘         │  (requests → ES direct)   │
           │                     └────────────┬──────────────┘
           ▼                                  │
┌──────────────────────┐                      │
│  Logstash            │                      │
│  Pipeline: arxiv     │                      │
│  input: file (JSON)  │                      │
│  filter: json, mutate│                      │
│  output: ES          │                      │
└──────────┬───────────┘                      │
           │                                  │
           ▼                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        ELASTICSEARCH                                │
│  Index: arxiv-papers     (mapping optimisé full-text)              │
│  Index: hn-items         (posts + commentaires, parent-child)      │
│  Index: arxiv-hn-links   (enrichissement croisé)                   │
│  Index: pipeline-logs    (observabilité)                           │
└──────────┬──────────────────────────────────────────────────────────┘
           │
     ┌─────┴─────┐
     ▼           ▼
┌─────────┐ ┌──────────────┐
│ Kibana  │ │ PostgreSQL   │
│ Dashb.  │ │ (miroir dbt) │
└─────────┘ └──────────────┘
```

### Livrables attendus

1. Un dépôt Git contenant tout le code, les configs et la documentation.
2. Un `docker-compose.yml` lançant l'ensemble de la stack (ES, Logstash, Kibana, Airflow, PostgreSQL, MinIO).
3. Deux pipelines d'ingestion fonctionnels : Logstash (ArXiv batch) + Python (HN streaming).
4. Un DAG Airflow orchestrant le pipeline complet (batch + déclenchement du poller HN).
5. Deux index Elasticsearch avec mappings optimisés (un pour ArXiv, un pour HN), plus un index de liens croisés.
6. Un module Python `search_service.py` encapsulant les requêtes full-text avancées (more_like_this, significant_terms, suggest, function_score).
7. Un dashboard Kibana « SciPulse » répondant aux questions métier ci-dessous.
8. Une suite de tests Great Expectations validant la qualité des données aux deux étapes (raw + enrichi).
9. Une présentation orale de 15-20 minutes par groupe.

### Questions métier à explorer (exemples pour le dashboard)

- **Tendances** : quelles sont les 20 catégories ArXiv les plus actives ce mois-ci ? Comment évolue le volume de publications en cs.AI vs. cs.CR vs. stat.ML sur 5 ans ?
- **Fouille de texte** : recherche « reinforcement learning robotics » avec `multi_match` sur titre + abstract, highlighting, et scoring pondéré par fraîcheur (`function_score` + decay).
- **Similarité** : à partir d'un article donné (par son ID ArXiv), afficher les 10 articles les plus proches via `more_like_this`.
- **Termes émergents** : via `significant_terms`, quels mots-clés distinguent les publications des 3 derniers mois par rapport au corpus historique ? (Détection de buzzwords émergents.)
- **Croisement ArXiv × HN** : quels articles ArXiv ont été les plus discutés sur Hacker News ? Corrélation entre score HN et nombre de citations ?
- **Analyse HN** : quels sont les sujets tech qui génèrent le plus de commentaires ? Évolution temporelle du score moyen par type de post (Show HN, Ask HN, article).
- **Suggestion** : correction de saisie « trnasformer atention » → « transformer attention ».

---

## Déroulé détaillé

### JOUR 1 — Fondations : DataOps, environnement et ingestion batch

#### Matin (3h30)

**Bloc théorique — DataOps & cycle de vie de la donnée (1h)**
- Définition et principes du DataOps (Agile + DevOps appliqués aux données).
- Le cycle de vie : ingestion → stockage → transformation → serving → monitoring.
- Les 3 piliers : automatisation, observabilité, collaboration.
- Panorama de la stack du cours et positionnement de chaque outil.
- Présentation du projet SciPulse : architecture cible, sources de données, livrables.

**TP — Mise en place de l'environnement (2h30)**
- Initialisation du dépôt Git (structure, `.gitignore`, `README`, branching model).
- Rédaction du `docker-compose.yml` : Elasticsearch (single-node), Logstash, Kibana, PostgreSQL, MinIO.
- Vérification : accès Kibana (http://localhost:5601), connexion PostgreSQL, bucket MinIO.
- Téléchargement du dump ArXiv (subset de ~100k articles pour le TP, full dataset en option).
- Premier script Python : découpage du dump JSON en fichiers de taille raisonnable et dépôt dans MinIO (bucket `arxiv-raw/`).

#### Après-midi (3h30)

**Bloc théorique — Ingestion et collecte (45 min)**
- Patterns d'ingestion : batch vs. streaming vs. micro-batch.
- Logstash : architecture (input → filter → output), codecs, pipelines multiples.
- Filebeat comme collecteur léger.
- Cas d'usage : quand utiliser Logstash vs. un script Python custom.

**TP — Pipeline d'ingestion batch ArXiv avec Logstash (2h45)**
- Configuration d'un pipeline Logstash pour ArXiv :
  - Input : plugin `file` (JSON depuis le volume partagé MinIO).
  - Filtres : `json` (parsing), `mutate` (renommage de champs, suppression de champs inutiles), `date` (parsing de la date de publication), `split` (éclatement du champ `authors` en tableau).
  - Output : Elasticsearch (index `arxiv-papers-raw`).
- Pipeline Python alternatif avec `elasticsearch-py` et `bulk` helpers pour comparer les approches et illustrer l'ingestion programmatique.
- Validation : vérifier les documents dans Kibana > Discover, compter les documents, inspecter un échantillon.

---

### JOUR 2 — Modélisation Elasticsearch, ingestion streaming et qualité

#### Matin (3h30)

**Bloc théorique — Modélisation dans Elasticsearch (45 min)**
- Concepts : index, mapping, types de champs (`text`, `keyword`, `date`, `integer`, `nested`…).
- Analyseurs : standard, anglais, custom. Tokenizer, filtres de tokens (stopwords, stemming Porter, synonymes techniques).
- Mapping explicite vs. dynamique : pourquoi le contrôle est essentiel.
- Multi-fields : un même champ en `text` (recherche full-text) ET `keyword` (agrégation, tri).
- Relations : nested objects vs. parent-child (join field) — cas des posts et commentaires HN.

**TP — Mapping optimisé ArXiv + début ingestion HN (2h45)**
- Conception du mapping de l'index `arxiv-papers` :
  - `title` : multi-field (`text` avec analyseur anglais custom + `keyword`).
  - `abstract` : `text` avec analyseur custom (stemming, synonymes scientifiques).
  - `authors` : `nested` (nom + affiliation quand disponible).
  - `categories` : `keyword` (multi-valued).
  - `date_published` : `date`.
  - `arxiv_id` : `keyword`.
- Création de l'index avec le mapping via l'API REST (`PUT`).
- Réindexation depuis `arxiv-papers-raw` via `_reindex` ou script Python.
- Conception du mapping `hn-items` : utilisation d'un `join` field pour la relation post/commentaire.
- Début du script `hn_poller.py` : polling de l'API HN Firebase (`/v0/newstories`, `/v0/item/{id}`), transformation et indexation dans ES.

#### Après-midi (3h30)

**Bloc théorique — Qualité des données & Great Expectations (45 min)**
- Pourquoi la qualité est un pilier DataOps (shift-left testing).
- Great Expectations : Expectations, Suites, Checkpoints, Data Docs.
- Stratégies de validation pour des données hétérogènes (abstracts de longueur variable, champs optionnels).

**TP — Tests de qualité et nettoyage (2h45)**
- Installation de Great Expectations, connexion à la source (dump ArXiv en CSV/Parquet ou PostgreSQL).
- Rédaction d'une suite d'expectations pour ArXiv :
  - `arxiv_id` jamais null et unique.
  - `abstract` longueur minimale de 50 caractères.
  - `categories` appartenant à un ensemble connu.
  - `date_published` dans une plage réaliste (1991–2025).
  - Pas de doublons sur `arxiv_id`.
- Rédaction d'une suite d'expectations pour HN :
  - `id` jamais null et unique.
  - `score` ≥ 0.
  - `type` dans {story, comment, job, poll}.
  - `time` (timestamp) dans une plage raisonnable.
- Exécution des Checkpoints, consultation des Data Docs (rapport HTML).
- Script Python de nettoyage (Pandas/Polars) : déduplication ArXiv, normalisation des noms d'auteurs, extraction des catégories primaires et secondaires.
- Rechargement des données nettoyées dans Elasticsearch.

---

### JOUR 3 — Transformation, enrichissement et fouille de texte

#### Matin (3h30)

**Bloc théorique — Transformation et enrichissement (45 min)**
- Patterns de transformation : ELT vs. ETL, idempotence, lignage.
- dbt : models, refs, tests, documentation.
- Enrichissement en Python : NLP léger, détection de liens croisés, scoring composite.

**TP — Pipeline de transformation et enrichissement croisé (2h45)**
- Mise en place de dbt sur PostgreSQL (miroir tabulaire des données ArXiv).
- Modèles dbt :
  - `stg_arxiv_papers` : staging avec normalisation.
  - `dim_categories` : table dimensionnelle des catégories ArXiv avec hiérarchie (domaine / sous-domaine).
  - `fct_publications_par_mois` : table de faits agrégée.
  - `fct_auteurs_prolifiques` : top auteurs par nombre de publications et par catégorie.
- Tests dbt : unicité, non-null, valeurs acceptées, relations référentielles.
- Script Python d'enrichissement :
  - Extraction de mots-clés depuis les abstracts par TF-IDF léger ou pattern matching (regex sur les termes techniques récurrents).
  - Détection de liens ArXiv dans les posts HN (`url` contenant `arxiv.org/abs/`) → création de l'index `arxiv-hn-links` (ID ArXiv + ID HN + score HN).
  - Réindexation des articles ArXiv enrichis avec un champ `hn_score` et `hn_comments_count` quand un lien existe.

#### Après-midi (3h30)

**Bloc théorique — Fouille de texte avec Elasticsearch (1h)**
- Recherche plein texte : `match`, `multi_match`, `match_phrase`, `bool` query.
- Scoring BM25 : principes et tuning (`boost`, `function_score`, `decay` sur la date).
- Agrégations textuelles : `terms`, `significant_terms`, `sampler`.
- Requêtes avancées : `more_like_this`, `highlight`, `suggest` (did-you-mean).
- Relations parent-child : requêtes `has_child` / `has_parent` pour naviguer posts ↔ commentaires HN.

**TP — Exploration full-text et agrégations (2h30)**
- Atelier guidé dans Kibana Dev Tools :
  - Recherche « reinforcement learning multi-agent » avec `multi_match` cross-fields (titre + abstract), `boost` sur le titre.
  - `match_phrase` avec `slop:2` : retrouver « neural network architecture » même si les mots ne sont pas exactement consécutifs.
  - Highlighting des termes trouvés dans les abstracts.
  - `function_score` : combiner pertinence textuelle + decay gaussien sur `date_published` (articles récents boostés) + boost si `hn_score > 100`.
  - Agrégation `significant_terms` : quels termes distinguent les papiers cs.AI publiés en 2024 vs. le corpus 2018-2020 ? (Détection de tendances : « diffusion », « LLM », « RLHF »…)
  - Requête `more_like_this` : à partir d'un abstract donné, trouver les 10 articles les plus similaires.
  - Suggestion `phrase_suggest` : correction de saisie scientifique (« convlutional nueral network » → « convolutional neural network »).
  - Requête `has_child` : trouver les posts HN ayant des commentaires contenant « breakthrough ».
- Encapsulation des requêtes dans un module Python (`search_service.py`) avec une interface propre.
- Tests unitaires (Pytest) du module de recherche : vérifier que `more_like_this` retourne des résultats, que le suggest corrige bien les fautes, que le scoring respecte l'ordre attendu.

---

### JOUR 4 — Orchestration, dashboarding et observabilité

#### Matin (3h30)

**Bloc théorique — Orchestration avec Airflow (45 min)**
- Concepts : DAG, Task, Operator, XCom, scheduling, retry, SLA.
- Bonnes pratiques DataOps : DAGs versionnés, tests, idempotence.
- Sensors et hooks : déclencher un DAG à l'arrivée d'un fichier dans MinIO.
- Pattern : orchestration d'un pipeline multi-sources (batch + polling).

**TP — DAG Airflow du pipeline complet (2h45)**
- Ajout d'Airflow au `docker-compose.yml` (webserver, scheduler, worker, PostgreSQL backend).
- Écriture du DAG principal `scipulse_pipeline` :
  1. `download_arxiv_dump` — télécharger le dump depuis Kaggle/source vers MinIO.
  2. `validate_raw_arxiv` — exécuter le Checkpoint Great Expectations sur les données brutes.
  3. `ingest_arxiv_logstash` — déclencher le pipeline Logstash (via `BashOperator` ou API).
  4. `transform_dbt` — lancer `dbt run` + `dbt test` sur PostgreSQL.
  5. `enrich_arxiv_python` — enrichissement NLP + réindexation ES.
  6. `validate_enriched` — second Checkpoint GE sur les données enrichies.
- Écriture d'un DAG secondaire `hn_poller_dag` :
  1. Exécuté toutes les 5 minutes (ou via un sensor).
  2. `poll_hn_api` — appeler `hn_poller.py` pour récupérer les derniers items.
  3. `detect_arxiv_links` — croiser les URLs HN avec les arxiv_id existants.
  4. `update_arxiv_hn_scores` — mettre à jour les scores croisés dans ES.
- Test des DAGs : trigger manuel, vérification du graph view, inspection des logs, gestion des erreurs.

#### Après-midi (3h30)

**Bloc théorique — Visualisation et observabilité (45 min)**
- Kibana : Index patterns, Discover, Lens, Dashboard, Canvas.
- Principes de dataviz appliqués aux dashboards opérationnels.
- Observabilité du pipeline : logs, métriques, alertes.

**TP — Dashboard Kibana « SciPulse » et monitoring (2h45)**
- Création des index patterns dans Kibana (un par index).
- Construction du dashboard « SciPulse » :
  - **Area chart** : évolution mensuelle du volume de publications par catégorie ArXiv (top 5).
  - **Bar chart horizontal** : top 20 des termes significatifs dans les abstracts récents (`significant_terms`).
  - **Metric tiles** : nombre total d'articles, nombre de posts HN liés, score HN moyen des articles les plus populaires.
  - **Data table** : résultats de recherche full-text avec highlighting dans l'abstract.
  - **Tag cloud** : mots-clés émergents extraits des titres récents.
  - **Line chart** : évolution du score HN moyen pour les articles ArXiv liés, par semaine.
  - **Pie chart** : répartition des types de posts HN (story, comment, job, poll).
- Ajout d'un index dédié `pipeline-logs` alimenté par Filebeat depuis les logs Airflow et Logstash.
- Dashboard de monitoring du pipeline :
  - Taux de succès/échec des tâches Airflow.
  - Durée d'exécution par tâche (histogramme).
  - Volume de documents ingérés par run.
  - Alertes : Watcher ES si le taux d'erreur dépasse un seuil.

---

### JOUR 5 (matin uniquement) — Présentations orales

#### Matin (3h30)

**Présentations orales par groupe (2h30 – 3h)**
- Chaque groupe présente (15-20 min + 5-10 min de questions) :
  - L'architecture du pipeline (schéma des flux batch + streaming).
  - Les choix techniques et les difficultés rencontrées.
  - Démonstration live :
    - Lancer le DAG Airflow et montrer l'exécution.
    - Montrer le poller HN en action (items arrivant dans Kibana Discover).
    - Exécuter une recherche full-text avec `more_like_this` ou `significant_terms`.
    - Présenter le dashboard SciPulse avec les visualisations.
  - Les résultats des tests de qualité (Data Docs Great Expectations).
  - Les insights découverts (tendances, termes émergents, articles populaires sur HN).
  - Ce qu'ils amélioreraient avec plus de temps (ML, embeddings sémantiques, alertes temps réel…).

**Débrief collectif et bilan (30 min – 1h)**
- Retour sur les bonnes pratiques DataOps vues en action.
- Discussion : batch vs. streaming en production, scalabilité, coûts.
- Comment transposer ces compétences en entreprise.
- Ressources pour aller plus loin.

---

## Récapitulatif des demi-journées

| # | Demi-journée | Thème principal | Outils clés |
|---|---|---|---|
| 1 | J1 matin | DataOps, environnement Docker, Git | Docker Compose, Git, MinIO |
| 2 | J1 après-midi | Ingestion batch (ArXiv) | Logstash, Python, elasticsearch-py |
| 3 | J2 matin | Mapping ES, ingestion streaming (HN) | Elasticsearch, Python (API Firebase) |
| 4 | J2 après-midi | Qualité des données | Great Expectations, Pandas/Polars |
| 5 | J3 matin | Transformation, enrichissement croisé | dbt, Python (TF-IDF, liens ArXiv↔HN) |
| 6 | J3 après-midi | Fouille de texte avancée | ES (full-text, MLT, significant_terms, function_score, parent-child) |
| 7 | J4 matin | Orchestration multi-sources | Airflow (2 DAGs : batch + polling) |
| 8 | J4 après-midi | Dashboarding & Observabilité | Kibana, Filebeat, Watcher |
| 9 | J5 matin | **Présentations orales** | — |

---

## Données sources — Détails pratiques

### ArXiv

- **Source** : Kaggle dataset « arxiv-metadata-oai-snapshot » (~3.5 Go JSON, 2M+ articles).
- **Subset recommandé pour le TP** : filtrer sur les catégories `cs.*` et `stat.ML` → ~500k articles, taille manageable.
- **Format** : JSON lines (un article par ligne).
- **Champs principaux** : `id`, `title`, `abstract`, `authors`, `categories`, `update_date`, `doi`.
- **Pas de rate limiting** : fichier statique, téléchargement unique.

### Hacker News

- **Source** : API Firebase publique — `https://hacker-news.firebaseio.com/v0/`.
- **Endpoints utiles** : `/newstories.json` (500 derniers IDs), `/topstories.json` (500 top IDs), `/item/{id}.json` (détail d'un item).
- **Rate limiting** : très permissif (pas de clé API, ~30 req/s recommandé).
- **Format** : JSON.
- **Champs principaux** : `id`, `type`, `by`, `time`, `title`, `url`, `score`, `descendants`, `kids`, `text`.
- **Volume** : ~300-500 nouveaux items/heure (stories + commentaires).

---

## Prérequis pour les apprenants

- Bases de Python (structures de données, scripts, pip, virtual environments).
- Notions de SQL (SELECT, JOIN, GROUP BY).
- Connaissance minimale de Git (clone, commit, push, branches).
- Un poste avec Docker Desktop installé et au moins 16 Go de RAM.
- Connexion internet (pour l'API Hacker News et le téléchargement du dump ArXiv).

---

## Ressources suggérées

- *Fundamentals of Data Engineering* — Joe Reis & Matt Housley (O'Reilly).
- *Elasticsearch: The Definitive Guide* — documentation officielle Elastic (elastic.co/guide).
- *Data Pipelines with Apache Airflow* — Bas Harenslak & Julian de Ruiter (Manning).
- Documentation Great Expectations : https://docs.greatexpectations.io
- DataOps Manifesto : https://dataopsmanifesto.org
- API Hacker News : https://github.com/HackerNews/API
- Dataset ArXiv Kaggle : https://www.kaggle.com/datasets/Cornell-University/arxiv
