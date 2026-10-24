# Jour 1 — Matin
# Fondements : DataOps, environnement et projet SciPulse

## 1. Pourquoi DataOps ?

### Le constat : la « dette de données »

La plupart des projets data échouent non pas à cause des algorithmes, mais à cause de **l'ingénierie autour des données**. Les symptômes sont récurrents :

- Un pipeline qui fonctionne en local mais casse en production.
- Des données corrompues qui ne sont détectées que des semaines plus tard.
- Un data scientist qui attend 3 jours qu'un ingénieur lui prépare un dataset.
- Personne ne sait quelle version du code a produit le dashboard de lundi dernier.
- Un hotfix en production fait par copier-coller, sans tests, sans review.

Ces problèmes ont un nom : c'est la **dette de données**, l'équivalent data de la dette technique logicielle. Et tout comme le monde du développement a répondu à sa dette technique avec DevOps, le monde de la donnée a besoin de **DataOps**.

### Définition

> **DataOps** est l'application des principes Agile et DevOps au cycle de vie des données.  
> L'objectif est de **réduire le temps de mise en valeur des données** tout en **augmentant la qualité et la fiabilité** des pipelines.

Le terme a été formalisé en 2018 par le *DataOps Manifesto* (dataopsmanifesto.org), qui s'inspire directement du Manifeste Agile et des principes DevOps.

### Ce que DataOps n'est PAS

Clarifions quelques confusions fréquentes :

- **DataOps ≠ DevOps pour la data.** C'est plus que ça : DevOps se concentre sur le déploiement de logiciels. DataOps ajoute la dimension spécifique des données — qualité, lignage, schéma, volumétrie, fraîcheur.
- **DataOps ≠ un outil.** Aucun outil ne « fait du DataOps ». C'est un ensemble de pratiques, de principes et de culture. Les outils les outillent.
- **DataOps ≠ MLOps.** MLOps s'occupe du cycle de vie des modèles de machine learning. DataOps s'occupe du cycle de vie des données en amont. Les deux sont complémentaires.

## 2. Les trois piliers du DataOps

Le DataOps repose sur trois piliers fondamentaux. Chaque outil et chaque pratique que nous verrons dans ce cours s'inscrit dans l'un de ces piliers.

### Pilier 1 — Automatisation

**Principe** : tout ce qui peut être automatisé doit l'être. Un pipeline exécuté manuellement est un pipeline qui tombera en panne un vendredi soir.

**Pratiques concrètes** :

- **Infrastructure as Code (IaC)** : l'environnement entier est décrit dans des fichiers (Docker Compose, Terraform, Ansible). Un nouveau développeur doit pouvoir lancer la stack complète en une commande.
- **Pipeline as Code** : les pipelines de données sont définis en code (Airflow DAGs, configs Logstash), versionnés dans Git, revus en pull request.
- **CI/CD pour la data** : chaque commit déclenche des tests automatiques sur les transformations, les schémas, les requêtes.
- **Orchestration** : les tâches sont planifiées, séquencées, avec gestion des dépendances, des retries et des alertes (Airflow, Prefect, Dagster).

**Dans notre projet** : Docker Compose (IaC), Airflow (orchestration), Git (versioning), Logstash configs (pipeline as code).

### Pilier 2 — Observabilité

**Principe** : on ne peut pas améliorer ce qu'on ne mesure pas. Un pipeline de données est une boîte noire tant qu'on ne l'instrumente pas.

**Pratiques concrètes** :

- **Logging structuré** : chaque étape du pipeline produit des logs exploitables (pas juste des `print()`).
- **Métriques** : nombre de documents ingérés, durée d'exécution, taux d'erreur, fraîcheur des données.
- **Alerting** : notification automatique quand un seuil est dépassé (volume anormalement bas, taux d'erreur en hausse).
- **Lignage (lineage)** : savoir d'où vient chaque donnée, quelles transformations elle a subies, et quand.
- **Data quality monitoring** : vérification continue que les données respectent les contrats de qualité.

**Dans notre projet** : Kibana dashboards (monitoring), Filebeat (collecte de logs), Great Expectations (qualité), Elasticsearch index `pipeline-logs` (métriques).

### Pilier 3 — Collaboration

**Principe** : les silos entre équipes (data engineers, data scientists, analystes, ops) sont l'ennemi de la vélocité.

**Pratiques concrètes** :

- **Git comme source de vérité** : tout le monde travaille sur le même dépôt, avec des branches, des reviews, et un historique.
- **Documentation as code** : la documentation est générée automatiquement depuis le code (dbt docs, Great Expectations Data Docs, Swagger/OpenAPI).
- **Environnements reproductibles** : chaque développeur a le même environnement grâce à Docker. « Ça marche sur ma machine » n'est plus une excuse.
- **Self-service** : les analystes peuvent explorer les données sans attendre qu'un ingénieur leur prépare un export.

**Dans notre projet** : Git (versioning), dbt docs (documentation), Kibana (self-service exploration), Docker Compose (environnement reproductible).

---

## 3. Le cycle de vie de la donnée

Tout pipeline de données, qu'il traite 100 lignes ou 100 milliards, suit le même cycle de vie. Comprendre ce cycle est essentiel pour savoir **où intervient chaque outil** de notre stack.

### Les 6 étapes

```
  ┌──────────┐    ┌───────────┐    ┌──────────────┐
  │ INGESTION│───▶│ STOCKAGE  │───▶│TRANSFORMATION│
  └──────────┘    └───────────┘    └──────────────┘
                                          │
                                          ▼
  ┌───────────┐   ┌───────────┐    ┌──────────────┐
  │MONITORING │◀──│  SERVING  │◀───│ INDEXATION    │
  └───────────┘   └───────────┘    └──────────────┘
```

#### Étape 1 — Ingestion

**Quoi** : collecter les données depuis les sources et les amener dans le système.

**Patterns** :

| Pattern | Description | Latence | Exemple |
|---------|------------|---------|---------|
| Batch | Traitement par lots, à intervalle régulier | Minutes à heures | Dump ArXiv quotidien |
| Micro-batch | Petits lots très fréquents | Secondes à minutes | Polling API HN toutes les 60s |
| Streaming | Traitement événement par événement | Millisecondes | Kafka, Kinesis (hors scope TP) |

**Outils dans notre stack** :
- **Logstash** : ingestion batch du dump ArXiv (plugin `file`, codec `json`).
- **Python + requests** : polling micro-batch de l'API Hacker News.
- **Filebeat** : collecte de fichiers de logs (complément léger à Logstash).
- **MinIO** : zone de landing (staging area) pour les fichiers bruts avant traitement.

**Bonne pratique DataOps** : toujours conserver les données brutes (raw) dans une zone séparée. On ne transforme jamais en place — on crée une copie enrichie. C'est le principe d'**immutabilité des données sources**.

#### Étape 2 — Stockage

**Quoi** : persister les données de manière fiable et accessible.

**Patterns** :

| Couche | Usage | Technologie (notre stack) |
|--------|-------|--------------------------|
| Landing zone | Données brutes, telles que reçues | MinIO (S3-compatible) |
| Base opérationnelle | Stockage structuré pour requêtes | PostgreSQL |
| Moteur de recherche | Indexation full-text et analytique | Elasticsearch |

**Bonne pratique DataOps** : séparer les couches de stockage. Les données brutes ne sont jamais dans la même base que les données transformées. C'est le principe du **data lakehouse** : un lac de données (MinIO) + un entrepôt structuré (PostgreSQL) + un moteur de recherche (Elasticsearch).

#### Étape 3 — Transformation

**Quoi** : nettoyer, enrichir, structurer les données pour les rendre exploitables.

**Deux approches** :

| Approche | Description | Quand l'utiliser |
|----------|-------------|------------------|
| **ETL** (Extract-Transform-Load) | Transformer avant de charger | Données volumineuses, transformation lourde |
| **ELT** (Extract-Load-Transform) | Charger d'abord, transformer ensuite | Moteur de destination puissant (BigQuery, Snowflake, ES) |

**Outils dans notre stack** :
- **dbt** : transformations SQL versionnées dans PostgreSQL (ELT).
- **Python (Pandas/Polars)** : transformations programmatiques (nettoyage, NLP, enrichissement).
- **Logstash filters** : transformations à l'ingestion (parsing, renommage, conversion de types).

**Bonne pratique DataOps** : chaque transformation doit être **idempotente** — l'exécuter une fois ou dix fois produit le même résultat. C'est ce qui permet de relancer un pipeline en toute sécurité après un échec.

#### Étape 4 — Indexation

**Quoi** : organiser les données pour permettre des recherches rapides.

C'est le cœur d'**Elasticsearch** dans notre stack. L'indexation ne se résume pas à « mettre les données dans ES » — c'est un acte de **modélisation** :

- Choisir le bon type pour chaque champ (`text` vs `keyword`).
- Configurer les analyseurs (stemming, synonymes, stopwords).
- Définir les multi-fields (un champ searchable ET aggregable).
- Optimiser pour les requêtes attendues.

Nous détaillerons tout cela au Jour 2.

#### Étape 5 — Serving (restitution)

**Quoi** : mettre les données à disposition des utilisateurs finaux.

**Canaux de restitution** :

- **Dashboards** : Kibana pour l'exploration interactive.
- **API** : module Python `search_service.py` pour intégrer la recherche dans une application.
- **Rapports** : exports programmés (dbt docs, Data Docs).

#### Étape 6 — Monitoring (observabilité)

**Quoi** : surveiller en continu la santé du pipeline et la qualité des données.

- **Pipeline monitoring** : les tâches Airflow s'exécutent-elles ? Combien de temps prennent-elles ?
- **Data quality monitoring** : les données respectent-elles les contrats ? (Great Expectations)
- **Infrastructure monitoring** : Elasticsearch est-il en bonne santé ? Combien de documents par index ?

---

## 4. Panorama de la stack SciPulse

Chaque outil du cours a un rôle précis dans le cycle de vie. Voici comment ils s'articulent :

```
┌─────────────────────────────────────────────────────────────────────┐
│                            DataOps                                  │
│         Git (versioning) · Docker (reproductibilité)                │
│         Pytest (tests code) · Great Expectations (tests data)       │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  INGESTION        STOCKAGE       TRANSFORMATION    SERVING          │
│  ─────────        ────────       ──────────────    ───────          │
│  Logstash         MinIO          dbt (SQL)         Kibana           │
│  Python           PostgreSQL     Python/Pandas     Python API       │
│  Filebeat         Elasticsearch  Logstash filters                   │
│                                                                     │
│  ORCHESTRATION                   MONITORING                         │
│  ──────────────                  ──────────                         │
│  Airflow                         Kibana dashboards                  │
│                                  Filebeat → ES                      │
│                                  Watcher (alertes)                  │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 5. Présentation du projet SciPulse

### Le pitch

Nous allons construire **SciPulse**, une plateforme de veille intelligente qui croise deux sources de données :

- **ArXiv** : 2 millions d'articles scientifiques (titres, abstracts, auteurs, catégories). Source batch — un dump JSON massif.
- **Hacker News** : le flux temps réel des posts et commentaires de la communauté tech. Source streaming — polling de l'API Firebase.

Le projet est conçu pour couvrir **toutes les étapes du cycle de vie** et **tous les piliers DataOps** :

| Pilier DataOps | Manifestation dans SciPulse |
|----------------|----------------------------|
| Automatisation | Docker Compose lance tout en une commande. Airflow orchestre le pipeline. Git versionne tout. |
| Observabilité | Kibana dashboard de monitoring. Great Expectations valide la qualité. Logs centralisés. |
| Collaboration | Dépôt Git partagé. dbt docs auto-générées. Kibana en self-service pour l'exploration. |

### Les sources de données

#### ArXiv (batch)

Le dataset ArXiv Kaggle contient les métadonnées de plus de 2 millions de publications scientifiques. Voici un exemple d'enregistrement :

```json
{
  "id": "2301.07041",
  "title": "A Comprehensive Survey of AI-Generated Content (AIGC)",
  "abstract": "Recently, AI-generated content (AIGC) has received increasing attention...",
  "authors": "Yihan Cao, Siyu Li, Yixin Liu, Zhiling Yan, ...",
  "categories": "cs.AI cs.CL cs.LG",
  "update_date": "2023-02-07",
  "doi": "10.1007/s10462-023-10539-y"
}
```

Points d'intérêt :

- Les **abstracts** sont des blocs de texte de 100 à 500 mots — parfaits pour la fouille de texte Elasticsearch.
- Les **catégories** sont hiérarchiques : `cs.AI` = Computer Science > Artificial Intelligence. Un article peut avoir plusieurs catégories.
- Les **auteurs** sont une chaîne de texte brute qu'il faudra parser et normaliser.
- La **date** permet des analyses temporelles sur 30 ans de publications.

#### Hacker News (streaming)

L'API Hacker News est publique, sans authentification, et très permissive. Elle expose le flux temps réel des posts et commentaires :

```json
{
  "id": 38792160,
  "type": "story",
  "by": "dhouston",
  "time": 1702396800,
  "title": "Show HN: A new way to explore ArXiv papers",
  "url": "https://arxiv.org/abs/2312.12345",
  "score": 245,
  "descendants": 67,
  "kids": [38792200, 38792350, ...]
}
```

Points d'intérêt :

- Le champ `url` peut contenir un lien vers un article ArXiv → **enrichissement croisé**.
- La relation `kids` (commentaires d'un post) permet d'explorer les **relations parent-child** dans Elasticsearch.
- Le `score` et `descendants` sont des métriques de popularité → **function_score** pour pondérer la pertinence.
- Le `type` peut être `story`, `comment`, `job`, `poll` → facettes et agrégations.

### Architecture cible

```
ArXiv (batch, dump Kaggle)          Hacker News (micro-batch, API Firebase)
        │                                       │
        ▼                                       ▼
   MinIO (landing)                     Python hn_poller.py
   bucket: arxiv-raw/                           │
        │                                       │
        ▼                                       │
   Logstash (parse JSON)                        │
        │                                       │
        ▼                                       ▼
┌─────────────────── Elasticsearch ───────────────────┐
│                                                     │
│  arxiv-papers-raw    (données brutes Logstash)      │
│  arxiv-papers        (mapping optimisé full-text)   │
│  hn-items            (posts + comments, join field) │
│  arxiv-hn-links      (table de liaison)             │
│  pipeline-logs       (observabilité)                │
│                                                     │
└──────────┬──────────────────────────────────────────┘
           │
     ┌─────┴──────┐
     ▼            ▼
  Kibana      PostgreSQL + dbt
  (dashboard)  (transformations SQL)
```

### Planning du projet sur 4 jours

| Jour | Ce qu'on construit |
|------|--------------------|
| J1 | Environnement Docker + ingestion batch ArXiv (Logstash) |
| J2 | Mapping ES optimisé + ingestion HN + tests de qualité |
| J3 | Transformations dbt + enrichissement NLP + fouille de texte ES |
| J4 | Orchestration Airflow + dashboard Kibana + monitoring |
| J5 | Présentations orales |
