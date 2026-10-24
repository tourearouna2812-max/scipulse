# SciPulse Insights — Pipeline Spark (Module 2)

**Auteur :** Toure Arouna · **Stack :** Spark 3.5.0 (PySpark), MinIO, Elasticsearch, Kibana, Airflow, Great Expectations 1.x

Ce document répond au livrable n°3 du brief (pipeline Spark : source de
citations, stratégie de jointure, indicateurs, score d'impact) et au
livrable n°4 (procédure de validation). Tous les chiffres sont ceux
mesurés sur le corpus du projet : 100 000 articles ArXiv (sous-ensemble
`arxiv-full-subset.json`, publications 2007–2013).

---

## 1. Architecture Medallion

| Couche | Emplacement | Contenu | Produit par |
|---|---|---|---|
| 🥉 Bronze | `arxiv-raw/` | dump ArXiv brut (JSON Lines) | DAG batch du module 1 |
| 🥉 Bronze | `hn-raw/algolia/`, `hn-raw/es_export/` | stories HN brutes | `fetch_hn_arxiv.py`, `export_hn_from_es.py` |
| 🥉 Bronze | `citations-raw/openalex/` | œuvres OpenAlex brutes | `fetch_openalex.py` |
| 🥈 Silver | `arxiv-clean/arxiv.parquet` | articles nettoyés, clés normalisées | `build_silver.py --source arxiv` |
| 🥈 Silver | `hn-clean/hn_arxiv.parquet` | stories HN liées à un article ArXiv | `build_silver.py --source hn` |
| 🥈 Silver | `citations-clean/openalex.parquet` | citations nettoyées + validées (GX) | `build_silver.py --source citations` |
| (préparation) | `results/*.parquet` | table enrichie, vélocité, TF-IDF, score | jobs Spark |
| 🥇 Gold | index ES `arxiv-papers-enriched` | une ligne par article, score d'impact | `write_to_elasticsearch.py` |

Kibana n'interroge que la couche Gold. Toutes les transformations sont
écrites avec des fonctions natives `pyspark.sql.functions` ; deux UDF
seulement, justifiées : similarité cosinus et extraction des mots-clés
d'un vecteur TF-IDF (pas d'équivalent natif).

Configuration centralisée dans `src/spark_jobs/config.py` (chemins,
hôtes, année de référence), surchargeable par variables d'environnement.

---

## 2. Choix de la source de citations : OpenAlex

| Critère | OpenAlex | Semantic Scholar |
|---|---|---|
| Licence des données | CC0 (domaine public) | licence propre, usage restreint |
| Accès | REST, clé gratuite (1 $/jour ≈ 10 000 appels list) | REST, clé optionnelle, quotas plus bas sans clé |
| Requête par lot | filtre OR jusqu'à 100 DOIs par appel | endpoint batch (500 IDs) |
| `counts_by_year` | ✅ natif → vélocité de citation directe | ❌ à reconstruire depuis les citations |
| Thématique | `primary_topic` (topic → subfield → field) | `fieldsOfStudy` plus grossier |
| Lien avec ArXiv | DOI DataCite `10.48550/arxiv.<id>` + URL arxiv.org dans `locations` | `externalIds.ArXiv` |

**Décision : OpenAlex**, pour `counts_by_year` (indispensable aux window
functions de vélocité), la hiérarchie `primary_topic` (agrégations par
sous-domaine dans Kibana) et la licence CC0.

**Stratégie d'ingestion** (amélioration par rapport au TP) : au lieu d'un
extrait aléatoire de 2 000 œuvres « Computer Science » (qui ne recoupe
presque pas nos articles), `fetch_openalex.py --mode arxiv-ids` interroge
OpenAlex **par DOI ArXiv** à partir de la liste d'IDs produite par la couche
Silver ArXiv, par lots de 50 DOIs par appel.

Volume ingéré : **98 504 œuvres** pour **100 000 IDs** demandés, en
**2 000 appels** et 1 176 s (≈ 20 min), soit **98 437 œuvres uniques**
après dédoublonnage Silver. Une première ingestion limitée à 20 000 IDs
donnait 19,7 % de couverture ; l'ingestion complète la porte à 98,5 %.

---

## 3. Couche Silver et qualité des données

### Nettoyage appliqué

| Source | Règles |
|---|---|
| ArXiv | id en minuscules, espaces compactés, `primary_category` = 1re catégorie, DOI normalisé (1er DOI si plusieurs), `doi_arxiv` construit, date de 1re soumission extraite de `versions[0].created`, `is_withdrawn` (règle du module 1 : résumé < 40 caractères ou « This paper has been withdrawn »), dédoublonnage sur `arxiv_id` |
| HN | fusion Algolia + export ES, `arxiv_id` extrait de l'URL (abs/pdf/html, ancien et nouveau format, version retirée), score et commentaires négatifs ou nuls → 0, auteur manquant → `[deleted]`, dédoublonnage sur `hn_id` (meilleur score gardé) |
| Citations | `openalex_id` sans préfixe URL, DOI normalisé, `arxiv_id` déduit du DOI DataCite ou d'une URL arxiv.org, `cited_by_count` négatif → 0, dédoublonnage sur `openalex_id` |

### Suite Great Expectations des citations (7 expectations, brief : ≥ 3)

| Colonne | Expectation | Pourquoi |
|---|---|---|
| openalex_id | not_be_null, be_unique, match_regex `^W\d+$` | identité de l'œuvre, clé du dédoublonnage |
| cited_by_count | not_be_null, be_between 0 et 1 000 000 | composante principale du score |
| doi_clean | match_regex `^10\.\d{4,9}/\S+$` (mostly 0.99) | vérifie que la normalisation a bien retiré `https://doi.org/` |
| oa_publication_year | be_between 1990 et année + 1 (mostly 0.99) | cohérence temporelle (score de récence) |

Résultat : **7/7 expectations validées (100 %)** sur 98 437 œuvres ;
seule `oa_publication_year` compte 0,01 % de valeurs hors bornes, dans la
tolérance `mostly = 0.99` (capture des Data Docs en annexe).

Dans le DAG Airflow, ces 7 règles sont rejouées en Spark par
`check_citations_quality.py` (*quality gate*) : GX 1.x exige Python ≥ 3.9
alors que l'image Spark 3.5.0 embarque Python 3.8. Même résultat : 7/7.

---

## 4. Jointures multi-sources

### Stratégie de matching ArXiv ⨝ OpenAlex

Quatre méthodes, de la plus fiable à la moins fiable. Un article candidat à
plusieurs méthodes garde la plus fiable (`row_number()` sur la priorité),
ce qui garantit **une seule ligne par article**.

| Priorité | Méthode | Clé ArXiv | Clé OpenAlex |
|---|---|---|---|
| 1 | `arxiv_id` | identifiant ArXiv | ID déduit du DOI DataCite / URL arxiv.org |
| 2 | `doi` | DOI éditeur du dump | DOI OpenAlex |
| 3 | `doi_arxiv` | `10.48550/arxiv.<id>` | DOI OpenAlex |
| 4 | `title_fallback` | titre normalisé ≥ 20 caractères | titre normalisé |

La jointure finale est un **LEFT join** : un article sans citation reste
dans le Gold (indispensable au cas d'usage « viral sur HN mais peu cité »).

HN : les stories sont agrégées par `arxiv_id` (score max, somme des
commentaires, nombre de publications) avant la jointure, pour ne pas
dupliquer un article posté plusieurs fois.

### Taux de couverture (`results/match_coverage.parquet`)

| Métrique | Articles | % du corpus |
|---|---|---|
| arxiv_id | 98 186 | 98,19 % |
| doi | 144 | 0,14 % |
| doi_arxiv | 0 | 0 % |
| title_fallback | 171 | 0,17 % |
| **total apparié à OpenAlex** | **98 501** | **98,50 %** |
| none (aucune citation) | 1 499 | 1,50 % |
| signal HN | 518 | 0,52 % |
| citations ET HN | 507 | 0,51 % |

**Lecture (question de synthèse J2) :** le DOI éditeur seul est absent
pour une large part des articles ArXiv (préprints jamais publiés en
revue) : seuls 144 articles ont été appariés par leur DOI éditeur. Le taux de
couverture global est donc limité d'abord par l'**extrait OpenAlex**
(nombre d'IDs interrogés), ensuite par la complétude des DOIs. C'est
pourquoi l'ingestion par DOI ArXiv (méthode 1) a été privilégiée : elle
ne dépend pas du DOI éditeur.

### Broadcast join vs sort-merge join (`diagnose_slow_join.py`)

| | Sort-merge join | Broadcast join |
|---|---|---|
| Opérateur observé | `SortMergeJoin` | `BroadcastHashJoin` |
| `Exchange` (shuffle) | 2 (les deux tables) | 1 `BroadcastExchange` (petite table seulement) |
| Temps mesuré (3 835 lignes jointes) | 9,71 s | **1,93 s (× 5,0)** |

**Choix :** toutes les jointures broadcastent la petite table (OpenAlex,
HN agrégé, meilleurs matchs). La grande table ArXiv n'est jamais
redistribuée. Le broadcast reste sûr tant que la table broadcastée tient
en mémoire de chaque executor (quelques dizaines de Mo ici) ; au-delà de
plusieurs centaines de Mo, il deviendrait risqué (OOM driver/executors)
et le sort-merge redeviendrait le bon choix.

Le broadcast explicite sur la table des meilleurs matchs est
nécessaire : issue d'une window function, sa taille est inconnue de
Spark, qui choisirait sinon un sort-merge join.

---

## 5. Indicateurs analytiques (window functions)

| Indicateur | Calcul | Fichier |
|---|---|---|
| Vélocité annuelle | `explode(counts_by_year)`, `lag` sur l'année précédente | `citation_velocity.parquet` |
| Moyenne mobile 3 ans | `avg` sur `rowsBetween(-2, 0)` | idem |
| `citations_3ans` | moyenne annuelle sur [REF_YEAR−2 ; REF_YEAR], années absentes = 0 | idem (utilisé par le score) |
| Croissance des sujets | citations par `topic_name` et année, `lag`, `rank` | `topic_growth.parquet` |
| Croissance des catégories | publications par `primary_category` et année, `lag`, `rank` (catégories ≥ 50 publications) | `category_growth.parquet` |

`REF_YEAR` = dernière année civile complète : l'année en cours est
partielle et sous-estimerait la vélocité de tous les articles.

### TF-IDF distribué (Spark MLlib)

`RegexTokenizer → StopWordsRemover (+ stop words scientifiques) → CountVectorizer (20 000 mots, minDF = 5) → IDF`.

CountVectorizer est préféré à HashingTF car il conserve le vocabulaire :
on en tire les **5 mots-clés** les plus pondérés de chaque article, exposés
dans Kibana (champ `keywords`). Contrepartie : un passage de plus sur le
corpus pour construire le vocabulaire.

| Corpus | Abstracts | fit | transform |
|---|---|---|---|
| complet (100 %) | 99 850 | 52,5 s | 17,4 s |

Vocabulaire retenu : 20 000 mots. Exemple : *quantum*, *network(s)*,
*codes*, *channel*, *graph* dominent le nuage de mots Kibana. Limite :
pas de lemmatisation (« network » et « networks » sont comptés à part).

`fit()` est séparé de `transform()` car l'IDF a besoin de la fréquence
documentaire de chaque mot **sur tout le corpus** avant de pondérer le
moindre document.

---

## 6. Score d'impact composite

```
score_citations = log(1 + cited_by_count) / max(log(1 + cited_by_count))
score_velocite  = log(1 + citations_3ans) / max(log(1 + citations_3ans))
score_hn        = log(1 + hn_score)       / max(log(1 + hn_score))
score_recence   = 2 ^ (− âge_en_années / 3)

score_impact = 100 × (0,40 · citations + 0,25 · vélocité + 0,20 · HN + 0,15 · récence)
```

**Justification**

- **Échelle logarithmique :** citations et points HN sont à queue lourde.
  Sans log, quelques articles écrasent tous les autres : c'était le défaut
  de la formule du TP (`0,5 × cited_by_count` brut, qui rend la
  composante HN négligeable).
- **Normalisation par le maximum :** les quatre composantes sont sur
  [0, 1], donc les poids se lisent directement comme des parts
  d'importance et le score est sur 0–100.
- **Récence (exigée par le brief) :** un article de 2015 cumule
  mécaniquement plus de citations qu'un article de 2024. Une demi-vie de
  3 ans correspond à l'horizon de la vélocité.
- **Pondérations :** l'impact académique (citations + vélocité = 65 %)
  reste majoritaire, c'est le signal le plus fiable et le plus
  difficile à manipuler ; HN (20 %) apporte la dimension « écho tech »
  sans pouvoir à lui seul propulser un article ; la récence (15 %)
  départage à impact égal.

**Analyse de sensibilité** (recouvrement du top 100 avec la configuration par défaut) :

| Configuration | Citations | Vélocité | HN | Récence | Recouvrement top 100 | N° 1 observé |
|---|---|---|---|---|---|---|
| Par défaut | 0,40 | 0,25 | 0,20 | 0,15 | 100 % | Scikit-learn: Machine Learning in Python (65,49) |
| HN dominant | 0,20 | 0,10 | 0,60 | 0,10 | 19 % | There are no particles, there are only fields |
| Citations seules | 1,00 | 0 | 0 | 0 | 85 % | — |
| Sans récence | 0,50 | 0,30 | 0,20 | 0 | 96 % | — |

Top 5 (configuration par défaut) : Scikit-learn (65,49) · word2vec,
*Efficient Estimation of Word Representations* (62,55) · SMOTE (60,81) ·
R-CNN, *Rich feature hierarchies* (60,77) · *Distributed Representations
of Words and Phrases* (57,08).

**Lecture :** le classement est robuste aux variations raisonnables
(85 % et 96 % de recouvrement) ; il ne bascule que si HN devient
majoritaire (19 %), ce qui confirme que la pondération HN à 20 % apporte
un signal sans dominer. La récence pèse peu car tout le corpus date de
2007–2013 (âges proches).

**Limite et piste de calibration (question de synthèse J3) :** les poids
restent un choix d'expert. Pour les calibrer, il faudrait une vérité
terrain externe, par exemple : prédire les citations à N+2 à partir des
composantes mesurées à N (régression → poids appris), ou comparer le
classement à une liste de référence (prix « best paper », articles
sélectionnés par des revues de synthèse) avec une corrélation de rang
(Spearman).

---

## 7. Couche Gold et orchestration

- Mapping explicite (`src/utils/mapping_arxiv_papers_enriched.json`),
  `"dynamic": "strict"` : un champ non prévu fait échouer l'écriture au
  lieu de polluer l'index. Catégories, sujets et mots-clés en `keyword`
  (agrégations, tri), titre et résumé en `text` (recherche plein texte),
  titre aussi en `title.raw` (keyword) pour les tableaux Kibana.
- Écriture `elasticsearch-hadoop` en `append` avec `es.mapping.id = arxiv_id` :
  idempotente (relancer met à jour, ne duplique pas), et le mapping
  n'est pas détruit (ce que ferait `overwrite`).
- Contrôle : nombre de documents Gold = **100 000**, égal au
  nombre d'`arxiv_id` distincts de la table enrichie. Un écart viendrait
  de doublons d'`arxiv_id` : même `_id` → le dernier écrit écrase les autres.
- DAG `scipulse_insights_spark` (hebdomadaire) :
  `ingest_citations → silver_citations`, `ingest_hn_arxiv → silver_hn`, puis
  `quality_gate_citations → create_gold_index → spark_enrich_and_score → check_gold`.
- Paramètre `ingest` (défaut `False`) : les tâches d'ingestion sont
  *skipped* et le Bronze existant est réutilisé, pour ne pas reconsommer
  le quota OpenAlex ; les tâches Silver ont `trigger_rule = none_failed`.
- Le *quality gate* est placé **avant** la Gold : une règle en échec
  arrête le DAG et l'index Elasticsearch n'est pas modifié.
- Exécution mesurée : **13 min 56 s**, toutes les tâches en succès ;
  `check_gold` : « arxiv-papers-enriched : 100,000 documents, tous scorés ».

**Mise en œuvre retenue :** l'image `apache/airflow` ne contient pas le
binaire `docker`. Les tâches sont des `PythonOperator` qui appellent le
SDK Python `docker` (installé via `_PIP_ADDITIONAL_REQUIREMENTS`) à travers
le socket Docker monté dans les conteneurs Airflow : `exec_create`, logs
diffusés en direct dans Airflow, échec si le code retour est non nul.

**Limites de cette approche « docker exec » (question de synthèse J4) :**
- l'UI Airflow ne voit qu'une commande shell : pas de suivi natif de
  l'application Spark, ni de lien vers son UI ;
- un code retour non nul est la seule information d'échec, sans
  distinction des causes ;
- la tâche dépend du socket Docker monté dans Airflow, ce qui est un
  risque de sécurité (accès root à l'hôte) et n'existe pas sur
  Kubernetes ou un cluster managé ;
- le `--packages` et la mémoire sont codés dans une chaîne de caractères.

Alternative : `SparkSubmitOperator` (provider `apache-spark`) avec une
connexion Spark configurée dans Airflow, ou `KubernetesPodOperator` en production.

---

## 8. Dashboard Kibana augmenté

Data view : `arxiv-papers-enriched`, champ temporel `published_date`.

| # | Visualisation | Configuration | Question à laquelle elle répond |
|---|---|---|---|
| 1 | Barres verticales | X : `primary_category` (top 15) · Y : moyenne de `score_impact` | Quels domaines ont le plus d'impact ? |
| 2 | Tableau « viraux sur HN mais peu cités » | filtre KQL `cited_by_count < 5 and hn_score > 100` · lignes `title.raw` · médianes `hn_score`, `cited_by_count` | Quels articles font du bruit sans reconnaissance académique ? |
| 3 | Tableau « très cités mais absents de HN » | filtre KQL `cited_by_count > 50 and has_hn : false` · top 10 `title.raw` · max `cited_by_count` desc | Quelle recherche influente échappe au grand public tech ? |
| 4 | Treemap | `subfield_name` (top 15) · moyenne `score_velocite` | Quels sous-domaines accélèrent ? |
| 5 | Nuage de mots (Tag cloud) | `keywords` (top 50) · nombre d'articles | De quoi parle le corpus ? |
| 6 | Courbe | X : `published_date` (année) · Y : moyenne `score_impact`, ventilée par `match_method` | Biais d'âge et effet de la méthode de matching |
| 7 | 3 métriques | nb d'articles · `has_hn : true` · `not match_method : "none"` | Couverture des sources, en un coup d'œil |

**Résultats observés**

- **Viraux mais peu cités** : 3 articles — *Harmony Explained* (213 pts HN,
  3 citations), *Coding Guidelines for Prolog* (126, 0), *Classic Nintendo
  Games are (Computationally) Hard* (115, 1). Valeurs identiques à la sortie
  Spark : la Gold est cohérente avec le pipeline.
- **Très cités, absents de HN** : Scikit-learn (69 575), R-CNN (32 625),
  SMOTE (32 505), *Distributed Representations of Words…* (24 512),
  *Fast unfolding of communities* (21 231)…
- **Treemap** : *Health Informatics*, *Gender Studies*, *Agronomy* en tête.
  Limite : moyenne sur peu d'articles, donc instable ; filtrer les
  sous-domaines ≥ 50 articles serait plus robuste.
- **Courbe** : `doi` ≈ 13 > `arxiv_id` ≈ 9–10 > `title_fallback` (irrégulier,
  171 articles) > `none` ≈ 0 : sans jointure, pas de citation, donc pas de score.
- **Métriques** : 100 000 articles · 518 discutés sur HN · 98 501 appariés.

Captures d'écran : `docs/captures/kibana_*.png`.

---

## 9. Procédure de validation sur jeu de test

1. **Tests unitaires** (`tests/test_transformations.py`, 10 tests, jeu de
   données construit à la main, résultat attendu connu) :
   normalisation DOI / titre, extraction d'ID ArXiv depuis une URL,
   nettoyage Silver des trois sources (doublons, valeurs négatives,
   auteurs manquants, articles retirés), **priorité des méthodes de
   matching et absence de doublon**, calcul de la vélocité, **bornes
   [0 ; 100] et monotonie du score**.
   ```
   docker compose exec spark python3 -m pytest /opt/spark-apps/tests -v
   ```
   Résultat : **10 passed** (10/10).
2. **Qualité Silver** : suites Great Expectations ArXiv, HN (module 1) et citations.
3. **Cohérence Gold** : `_count` ES = nombre d'`arxiv_id` distincts ; tâche
   `check_gold` du DAG (index non vide, tous les documents scorés).
4. **Requêtes analytiques cibles** : chaque visualisation de la section 8
   répond à une question explicite ; vérifier qu'elle renvoie des
   résultats non vides et plausibles (ex. un article très connu présent
   dans ton corpus doit apparaître parmi les plus cités).
5. **Généralisation** : relancer le pipeline sur un échantillon différent
   (`--limit`, `TFIDF_SAMPLE`) et comparer la stabilité du top 100
   (analyse de sensibilité) et des taux de couverture.

---

## 10. Synthèse pour la restitution

| Jour | Question | Ma réponse |
|---|---|---|
| J1 | Point de bascule Pandas / Spark | Pandas 9,13 s vs Spark 46,10 s sur 100 000 articles (Pandas × 5) ; Spark 180,25 s sur 3 002 164 articles (5,2 Go), soit 60 µs/article contre 91 µs pour Pandas. Volume × 30 → temps × 3,9. Le point de bascule est atteint quand les données ne tiennent plus en mémoire d'une machine (quelques Go) |
| J2 | Couverture du matching DOI | voir section 4 : limitée par l'extrait OpenAlex puis par les DOIs manquants ; matching par ID ArXiv privilégié |
| J3 | Légitimité des pondérations | voir section 6 : choix d'expert, normalisé, testé par sensibilité ; calibration possible par prédiction des citations futures |
| J4 | Limites de l'intégration Airflow | voir section 7 : BashOperator + docker exec, alternative SparkSubmitOperator |

## 11. Difficultés rencontrées et solutions

| Problème | Solution |
|---|---|
| Image `bitnami/spark` retirée de Docker Hub | image officielle `spark:3.5.0-python3` + Dockerfile |
| PySpark local sur Windows (Java, winutils) | jobs lancés dans le conteneur via `scripts/spark.ps1` |
| Collision de la colonne `id` (ArXiv et OpenAlex) dans le code du TP | clés renommées `arxiv_id` / `openalex_id` |
| Champ `arxiv_id` absent de l'index `hn-items` | extraction de l'ID ArXiv depuis l'URL en Silver |
| Extrait OpenAlex aléatoire → quasi aucun match | ingestion ciblée par DOI ArXiv |
| `input()` interactif dans le script de mapping (bloque Airflow) | option `--recreate` |
| `numpy==1.26.4` introuvable (image en Python 3.8) | versions compatibles 3.8 (numpy 1.24.4, pandas 2.0.3) + `from __future__ import annotations` |
| Chemin du dump ArXiv différent du TP | variable `ARXIV_RAW_PATH` dans `.env` |
| Couverture OpenAlex de 19,7 % avec 20 000 IDs | ingestion des 100 000 IDs → 98,5 % |
| Pas de binaire `docker` dans l'image Airflow | SDK Python `docker` + socket Docker monté |
| GX 1.x incompatible avec Python 3.8 (image Spark) | *quality gate* Spark miroir de la suite GX dans le DAG |
| Clé API exposée par erreur | clé régénérée, `.env` dans `.gitignore` |
