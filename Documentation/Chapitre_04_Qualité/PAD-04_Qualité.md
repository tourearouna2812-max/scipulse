# Jour 2 — Après-midi
# Bloc théorique : Qualité des données et Great Expectations

> **Durée** : 45 minutes  
> **Positionnement** : transversal — la qualité intervient à chaque étape du cycle de vie  
> **Objectif** : comprendre pourquoi la qualité des données est un pilier DataOps, connaître les dimensions de la qualité, et maîtriser les concepts fondamentaux de Great Expectations

---

## 1. La qualité des données : un problème systémique

### 1.1. Le coût de la mauvaise qualité

En 2016, IBM estimait que les données de mauvaise qualité coûtaient à l'économie américaine 3 100 milliards de dollars par an [1]. Ce chiffre est difficile à vérifier, mais l'ordre de grandeur est cohérent avec ce que les praticiens observent au quotidien. Les symptômes sont omniprésents :

- Un dashboard qui affiche des chiffres aberrants un lundi matin, parce qu'un fichier source contenait des lignes dupliquées.
- Un modèle de machine learning dont la performance se dégrade silencieusement, parce que la distribution des données d'entraînement a changé sans que personne ne le détecte.
- Un rapport réglementaire qui doit être resoumis, parce qu'un champ de date était dans le mauvais format pour 2 % des enregistrements.
- Une migration de base de données qui « réussit » mais corrompt des milliers de relations, parce que les contraintes d'intégrité n'avaient pas été testées sur les données réelles.

Ces incidents partagent une caractéristique commune : ils ne sont détectés qu'**en aval**, souvent par un utilisateur final, longtemps après que le problème s'est produit en amont. Le temps entre l'introduction du problème et sa détection — le *time to detection* — est le facteur qui détermine le coût de la correction. Un problème détecté à l'ingestion coûte quelques minutes à corriger. Le même problème détecté dans un rapport mensuel coûte des jours, parce qu'il faut retracer le lignage, identifier la cause racine, corriger les données, et régénérer tous les artefacts en aval.

### 1.2. La qualité comme pilier DataOps

Le DataOps répond à ce problème par un principe emprunté au monde du logiciel : le **shift-left testing** — tester le plus tôt possible dans le pipeline, au plus près de la source.

En développement logiciel, le shift-left consiste à écrire des tests unitaires avant le code (TDD), à intégrer en continu (CI), et à déployer automatiquement (CD). Les bugs sont détectés en minutes, pas en semaines.

En ingénierie de données, le shift-left consiste à :

- **Valider les données à l'entrée du pipeline** (juste après l'ingestion) : le fichier est-il complet ? Les champs obligatoires sont-ils présents ? Les types sont-ils corrects ?
- **Valider les données après chaque transformation** : la jointure a-t-elle produit le nombre attendu de lignes ? Y a-t-il des valeurs aberrantes introduites par le calcul ?
- **Valider les données avant la mise à disposition** (avant le serving) : l'index Elasticsearch contient-il le bon nombre de documents ? Les agrégations retournent-elles des résultats cohérents ?

Chaque point de validation est un **checkpoint** — un barrage qui empêche les données corrompues de se propager en aval. C'est exactement ce que Great Expectations implémente.

### 1.3. Les données ne sont pas du code — pourquoi c'est plus dur

Tester du code est relativement simple : on connaît les entrées, on connaît les sorties attendues, et on peut écrire des assertions déterministes (`assert sum(1, 2) == 3`).

Tester des données est fondamentalement plus difficile, pour trois raisons que Reis et Housley analysent en détail dans *Fundamentals of Data Engineering* [2] :

**Les données changent constamment.** Un code source ne change que quand un développeur fait un commit. Les données changent à chaque ingestion — nouvelles lignes, nouvelles valeurs, nouvelles distributions. Un test qui passe aujourd'hui peut échouer demain, non pas parce que le pipeline est cassé, mais parce que les données du monde réel ont évolué.

**Les données n'ont pas de « spécification ».** Un code est écrit pour implémenter une spécification (une user story, une RFC, un contrat d'API). Les données arrivent telles que la source les produit — et la source ne documente pas toujours son schéma, ses formats, ses valeurs nulles, ou ses conventions de nommage.

**Les problèmes de données sont souvent statistiques.** Un bug logiciel est binaire : le code plante ou ne plante pas. Un problème de données peut être subtil : 0.3 % de valeurs nulles dans un champ qui n'en avait jamais eu. Est-ce un bug ? Un changement de comportement de la source ? Une évolution normale ? La réponse nécessite du contexte métier.

C'est pourquoi le monde de la data a développé des outils spécialisés — qui ne sont ni des tests unitaires classiques, ni des contrôles SQL ad hoc, mais un cadre structuré pour exprimer des **attentes** (expectations) sur les données et les vérifier automatiquement.

---

## 2. Les dimensions de la qualité des données

La littérature en data quality identifie plusieurs dimensions complémentaires. Connaître ces dimensions aide à structurer les tests de qualité — chaque expectation que nous écrirons dans Great Expectations vérifie une ou plusieurs de ces dimensions.

### 2.1. Complétude (completeness)

Les données sont-elles toutes là ? À deux niveaux :

- **Complétude de l'enregistrement** : tous les champs obligatoires sont-ils remplis ? Le champ `abstract` est-il toujours présent dans les articles ArXiv ?
- **Complétude du jeu de données** : a-t-on tous les enregistrements attendus ? Si le dump ArXiv contient habituellement 100 000 articles et qu'un jour il en contient 50 000, quelque chose a probablement mal tourné.

### 2.2. Validité (validity)

Les valeurs respectent-elles les contraintes attendues ?

- **Type** : un champ date contient-il bien une date ? Un score est-il bien un nombre ?
- **Format** : l'ID ArXiv respecte-t-il le pattern `YYMM.NNNNN` ?
- **Domaine** : la catégorie est-elle dans l'ensemble des catégories ArXiv connues ?
- **Plage** : la date de publication est-elle entre 1991 (création d'ArXiv) et aujourd'hui ?

### 2.3. Unicité (uniqueness)

Chaque enregistrement est-il unique ? Les doublons sont un problème classique en ingestion de données, surtout avec les pipelines qui relisent des fichiers (replay). Dans SciPulse, l'`arxiv_id` doit être unique dans l'index.

### 2.4. Cohérence (consistency)

Les données sont-elles cohérentes entre elles et entre les sources ?

- **Cohérence interne** : si un article a `primary_category: "cs.AI"`, est-ce que `"cs.AI"` apparaît bien dans le tableau `categories` ?
- **Cohérence inter-sources** : si un post HN pointe vers un article ArXiv, cet article existe-t-il bien dans notre index ArXiv ?

### 2.5. Fraîcheur (timeliness)

Les données sont-elles suffisamment récentes pour l'usage prévu ? Si le poller HN n'a pas tourné depuis 24 heures, les données ne reflètent plus l'actualité. La fraîcheur se mesure par le delta entre le timestamp de la donnée la plus récente et l'heure actuelle.

### 2.6. Exactitude (accuracy)

Les données reflètent-elles la réalité ? C'est la dimension la plus difficile à vérifier automatiquement — elle nécessite souvent une vérité terrain (ground truth) externe. Dans SciPulse, on peut vérifier partiellement l'exactitude en comparant un échantillon d'articles avec leur page ArXiv.

### 2.7. Récapitulatif pour SciPulse

| Dimension | Exemple ArXiv | Exemple HN | Comment tester |
|-----------|---------------|------------|----------------|
| Complétude | `abstract` jamais null | `title` jamais null pour les stories | `expect_column_values_to_not_be_null` |
| Validité | Date entre 1991 et aujourd'hui | Score ≥ 0 | `expect_column_values_to_be_between` |
| Unicité | `arxiv_id` unique | `hn_id` unique | `expect_column_values_to_be_unique` |
| Cohérence | `primary_category` ∈ `categories` | — | Script Python custom |
| Fraîcheur | Date max < 1 an | Timestamp max < 24h | `expect_column_max_to_be_between` |

---

## 3. Les approches de la validation de données

Avant de plonger dans Great Expectations, situons-le dans le paysage des outils et approches de validation.

### 3.1. Les contrôles SQL ad hoc

L'approche la plus basique : écrire des requêtes SQL manuelles pour vérifier les données après chaque chargement.

```sql
-- Vérifier les doublons
SELECT arxiv_id, COUNT(*) FROM arxiv_papers GROUP BY arxiv_id HAVING COUNT(*) > 1;

-- Vérifier les nulls
SELECT COUNT(*) FROM arxiv_papers WHERE abstract IS NULL;

-- Vérifier la plage de dates
SELECT MIN(date_published), MAX(date_published) FROM arxiv_papers;
```

C'est simple et efficace, mais ça ne passe pas à l'échelle : chaque contrôle est un script ad hoc, non réutilisable, non documenté, sans historique des exécutions, et sans rapport de synthèse. C'est l'équivalent data des `print("debug")` dans le code — ça dépanne mais ce n'est pas de l'ingénierie.

### 3.2. Les contraintes de schéma (dbt tests, JSON Schema)

Un cran au-dessus : définir des contraintes déclaratives sur le schéma.

**dbt tests** (que nous utiliserons au Jour 3) permettent de déclarer des contraintes dans un fichier YAML :

```yaml
models:
  - name: stg_arxiv_papers
    columns:
      - name: arxiv_id
        tests: [not_null, unique]
      - name: primary_category
        tests: [not_null]
```

C'est mieux : les tests sont versionnés, réutilisables, et exécutés automatiquement par `dbt test`. Mais ils se limitent à des contraintes simples (null, unique, valeurs acceptées) et sont liés à SQL et dbt.

### 3.3. Les frameworks de validation dédiés

Les frameworks spécialisés comme **Great Expectations**, **Soda**, **Deequ** (Amazon), ou **Pandera** offrent un cadre complet : une bibliothèque riche de validations prédéfinies, un mécanisme de profiling automatique, des rapports visuels, un historique des exécutions, et une intégration dans les pipelines d'orchestration.

Great Expectations est le plus mature et le plus adopté dans l'écosystème Python/DataOps. C'est celui que nous utiliserons dans SciPulse.

---

## 4. Great Expectations — concepts fondamentaux

### 4.1. Philosophie

Great Expectations (GX) repose sur une idée centrale : **les données doivent avoir des tests automatisés, tout comme le code** [3]. Le parallèle est explicite :

| Monde du code | Monde des données (GX) |
|---------------|----------------------|
| Test unitaire | Expectation |
| Suite de tests | Expectation Suite |
| Exécution de tests (pytest run) | Checkpoint |
| Rapport de couverture | Data Docs |
| Assertion (`assert x == 5`) | `expect_column_values_to_not_be_null("arxiv_id")` |

Le nom « Great Expectations » est un clin d'œil au roman de Dickens — l'idée est qu'on déclare ses *attentes* (expectations) sur les données, puis qu'on vérifie si la réalité s'y conforme.

### 4.2. Les quatre concepts clés

#### Concept 1 — Expectation

Une **expectation** est une assertion sur les données. C'est l'unité de base de la validation. GX en propose plus de 300 prédéfinies [4], organisées par catégorie :

**Sur les valeurs d'une colonne** :

| Expectation | Ce qu'elle vérifie |
|-------------|-------------------|
| `expect_column_values_to_not_be_null` | Aucune valeur nulle |
| `expect_column_values_to_be_unique` | Pas de doublons |
| `expect_column_values_to_be_between` | Valeurs dans un intervalle |
| `expect_column_values_to_be_in_set` | Valeurs dans un ensemble connu |
| `expect_column_values_to_match_regex` | Valeurs qui matchent un pattern |
| `expect_column_value_lengths_to_be_between` | Longueur de la valeur dans un intervalle |
| `expect_column_values_to_be_of_type` | Type de données correct |

**Sur les statistiques d'une colonne** :

| Expectation | Ce qu'elle vérifie |
|-------------|-------------------|
| `expect_column_mean_to_be_between` | Moyenne dans un intervalle |
| `expect_column_median_to_be_between` | Médiane dans un intervalle |
| `expect_column_min_to_be_between` | Minimum dans un intervalle |
| `expect_column_max_to_be_between` | Maximum dans un intervalle |
| `expect_column_stdev_to_be_between` | Écart-type dans un intervalle |

**Sur la table / le dataset** :

| Expectation | Ce qu'elle vérifie |
|-------------|-------------------|
| `expect_table_row_count_to_be_between` | Nombre de lignes dans un intervalle |
| `expect_table_column_count_to_equal` | Nombre exact de colonnes |
| `expect_table_columns_to_match_ordered_list` | Colonnes dans l'ordre attendu |

Chaque expectation retourne un résultat structuré : succès/échec, nombre de lignes validées, nombre de lignes en échec, et échantillon des valeurs problématiques.

**L'aspect le plus puissant des expectations** est qu'elles sont **déclaratives et paramétrables**. On ne dit pas « vérifie que ça marche » — on dit *exactement* ce qu'on attend, avec des seuils quantifiés. Par exemple :

```python
# Au moins 95% des abstracts doivent avoir plus de 50 caractères
validator.expect_column_value_lengths_to_be_between(
    "abstract",
    min_value=50,
    mostly=0.95  # Tolérance : 5% d'exceptions acceptées
)
```

Le paramètre `mostly` est fondamental : il reconnaît que les données du monde réel ne sont jamais parfaites à 100%. Exiger 100% de conformité mène à des faux positifs constants. Exiger 95% capture les vrais problèmes tout en tolérant le bruit naturel.

#### Concept 2 — Expectation Suite

Une **suite** est un ensemble d'expectations regroupées logiquement — l'équivalent d'un fichier de tests. On crée typiquement une suite par source de données ou par étape du pipeline :

- `arxiv_raw_suite` : validations sur les données ArXiv brutes (juste après l'ingestion).
- `arxiv_enriched_suite` : validations sur les données ArXiv enrichies (après transformation).
- `hn_raw_suite` : validations sur les données Hacker News.

Une suite est un fichier JSON qui contient la liste des expectations et leurs paramètres. Elle est versionnée dans Git comme le reste du code.

#### Concept 3 — Checkpoint

Un **checkpoint** orchestre l'exécution d'une suite sur un jeu de données réel. C'est le `pytest run` de la data :

1. Il se connecte à la source de données (fichier CSV, DataFrame Pandas, table SQL, …).
2. Il exécute chaque expectation de la suite.
3. Il produit un rapport de résultats.
4. Il peut déclencher des actions en cas d'échec (alerte, arrêt du pipeline, …).

Dans Airflow, un checkpoint est une tâche du DAG. Si le checkpoint échoue, le pipeline s'arrête — les données corrompues ne se propagent pas en aval. C'est le shift-left en action.

#### Concept 4 — Data Docs

Les **Data Docs** sont des rapports HTML auto-générés qui documentent :

- Les expectations définies (« quels tests existe-t-il ? »).
- Les résultats des dernières exécutions (« les tests passent-ils ? »).
- L'historique des validations (« depuis quand ce test échoue-t-il ? »).

C'est un pilier de la **collaboration** DataOps : n'importe qui dans l'équipe peut ouvrir le rapport HTML et comprendre l'état de santé des données, sans lire le code Python.

### 4.3. Le workflow typique

Le workflow Great Expectations en quatre étapes :

```
┌─────────────────────────────────────────────────────────────────────┐
│  1. CONNECT                                                         │
│     Connecter GX à la source de données                            │
│     (fichier, DataFrame, base SQL, ...)                            │
└───────────────┬─────────────────────────────────────────────────────┘
                │
                ▼
┌─────────────────────────────────────────────────────────────────────┐
│  2. EXPECT                                                          │
│     Définir les expectations (déclaratives)                        │
│     → créer une Expectation Suite                                  │
│     Approche manuelle : écrire les expectations une par une        │
│     Approche assistée : profiler les données (Data Assistant)      │
└───────────────┬─────────────────────────────────────────────────────┘
                │
                ▼
┌─────────────────────────────────────────────────────────────────────┐
│  3. VALIDATE                                                        │
│     Exécuter le Checkpoint                                         │
│     → chaque expectation est vérifiée sur les données réelles      │
│     → résultat : success / failure pour chaque expectation         │
└───────────────┬─────────────────────────────────────────────────────┘
                │
                ▼
┌─────────────────────────────────────────────────────────────────────┐
│  4. ACT                                                             │
│     Consulter les Data Docs (rapport HTML)                         │
│     Intégrer dans le pipeline (Airflow : stop si échec)            │
│     Alerter si nécessaire                                          │
└─────────────────────────────────────────────────────────────────────┘
```

### 4.4. Sources de données supportées

GX se connecte à pratiquement toutes les sources courantes :

| Source | Connecteur GX | Usage SciPulse |
|--------|---------------|----------------|
| Fichier CSV / JSON | `PandasDatasource` | Validation du dump ArXiv brut |
| DataFrame Pandas | `RuntimeDataConnector` | Validation après transformation Python |
| DataFrame Polars | Support natif (GX ≥ 0.18) | Alternative à Pandas |
| PostgreSQL | `SqlAlchemyDatasource` | Validation des tables dbt |
| Spark DataFrame | `SparkDFDatasource` | Hors scope TP |

Pour Elasticsearch, il n'y a pas de connecteur natif GX. La stratégie est d'extraire un échantillon via `elasticsearch-py` dans un DataFrame Pandas, puis de valider le DataFrame. C'est l'approche que nous utiliserons dans le TP.

---

## 5. Stratégies de validation pour des données hétérogènes

### 5.1. Données brutes vs. données transformées

Les validations ne sont pas les mêmes selon l'étape du pipeline :

**À l'entrée (données brutes)** — on vérifie la *forme* :

- Le fichier existe-t-il ? Est-il non-vide ?
- Les champs attendus sont-ils présents ?
- Les types sont-ils cohérents ?
- Le nombre de lignes est-il dans l'intervalle attendu ?
- Les identifiants sont-ils uniques ?

Ce sont des contrôles défensifs : on ne fait pas confiance à la source. Si le fichier change de format ou perd des colonnes, on veut le savoir immédiatement.

**Après transformation (données enrichies)** — on vérifie le *fond* :

- Les valeurs calculées sont-elles dans des plages réalistes ?
- Les distributions sont-elles cohérentes avec les observations historiques ?
- Les jointures ont-elles produit le bon nombre de lignes ?
- Les enrichissements (NLP, croisement HN) ont-ils fonctionné ?

Ce sont des contrôles sémantiques : on vérifie que le pipeline a fait son travail correctement.

### 5.2. Le paramètre `mostly` — tolérer l'imperfection

Le monde réel est imparfait. Un dump ArXiv de 100 000 articles contiendra inévitablement quelques anomalies : un abstract vide, un champ date mal formaté, un doublon introduit par la source.

Exiger 100 % de conformité sur chaque expectation mène à des faux positifs permanents — le checkpoint échoue à chaque exécution, l'équipe s'habitue à ignorer les échecs, et les vrais problèmes passent inaperçus. C'est le phénomène d'*alert fatigue*, bien documenté en SRE (Site Reliability Engineering) [5].

La solution est le paramètre `mostly` : un seuil de tolérance entre 0 et 1.

```python
# Échec uniquement si plus de 1% des abstracts sont vides
validator.expect_column_values_to_not_be_null("abstract", mostly=0.99)

# Échec uniquement si plus de 5% des dates sont hors plage
validator.expect_column_values_to_be_between(
    "date_published",
    min_value="1991-01-01",
    max_value="2025-12-31",
    mostly=0.95
)
```

**Comment choisir la valeur de `mostly`** :

- **0.99 – 1.0** : champs critiques (identifiants, clés de jointure). Un seul manquant peut casser le pipeline.
- **0.95 – 0.99** : champs importants mais tolérables (dates, catégories). Quelques anomalies sont acceptables.
- **0.80 – 0.95** : champs optionnels ou hétérogènes (affiliations des auteurs, DOI). On sait que la source est incomplète.
- **< 0.80** : rarement utile. Si plus de 20 % des valeurs sont invalides, c'est probablement un problème structurel qu'il faut traiter autrement.

### 5.3. Le profiling comme point de départ

Pour un jeu de données inconnu, écrire des expectations à la main est fastidieux. GX propose un **Data Assistant** qui profile automatiquement les données et suggère des expectations :

```python
result = context.assistants.onboarding.run(batch_request=batch_request)
suite = result.get_expectation_suite()
```

Le profiler analyse chaque colonne (type, distribution, valeurs distinctes, nulls…) et génère un premier jeu d'expectations. C'est un point de départ — pas un résultat final. Les expectations auto-générées sont souvent trop strictes (calibrées sur les données actuelles, sans marge) ou trop laxistes (le profiler ne connaît pas les règles métier).

La bonne pratique est de profiler, puis d'**éditer manuellement** les expectations : ajuster les seuils, supprimer les expectations non pertinentes, ajouter les règles métier que le profiler ne peut pas deviner.

---

## 6. Intégration dans le pipeline DataOps

### 6.1. GX dans Airflow

Dans un pipeline orchestré par Airflow, les checkpoints GX sont des tâches comme les autres. Le pattern typique :

```
download → validate_raw → ingest → transform → validate_enriched → serve
```

Si `validate_raw` échoue, `ingest` ne s'exécute pas. Si `validate_enriched` échoue, `serve` ne s'exécute pas. Les données corrompues ne se propagent jamais.

Dans notre DAG `scipulse_arxiv_pipeline`, c'est exactement cette structure :

```python
download_arxiv >> validate_raw >> ingest_logstash >> transform_dbt >> enrich_python >> validate_enriched
```

Chaque tâche `validate_*` exécute un checkpoint GX. En cas d'échec, Airflow marque la tâche comme `failed`, arrête le DAG, et peut envoyer une alerte.

### 6.2. Data contracts

Le concept de **data contract** prolonge naturellement les expectations. Un data contract est un accord formalisé entre le producteur de données et le consommateur : « je m'engage à fournir des données qui respectent ces expectations, et tu t'engages à ne pas consommer des données qui ne les respectent pas » [6].

Dans SciPulse, les expectations servent de data contract implicite :

- Le producteur (le dump ArXiv, l'API HN) doit fournir des données qui passent la suite `raw`.
- Le pipeline (Logstash, Python, dbt) doit produire des données qui passent la suite `enriched`.
- Le consommateur (Kibana, le search_service) peut supposer que les données dans `arxiv-papers` respectent le contrat.

### 6.3. Les Data Docs comme documentation vivante

Un avantage souvent sous-estimé de GX est que les Data Docs servent de **documentation automatique** des données. Au lieu d'un document Word décrivant le schéma attendu (qui sera obsolète en deux semaines), les Data Docs montrent :

- Quels tests existent sur chaque colonne.
- Les résultats de la dernière exécution (avec pourcentages de conformité).
- Les valeurs attendues (plages, ensembles, patterns).

C'est de la documentation vivante — elle se met à jour à chaque exécution du checkpoint. C'est un outil de collaboration puissant : un data analyst qui découvre un jeu de données peut ouvrir les Data Docs et comprendre immédiatement les contraintes de qualité, sans lire le code du pipeline.

---

## 7. Limites et pièges courants

### 7.1. Le sur-testing

Écrire trop d'expectations est aussi problématique que d'en écrire trop peu. Chaque expectation a un coût :

- Temps d'exécution (surtout sur des tables volumineuses).
- Maintenance (les seuils doivent être mis à jour quand les données évoluent).
- Bruit (plus il y a de tests, plus il y a de faux positifs potentiels).

La règle pragmatique : tester ce qui **casserait le pipeline en aval** si c'était faux. Un abstract de 49 caractères au lieu de 50 ne cassera probablement rien. Un `arxiv_id` null cassera toute la chaîne d'indexation.

### 7.2. La dérive des seuils

Les données évoluent. Un seuil `mostly=0.99` calibré sur les données de janvier peut devenir trop strict en mars si la source a changé de format. Il faut réviser les seuils régulièrement — les Data Docs facilitent ce suivi en montrant les tendances.

### 7.3. La validation n'est pas la correction

GX détecte les problèmes, il ne les corrige pas. Si un checkpoint échoue, il faut un processus humain ou automatisé pour :

1. Diagnostiquer la cause (changement de source ? bug dans une transformation ?).
2. Corriger (adapter le pipeline ou nettoyer les données).
3. Relancer.

Le pipeline doit prévoir un chemin de correction — pas seulement un arrêt en cas d'échec.

---

## 8. Ce qui vient dans le TP

Le TP met en pratique ces concepts sur les données SciPulse :

1. **Installation de Great Expectations** et configuration du contexte.
2. **Rédaction d'une suite pour ArXiv** : validations de complétude, validité, unicité sur les données brutes extraites depuis Elasticsearch.
3. **Rédaction d'une suite pour HN** : validations adaptées à la structure des items HN.
4. **Exécution des checkpoints** et consultation des Data Docs.
5. **Script de nettoyage** (Pandas) guidé par les résultats des validations : déduplication, normalisation des auteurs, extraction de catégories.
6. **Re-validation** après nettoyage pour vérifier l'amélioration.

---

## Bibliographie

### Ouvrages de référence

[1] Redman, T.C. (2016). « Bad Data Costs the U.S. $3 Trillion Per Year ». *Harvard Business Review*. https://hbr.org/2016/09/bad-data-costs-the-u-s-3-trillion-per-year  
L'article qui a popularisé le chiffre des 3 100 milliards de dollars. Redman, considéré comme le « Data Doc », argumente que le coût de la mauvaise qualité est systémiquement sous-estimé parce que la plupart des entreprises ne le mesurent pas. L'article reste la référence la plus citée pour motiver un investissement dans la qualité des données.

[2] Reis, J., & Housley, M. (2022). *Fundamentals of Data Engineering*. O'Reilly Media.  
Le chapitre 8 (Data Quality and Observability) est la référence la plus complète et la plus récente sur la qualité des données dans le contexte du data engineering moderne. Il couvre les dimensions de la qualité, les stratégies de validation, le concept de data observability, et le positionnement des outils (GX, Soda, Monte Carlo). Le parallèle avec l'observabilité des systèmes distribués (pillar: logs, metrics, traces → data: freshness, volume, distribution) est particulièrement éclairant.

[3] Great Expectations. (2024). *Great Expectations Documentation*. https://docs.greatexpectations.io  
La documentation officielle de GX est exceptionnellement bien écrite. La section « Getting Started » fournit un tutorial complet. La « Gallery of Expectations » catalogue les 300+ expectations avec des exemples. La section « How to Guides » couvre l'intégration avec Airflow, Spark, et les principales bases de données. C'est la référence pour le TP.

[4] Great Expectations. (2024). *Expectations Gallery*. https://greatexpectations.io/expectations  
Le catalogue complet de toutes les expectations disponibles, organisées par type (column, table, multi-column). Chaque expectation a une documentation détaillée avec paramètres, exemples, et cas d'usage. Indispensable pendant la rédaction des suites.

[5] Beyer, B., Jones, C., Petoff, J., & Murphy, N.R. (2016). *Site Reliability Engineering: How Google Runs Production Systems*. O'Reilly Media.  
Le chapitre 29 (« Dealing with Interrupts ») et le chapitre 31 (« Communication and Collaboration in SRE ») traitent en profondeur de l'alert fatigue et de ses conséquences. Bien que le contexte soit l'opération de systèmes logiciels, les leçons sont directement transposables au monitoring de pipelines de données : trop d'alertes mènent à ignorer toutes les alertes, y compris les critiques.

[6] Dehghani, Z. (2022). *Data Mesh: Delivering Data-Driven Value at Scale*. O'Reilly Media.  
Le chapitre 11 (« Data Product Interfaces ») formalise le concept de data contract comme interface entre producteurs et consommateurs de données. Dehghani propose que chaque « data product » expose un contrat explicite (schéma, SLOs de qualité, SLOs de fraîcheur) — les expectations GX en sont une implémentation concrète.

### Articles et ressources complémentaires

[7] Moses, B., Gavish, L., & Vorwerck, M. (2022). *Data Quality Fundamentals*. O'Reilly Media.  
Ouvrage entièrement dédié à la qualité des données, par les fondateurs de Monte Carlo (un outil de data observability). Couvre les dimensions de la qualité, les pipelines de détection d'anomalies, les stratégies de résolution, et les métriques de qualité. Complémentaire à Great Expectations : Monte Carlo détecte les anomalies statistiques automatiquement, GX vérifie des règles explicites.

[8] DataOps Manifesto. https://dataopsmanifesto.org  
Le principe 14 du manifeste — « Quality is paramount » — et le principe 15 — « Monitor quality and performance » — posent la qualité comme obligation, pas comme option. Les 18 principes constituent le cadre philosophique qui sous-tend les pratiques vues en cours.

[9] Breck, E. et al. (2017). « The ML Test Score: A Rubric for ML Production Readiness and Technical Debt Reduction ». *Proceedings of IEEE Big Data*. https://research.google/pubs/pub46555/  
Article de Google qui propose un score de maturité pour les systèmes de ML en production. La section sur les tests de données (Data Tests) formalise les catégories de validations (feature expectations, training-serving skew, data invariants) de manière directement utilisable. Bien que centré sur le ML, le cadre s'applique à tout pipeline de données.

[10] Great Expectations. (2024). *How to integrate Great Expectations with Airflow*. https://docs.greatexpectations.io/docs/deployment_patterns/how_to_use_great_expectations_with_airflow  
Guide officiel d'intégration GX + Airflow. Montre comment encapsuler un checkpoint GX dans un `PythonOperator` ou un `GreatExpectationsOperator` Airflow, avec gestion des échecs et alerting.

[11] Redman, T.C. (2001). *Data Quality: The Field Guide*. Digital Press.  
L'ouvrage fondateur de la discipline de la qualité des données. Redman y pose les bases conceptuelles (dimensions, métriques, processus d'amélioration) qui restent le socle de tous les frameworks actuels. Académique mais accessible, il fournit le vocabulaire que vous retrouverez dans la documentation de tous les outils de data quality.

[12] Ehrlinger, L., & Wöß, W. (2022). « A Survey of Data Quality Measurement and Monitoring Tools ». *Frontiers in Big Data*, 5, 850611.  
Revue de littérature récente comparant les outils de data quality (Great Expectations, Deequ, Soda, Pandera, TDDA). L'article propose une taxonomie des fonctionnalités (profiling, validation, monitoring, remediation) et évalue chaque outil selon ces critères. Utile pour situer GX dans le paysage et comprendre ses forces et limites relatives.
