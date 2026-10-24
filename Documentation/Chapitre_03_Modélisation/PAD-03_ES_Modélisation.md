# Jour 2 — Matin
# Bloc théorique : Modélisation dans Elasticsearch

> **Durée** : 45 minutes  
> **Positionnement** : étapes stockage et indexation du cycle de vie  
> **Objectif** : comprendre le modèle de données d'Elasticsearch, concevoir un mapping optimisé pour la fouille de texte, et choisir le bon type de relation entre documents

---

## 1. Elasticsearch comme moteur de recherche et d'analytique

### 1.1. Ce qu'est Elasticsearch (et ce qu'il n'est pas)

Elasticsearch est un moteur de recherche et d'analytique distribué, construit au-dessus de la bibliothèque Apache Lucene. Contrairement à une base de données relationnelle, Elasticsearch est conçu pour répondre à deux types de questions :

- **Recherche** : « quels documents correspondent le mieux à cette requête ? » → pertinence, scoring, ranking.
- **Analytique** : « combien de documents par catégorie ? quelle est la moyenne de ce champ ? » → agrégations, statistiques.

Il excelle dans ces deux domaines grâce à une structure de données spécifique : l'**index inversé** (inverted index), hérité de Lucene.

**Ce qu'Elasticsearch n'est pas** :

- Ce n'est pas une base relationnelle : pas de JOIN au sens SQL, pas de transactions ACID strictes, pas de schéma rigide.
- Ce n'est pas un système de stockage primaire : il est conçu pour la recherche, pas pour être la source de vérité. En architecture DataOps, les données brutes vivent dans MinIO ou PostgreSQL ; Elasticsearch est une couche de *serving*.
- Ce n'est pas un data lake : il s'attend à des données structurées (ou semi-structurées avec un mapping).

### 1.2. L'index inversé — le cœur du moteur

Pour comprendre les choix de modélisation dans Elasticsearch, il faut comprendre la structure de données sous-jacente : l'**index inversé**.

Prenons trois documents :

| Doc ID | Texte |
|--------|-------|
| 1 | "deep learning for image recognition" |
| 2 | "reinforcement learning in robotics" |
| 3 | "deep reinforcement learning" |

L'index inversé construit une table de correspondance **terme → liste de documents** :

| Terme | Documents |
|-------|-----------|
| deep | 1, 3 |
| learning | 1, 2, 3 |
| image | 1 |
| recognition | 1 |
| reinforcement | 2, 3 |
| robotics | 2 |

Quand un utilisateur cherche `"deep learning"`, Elasticsearch consulte l'index inversé, trouve les documents qui contiennent les deux termes (intersection : document 1 et 3), et calcule un score de pertinence pour les classer.

Cette structure explique pourquoi :

- La recherche textuelle est extrêmement rapide (lookup dans une table, pas scan séquentiel).
- Le **processus d'analyse** (comment le texte est découpé en termes) est crucial — si on ne tokénise pas correctement, les termes ne matcheront pas.
- Le type de champ (`text` vs `keyword`) détermine si un index inversé est construit ou non.

Lucene, et par extension Elasticsearch, stocke en réalité plusieurs structures complémentaires pour chaque champ : l'index inversé pour la recherche, les *doc values* pour le tri et les agrégations, et les *stored fields* pour le `_source` [1][2]. Comprendre cette dualité éclaire les choix de mapping que nous allons faire.

---

## 2. Les concepts fondamentaux

### 2.1. Index

Un **index** Elasticsearch est une collection de documents qui partagent une structure similaire. C'est l'équivalent approximatif d'une table en SQL, mais avec plus de souplesse.

Chaque index a :

- Un **nom** (par convention en minuscules, avec des tirets : `arxiv-papers`, `hn-items`).
- Un **mapping** (la définition de la structure des documents).
- Des **settings** (nombre de shards, réplicas, analyseurs custom).

**Dans SciPulse**, nous aurons plusieurs index :

| Index | Contenu | Nb docs estimé |
|-------|---------|----------------|
| `arxiv-papers` | Articles scientifiques (mapping optimisé) | ~100 000 |
| `hn-items` | Posts et commentaires HN | ~10 000+ (croissant) |
| `arxiv-hn-links` | Liens croisés ArXiv ↔ HN | ~500+ |
| `pipeline-logs` | Logs du pipeline | Variable |

### 2.2. Document

Un **document** est l'unité de base d'Elasticsearch : un objet JSON indexé dans un index. Chaque document a :

- Un **`_id`** : identifiant unique au sein de l'index (généré automatiquement ou fourni, comme notre `arxiv_id`).
- Un **`_source`** : le JSON original tel qu'il a été indexé.
- Des **métadonnées** : `_index`, `_score` (lors d'une recherche), `_version`.

### 2.3. Mapping

Le **mapping** est la définition de la structure d'un index : quels champs existent, quel est leur type, et comment ils sont analysés. C'est l'équivalent du `CREATE TABLE` en SQL, mais plus riche parce qu'il inclut la configuration d'analyse textuelle.

Elasticsearch propose deux modes :

**Dynamic mapping** (automatique) : quand on indexe un document sans avoir défini de mapping, ES devine les types. C'est ce qui s'est passé hier avec `arxiv-papers-raw` :

- Il a vu `"2023-02-07"` → type `date`. Bien.
- Il a vu `"cs.AI cs.LG"` → type `text`. Problème : on ne peut pas agréger un champ `text` efficacement.
- Il a vu `"A survey of..."` → type `text`. Bien, mais avec l'analyseur `standard` par défaut (pas de stemming anglais, pas de synonymes).

**Explicit mapping** (déclaratif) : on définit le mapping avant d'indexer le premier document. C'est la bonne pratique en production, et c'est ce que nous allons faire aujourd'hui.

**Règle DataOps** : en production, toujours utiliser un mapping explicite. Le mapping automatique est utile pour l'exploration rapide, mais il fait des choix sous-optimaux. Et surtout, une fois qu'un champ a un type dans un index, **on ne peut plus le changer** — il faut recréer l'index et réindexer les données. D'où l'importance de bien concevoir le mapping dès le départ.

---

## 3. Les types de champs

Elasticsearch propose une vingtaine de types de champs. Nous allons nous concentrer sur les plus importants pour SciPulse.

### 3.1. `text` — recherche plein texte

Le type `text` est conçu pour la **recherche en langage naturel**. Le contenu est passé à travers un **analyseur** qui le découpe en termes, les normalise (minuscules, stemming…), et construit l'index inversé.

```json
"abstract": {
  "type": "text",
  "analyzer": "scientific_english"
}
```

**Quand l'utiliser** : titres, descriptions, abstracts, commentaires — tout champ sur lequel on veut faire des recherches `match`, `multi_match`, `more_like_this`.

**Ce qu'on ne peut PAS faire** avec un champ `text` : du tri exact, des agrégations `terms` (elles retourneraient des termes individuels, pas la valeur complète), du filtrage exact.

**Pourquoi** : le texte original n'est pas stocké tel quel dans l'index inversé — il est découpé en tokens. « A Comprehensive Survey of AI-Generated Content » devient les tokens `comprehensive`, `survey`, `ai`, `generated`, `content`. L'information « quelle était la valeur complète du champ » est perdue dans l'index inversé (elle reste dans le `_source`, mais n'est pas utilisable pour le tri et les agrégations).

### 3.2. `keyword` — valeurs exactes

Le type `keyword` stocke la valeur **telle quelle**, sans analyse. Il est optimisé pour le filtrage exact, le tri, et les agrégations.

```json
"primary_category": {
  "type": "keyword"
}
```

**Quand l'utiliser** : catégories, tags, identifiants, statuts, codes, URLs — tout champ dont on veut la valeur exacte pour filtrer (`term`), agréger (`terms`), ou trier.

**Ce qu'on ne peut PAS faire** : de la recherche en langage naturel. Si on cherche `"reinforcement"` dans un champ `keyword` qui contient `"reinforcement learning"`, ça ne matchera pas — le filtre `term` compare la valeur exacte complète.

### 3.3. La solution : les multi-fields

Le problème se pose immédiatement : pour un champ comme `title`, on veut **à la fois** faire de la recherche plein texte (« trouve les articles qui parlent de *transformer* ») **et** des agrégations exactes (« quels sont les 10 titres les plus fréquents »).

La solution d'Elasticsearch est le **multi-field** : un même champ est indexé sous plusieurs types simultanément.

```json
"title": {
  "type": "text",
  "analyzer": "scientific_english",
  "fields": {
    "keyword": {
      "type": "keyword",
      "ignore_above": 256
    }
  }
}
```

Avec cette configuration :

- `title` → type `text`, analysé, pour la recherche `match`.
- `title.keyword` → type `keyword`, valeur exacte, pour le tri et les agrégations `terms`.

C'est un pattern omniprésent dans Elasticsearch. La documentation officielle le recommande systématiquement pour les champs textuels qui ont des usages multiples [2].

**`ignore_above: 256`** : les valeurs de plus de 256 caractères ne sont pas indexées dans le sous-champ `keyword`. C'est une protection contre les titres excessivement longs qui gaspilleraient de l'espace dans l'index inversé keyword.

### 3.4. `date` — dates et timestamps

```json
"date_published": {
  "type": "date",
  "format": "yyyy-MM-dd||yyyy-MM-dd'T'HH:mm:ss||epoch_millis"
}
```

Le type `date` accepte plusieurs formats (séparés par `||`). Elasticsearch convertit en interne toutes les dates en millisecondes epoch pour le stockage et le calcul.

**Quand l'utiliser** : dates de publication, timestamps, dates de modification. Permet les requêtes `range`, les histogrammes temporels (`date_histogram`), le tri chronologique, et le `decay` dans `function_score` (boost des documents récents).

### 3.5. `integer`, `long`, `float`, `double` — numériques

```json
"score": { "type": "integer" },
"hn_score": { "type": "integer" }
```

Pour les métriques numériques. Permettent les requêtes `range`, les agrégations statistiques (`avg`, `sum`, `min`, `max`, `percentiles`), et le tri.

### 3.6. `nested` — objets avec requêtes indépendantes

Le type `nested` résout un problème subtil. Considérons un article avec plusieurs auteurs :

```json
{
  "authors": [
    { "name": "Alice Martin", "affiliation": "MIT" },
    { "name": "Bob Chen", "affiliation": "Stanford" }
  ]
}
```

Avec un type `object` standard (le défaut pour un JSON imbriqué), Elasticsearch « aplatit » le tableau :

```
authors.name:        ["Alice Martin", "Bob Chen"]
authors.affiliation: ["MIT", "Stanford"]
```

Le problème : si on cherche « auteur nommé *Alice* affilié à *Stanford* », ça matchera — alors qu'Alice est à MIT et Bob à Stanford. Les associations entre sous-champs sont perdues.

Le type `nested` maintient l'indépendance de chaque objet du tableau en les stockant comme des documents internes séparés (cachés, gérés par Lucene). Les requêtes `nested` permettent de chercher « un auteur dont le nom est Alice ET l'affiliation est MIT ».

```json
"authors": {
  "type": "nested",
  "properties": {
    "name": { "type": "text", "fields": { "keyword": { "type": "keyword" } } },
    "affiliation": { "type": "text", "fields": { "keyword": { "type": "keyword" } } }
  }
}
```

**Coût** : chaque objet nested est un document Lucene interne. Un article avec 20 auteurs génère 21 documents Lucene (1 parent + 20 nested). Cela augmente la taille de l'index et le coût des requêtes. À utiliser judicieusement — pas pour des tableaux de centaines d'éléments.

### 3.7. `join` — relations parent-enfant

Le type `join` établit une relation parent-enfant entre des documents du **même index**. Contrairement à `nested` (qui stocke les enfants à l'intérieur du parent), `join` stocke les parents et les enfants comme des documents séparés et indépendants.

```json
"relation": {
  "type": "join",
  "relations": {
    "story": "comment"
  }
}
```

Avec ce mapping, un document peut être un `story` (parent) ou un `comment` (enfant d'un story) :

```json
// Document parent (story)
{ "hn_id": "123", "title": "Show HN: ...", "relation": { "name": "story" } }

// Document enfant (comment)
{ "hn_id": "456", "text": "Great!", "relation": { "name": "comment", "parent": "123" } }
```

Les requêtes `has_child` et `has_parent` permettent de naviguer la relation :

- `has_child` : « trouve les stories qui ont au moins un commentaire contenant *breakthrough* ».
- `has_parent` : « trouve les commentaires dont la story parente a un score > 100 ».

**Quand utiliser `nested` vs `join`** :

| Critère | `nested` | `join` |
|---------|----------|--------|
| Stockage | Enfants à l'intérieur du parent | Documents séparés |
| Mise à jour des enfants | Réindexe le parent entier | Indépendante |
| Volumétrie enfants | Faible (dizaines) | Grande (milliers) |
| Performance requêtes | Rapide | Plus lente (routing) |
| Cas SciPulse | Auteurs d'un article ArXiv | Commentaires d'un post HN |

**Dans SciPulse** : les auteurs d'un article sont en `nested` (peu nombreux, mis à jour rarement, toujours affichés avec l'article). Les commentaires de HN sont en `join` (potentiellement nombreux, mis à jour indépendamment du post parent, requêtés séparément).

### 3.8. `completion` — autocomplétion

Le type `completion` est optimisé pour les suggestions en temps de frappe (typeahead) :

```json
"title": {
  "type": "text",
  "fields": {
    "suggest": {
      "type": "completion",
      "analyzer": "simple"
    }
  }
}
```

Il utilise une structure de données en mémoire (FST — Finite State Transducer) qui permet des lookups en temps constant. Utile pour le champ de recherche de SciPulse : l'utilisateur tape « trans… » et voit apparaître « transformer », « transfer learning », « translation ».

### 3.9. Récapitulatif des types pour SciPulse

| Champ | Type choisi | Justification |
|-------|-------------|---------------|
| `arxiv_id` | `keyword` | Identifiant exact, filtrage, pas de recherche textuelle |
| `title` | `text` + `keyword` + `completion` | Recherche + agrégation + autocomplétion |
| `abstract` | `text` (avec term_vector) | Recherche plein texte, more_like_this, highlighting |
| `authors` | `nested` (name + affiliation) | Préserver l'association nom↔affiliation |
| `categories` | `keyword` | Valeurs exactes multi-valuées, filtrage et agrégation |
| `primary_category` | `keyword` | Facette principale |
| `date_published` | `date` | Range, histogrammes, decay dans function_score |
| `doi` | `keyword` | Identifiant exact |
| `hn_score` | `integer` | Agrégations numériques, function_score |
| `relation` (HN) | `join` (story↔comment) | Relation parent-enfant entre posts et commentaires |

---

## 4. Les analyseurs — le cœur de la fouille de texte

### 4.1. Le processus d'analyse

Quand un texte est indexé dans un champ `text`, il passe par un **analyseur** qui le transforme en une séquence de termes (tokens). L'analyseur est composé de trois étapes exécutées en séquence :

```
   Texte brut
       │
       ▼
┌──────────────────┐
│  CHARACTER        │  Nettoyage du texte brut (suppression HTML, remplacement
│  FILTERS          │  de caractères spéciaux…)
└──────┬───────────┘
       │
       ▼
┌──────────────────┐
│  TOKENIZER        │  Découpage du texte en tokens individuels
│                   │  "neural networks" → ["neural", "networks"]
└──────┬───────────┘
       │
       ▼
┌──────────────────┐
│  TOKEN            │  Transformation de chaque token :
│  FILTERS          │  minuscules, stemming, synonymes, stopwords…
└──────┬───────────┘
       │
       ▼
   Tokens finaux  →  index inversé
```

**Le même analyseur est appliqué à l'indexation ET à la recherche.** Si l'abstract contient « neural networks » et que l'analyseur produit le stem `network`, alors la recherche « network » matchera — parce que le terme de recherche est aussi stemmé en `network` avant d'être cherché dans l'index inversé. Cette symétrie est fondamentale pour comprendre le comportement de la recherche.

### 4.2. L'analyseur `standard` (défaut)

C'est l'analyseur par défaut, appliqué à tout champ `text` sans configuration explicite.

- **Tokenizer** : `standard` — découpe sur les espaces et la ponctuation, gère Unicode.
- **Token filters** : `lowercase` — met en minuscules.

```
Entrée : "A Comprehensive Survey of AI-Generated Content (AIGC)"
Tokens : [a, comprehensive, survey, of, ai, generated, content, aigc]
```

C'est un bon point de départ, mais il a des limites pour la recherche scientifique :

- Pas de **stemming** : « networks » et « network » sont deux termes différents → une recherche « network » ne trouvera pas « networks ».
- Pas de **stopwords** : « of », « a », « the » sont indexés → ils occupent de l'espace et polluent les agrégations.
- Pas de **synonymes** : « CNN » et « convolutional neural network » sont des termes complètement distincts.

### 4.3. L'analyseur `english` (intégré)

Elasticsearch fournit des analyseurs prédéfinis pour une trentaine de langues, y compris l'anglais [2] :

```
Entrée : "A Comprehensive Survey of AI-Generated Content (AIGC)"
Tokens : [comprehens, survey, ai, generat, content, aigc]
```

Changements par rapport à `standard` :

- **Stopwords anglais** supprimés : « a », « of » ont disparu.
- **Stemming Porter** appliqué : « comprehensive » → `comprehens`, « generated » → `generat`.
- **Possessifs** supprimés : « John's » → `john`.

C'est mieux, mais le stemming de Porter est assez agressif (« comprehensive » → `comprehens` perd de la lisibilité) et il n'y a toujours pas de synonymes.

### 4.4. Construire un analyseur custom

Pour SciPulse, nous allons créer un analyseur sur mesure qui combine les bonnes propriétés des analyseurs existants avec des fonctionnalités spécifiques à notre domaine.

```json
{
  "settings": {
    "analysis": {
      "filter": {
        "english_stemmer": {
          "type": "stemmer",
          "language": "english"
        },
        "english_stop": {
          "type": "stop",
          "stopwords": "_english_"
        },
        "scientific_synonyms": {
          "type": "synonym",
          "synonyms": [
            "cnn, convolutional neural network",
            "rnn, recurrent neural network",
            "llm, large language model",
            "rl, reinforcement learning",
            "nlp, natural language processing",
            "bert, bidirectional encoder representations from transformers",
            "gpt, generative pre-trained transformer",
            "rlhf, reinforcement learning from human feedback"
          ]
        }
      },
      "analyzer": {
        "scientific_english": {
          "type": "custom",
          "tokenizer": "standard",
          "filter": [
            "lowercase",
            "english_stop",
            "scientific_synonyms",
            "english_stemmer"
          ]
        }
      }
    }
  }
}
```

Décomposons chaque composant :

**Tokenizer `standard`** : le découpeur par défaut. Il gère correctement Unicode, les acronymes, et la ponctuation. Pas besoin de le personnaliser.

**Token filter `lowercase`** : convertit en minuscules. « BERT » et « bert » deviennent le même token. Appliqué en premier pour que les filtres suivants travaillent sur du texte normalisé.

**Token filter `english_stop`** : supprime les mots vides anglais (« the », « a », « of », « is », « in »…). Réduit le bruit dans l'index inversé et améliore la pertinence des agrégations.

**Token filter `scientific_synonyms`** : le filtre qui fait la différence pour un corpus scientifique. Quand quelqu'un cherche « CNN », l'analyseur étend la recherche à « convolutional neural network » (et vice versa). L'ordre est important : les synonymes doivent être appliqués **avant** le stemming pour que les formes étendues soient aussi stemmées.

**Token filter `english_stemmer`** : réduit les mots à leur racine. « learning » → `learn`, « networks » → `network`, « generated » → `generat`. Le stemmer anglais d'Elasticsearch utilise l'algorithme de Porter, raffiné par Snowball [3].

**Ordre des filtres** : l'ordre dans le tableau `filter` est critique. Voici pourquoi :

```
lowercase → english_stop → scientific_synonyms → english_stemmer

1. "CNN for Image Recognition" 
2. → "cnn for image recognition"           (lowercase)
3. → "cnn image recognition"               (stop: "for" supprimé)
4. → "cnn convolutional neural network      (synonyms: CNN étendu)
      image recognition"
5. → "cnn convolut neural network           (stemmer)
      imag recognit"
```

Si on inversait stemmer et synonymes, le stemmer transformerait « CNN » en « cnn » avant que le filtre de synonymes ne puisse le reconnaître — et l'expansion ne se ferait pas.

### 4.5. Tester un analyseur

Elasticsearch expose une API `_analyze` qui permet de tester un analyseur sans indexer de document :

```
POST /arxiv-papers/_analyze
{
  "analyzer": "scientific_english",
  "text": "A new approach to CNN-based reinforcement learning for NLP"
}
```

Cette API retourne la liste des tokens produits. C'est indispensable pour déboguer un analyseur : si une recherche ne retourne pas les résultats attendus, c'est souvent parce que l'analyseur ne produit pas les tokens qu'on croit.

### 4.6. `term_vector` — pour `more_like_this` et le highlighting

Pour certaines fonctionnalités avancées, Elasticsearch a besoin d'informations supplémentaires sur les tokens au-delà de l'index inversé de base. C'est le rôle du **term vector** :

```json
"abstract": {
  "type": "text",
  "analyzer": "scientific_english",
  "term_vector": "with_positions_offsets"
}
```

- **`with_positions_offsets`** stocke, pour chaque token, sa position dans le texte et ses offsets (position en caractères dans la chaîne originale). Cela permet :
  - Un **highlighting** rapide et précis (surligner les termes trouvés dans l'abstract).
  - Des requêtes **`more_like_this`** performantes (trouver les documents similaires sans recalculer les tokens à la volée).

**Coût** : augmente la taille de l'index d'environ 30-50%. C'est un trade-off conscient pour SciPulse : les abstracts sont notre principal champ de recherche, et `more_like_this` est une fonctionnalité clé du projet.

---

## 5. Mapping explicite vs. dynamique — retour d'expérience

### 5.1. Les limites du mapping dynamique

Hier, nous avons indexé 100 000 articles avec le mapping auto-généré. Voici ce qu'Elasticsearch a deviné, et les problèmes que cela pose :

| Champ | Type deviné | Problème |
|-------|-------------|----------|
| `arxiv_id` | `text` + `keyword` | ✅ Correct (multi-field auto) |
| `title` | `text` + `keyword` | ⚠️ Analyseur standard, pas de stemming ni synonymes |
| `abstract` | `text` + `keyword` | ⚠️ Analyseur standard, pas de term_vector |
| `categories` | `text` + `keyword` | ⚠️ Analysé comme du texte libre — « cs.AI » tokenisé en `cs` + `ai` |
| `primary_category` | `text` + `keyword` | ⚠️ Même problème que categories |
| `date_updated` | `date` | ✅ Correct |
| `authors` | `text` + `keyword` | ⚠️ Chaîne plate, pas de nested |

Les conséquences concrètes :

- Une recherche `"CNN"` ne trouve pas les articles qui parlent de « convolutional neural network » (pas de synonymes).
- Une recherche `"network"` ne trouve pas « networks » (pas de stemming).
- Une agrégation sur `categories` retourne `cs`, `ai`, `lg` comme termes séparés au lieu de `cs.AI`, `cs.LG` (tokenisation du texte).
- Le highlighting de `more_like_this` est lent (pas de term_vector).

### 5.2. La règle d'or

> **En production, toujours définir un mapping explicite avant d'indexer le premier document.**

Le mapping dynamique est utile pour l'exploration rapide (« je jette des données dans ES pour voir à quoi elles ressemblent »). Mais dès qu'on veut une recherche de qualité, il faut prendre le contrôle.

Et rappelez-vous : **un mapping ne peut pas être modifié une fois les données indexées**. On peut *ajouter* des champs, mais pas *changer le type* d'un champ existant. Pour corriger un mapping, il faut créer un nouvel index avec le bon mapping, et réindexer les données avec l'API `_reindex`. C'est exactement ce que nous allons faire dans le TP.

---

## 6. Le scoring BM25 — comment Elasticsearch classe les résultats

### 6.1. Le problème du classement

Quand une requête `match` retourne 5 000 résultats, dans quel ordre les afficher ? C'est le problème du **ranking** (classement par pertinence). Elasticsearch utilise l'algorithme **BM25** (Best Matching 25), devenu le standard de fait en recherche d'information [4].

### 6.2. L'intuition derrière BM25

BM25 attribue un score à chaque document en fonction de trois facteurs :

**TF (Term Frequency)** : plus un terme apparaît fréquemment dans un document, plus ce document est pertinent pour ce terme. Mais avec une saturation : passer de 1 occurrence à 5 augmente beaucoup le score ; passer de 50 à 55, presque pas. C'est le paramètre `k1` qui contrôle cette saturation (par défaut 1.2).

**IDF (Inverse Document Frequency)** : plus un terme est rare dans le corpus, plus il est discriminant. Le mot « the » apparaît dans presque tous les documents → IDF faible, presque pas d'impact sur le score. Le mot « transformer » apparaît dans 2% des documents → IDF élevé, forte contribution au score.

**Field length normalization** : un terme qui apparaît dans un titre de 5 mots est plus significatif que le même terme dans un abstract de 300 mots. BM25 normalise le score par la longueur du champ, contrôlé par le paramètre `b` (par défaut 0.75).

La formule simplifiée (pour un seul terme `t` dans un document `D`) :

```
score(t, D) = IDF(t) × [ TF(t,D) × (k1 + 1) ] / [ TF(t,D) + k1 × (1 - b + b × |D| / avgDL) ]
```

Où `|D|` est la longueur du document et `avgDL` la longueur moyenne des documents dans l'index.

### 6.3. Implications pour SciPulse

- **Boosting du titre** : dans une requête `multi_match` sur `title^3` et `abstract`, le `^3` multiplie le score du titre par 3. Un match dans le titre est considéré 3× plus pertinent qu'un match dans l'abstract — ce qui est intuitif (le titre résume le contenu).

- **`function_score`** : BM25 donne un score de pertinence textuelle, mais on peut vouloir intégrer d'autres signaux. La requête `function_score` permet de combiner le score BM25 avec des fonctions arbitraires :
  - `decay` gaussien sur `date_published` : les articles récents sont boostés.
  - `field_value_factor` sur `hn_score` : les articles populaires sur HN sont boostés.

- **Analyseur custom** : le stemming et les synonymes impactent directement BM25. Si « CNN » est étendu en « convolutional neural network », le document qui contient les trois mots aura un TF plus élevé pour cette requête.

---

## 7. Settings et dimensionnement

### 7.1. Shards et réplicas

Un index Elasticsearch est divisé en **shards** (partitions) distribués sur les nœuds du cluster :

- **Primary shards** : le nombre de partitions de l'index. Fixé à la création, non modifiable ensuite. Chaque shard est un index Lucene indépendant.
- **Replica shards** : des copies des primary shards pour la haute disponibilité et la parallélisation des lectures.

```json
"settings": {
  "number_of_shards": 1,
  "number_of_replicas": 0
}
```

**Pour SciPulse** : un seul shard, zéro réplica. Avec 100 000 documents et un seul nœud, c'est la configuration optimale. La règle empirique d'Elastic est qu'un shard devrait contenir entre 10 Go et 50 Go de données [5]. Nos 100 000 articles font ~500 Mo — bien en dessous du seuil.

### 7.2. `refresh_interval`

Elasticsearch ne rend pas les documents cherchables immédiatement. Par défaut, il rafraîchit l'index toutes les secondes (`refresh_interval: 1s`). Pendant un chargement bulk massif, on peut désactiver le refresh pour accélérer l'ingestion, puis le réactiver :

```json
PUT /arxiv-papers/_settings
{ "refresh_interval": "-1" }

// ... ingestion bulk ...

PUT /arxiv-papers/_settings
{ "refresh_interval": "1s" }

POST /arxiv-papers/_refresh
```

---

## 8. Relations entre documents — synthèse

Pour clore ce bloc théorique, synthétisons les trois approches relationnelles d'Elasticsearch, car c'est un point de confusion fréquent lorsqu'on vient du monde relationnel.

### 8.1. Pourquoi pas de JOIN ?

En SQL, on ferait `SELECT * FROM articles JOIN authors ON ...`. Elasticsearch n'a pas de JOIN, et c'est un choix de design délibéré. Un JOIN distribué (entre des shards sur des machines différentes) est extrêmement coûteux en réseau et en latence. Elasticsearch privilégie la **dénormalisation** : dupliquer les données pour éviter les jointures.

### 8.2. Les quatre stratégies

| Stratégie | Principe | Performance | Cas SciPulse |
|-----------|----------|-------------|-------------|
| **Dénormalisation** | Dupliquer les données dans chaque document | Excellente | Champ `authors_flat` (texte) à côté de `authors` (nested) |
| **Nested** | Objets enfants stockés dans le parent | Très bonne | Auteurs d'un article ArXiv |
| **Join (parent-child)** | Documents séparés, liés par un champ join | Correcte | Posts et commentaires HN |
| **Application-side join** | Deux requêtes séparées, jointure en Python | Variable | Croisement ArXiv ↔ HN (via index `arxiv-hn-links`) |

La règle : commencer par la dénormalisation (la plus simple et la plus performante), et ne passer à `nested` ou `join` que si on a besoin de requêtes croisées sur les sous-objets. L'*application-side join* (deux requêtes + jointure en code) est souvent le meilleur compromis quand les entités vivent dans des index différents.

---

## 9. Ce qui vient dans le TP

Le TP de ce matin met en pratique ces concepts :

1. **Conception du mapping `arxiv-papers`** avec l'analyseur custom `scientific_english`, les multi-fields, le nested pour les auteurs, et le term_vector sur l'abstract.
2. **Création de l'index** via l'API REST.
3. **Réindexation** depuis `arxiv-papers-raw` vers `arxiv-papers`.
4. **Tests dans Kibana Dev Tools** : comparer le comportement de la recherche avant (mapping dynamique) et après (mapping explicite).
5. **Début du mapping `hn-items`** avec le join field pour la relation post/commentaire.

---

## Bibliographie

### Ouvrages de référence

[1] Gormley, C., & Tong, Z. (2015). *Elasticsearch: The Definitive Guide*. O'Reilly Media.  
Bien que partiellement daté (basé sur ES 2.x), cet ouvrage reste la meilleure introduction conceptuelle à Elasticsearch. Les chapitres sur l'index inversé (ch. 6), les analyseurs (ch. 7-8), et le scoring TF/IDF (ch. 12) sont des références incontournables pour comprendre le *pourquoi* derrière chaque choix de modélisation. Disponible gratuitement sur elastic.co/guide.

[2] Elastic. (2024). *Elasticsearch Reference — Mapping*. https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping.html  
La documentation officielle du mapping est la référence canonique pour les types de champs, les paramètres de mapping, et les analyseurs intégrés. Les sections « Field data types », « Mapping parameters » et « Text analysis » couvrent l'intégralité de ce bloc théorique. La page « Multi-fields » explique en détail le pattern `text` + `keyword`.

[3] Elastic. (2024). *Elasticsearch Reference — Text Analysis*. https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis.html  
Documentation exhaustive du système d'analyse textuelle : character filters, tokenizers, token filters. La section « Language analyzers » liste les 30+ analyseurs intégrés par langue. La section « Custom analyzers » guide la construction d'analyseurs sur mesure. La référence des token filters (stemmer, synonym, stop, etc.) est indispensable pour le TP.

[4] Robertson, S., & Zaragoza, H. (2009). « The Probabilistic Relevance Framework: BM25 and Beyond ». *Foundations and Trends in Information Retrieval*, 3(4), 333-389.  
L'article de référence sur BM25 par ses créateurs. Robertson et Zaragoza retracent l'évolution du modèle probabiliste de la recherche d'information depuis les travaux fondateurs des années 1970 jusqu'à BM25. L'article est accessible mathématiquement et éclaire les paramètres `k1` et `b` que nous avons évoqués. Indispensable pour qui veut comprendre *pourquoi* BM25 fonctionne, pas juste l'utiliser.

[5] Elastic. (2024). *Elasticsearch Reference — Size Your Shards*. https://www.elastic.co/guide/en/elasticsearch/reference/current/size-your-shards.html  
Guide officiel de dimensionnement des shards : règles empiriques (10-50 Go par shard), impact du nombre de shards sur la performance, stratégies de time-based indexing. Essentiel pour la mise en production, même si notre TP utilise un seul shard.

### Articles et ressources complémentaires

[6] Reis, J., & Housley, M. (2022). *Fundamentals of Data Engineering*. O'Reilly Media.  
Le chapitre 6 (Storage) discute les trade-offs entre les différents systèmes de stockage (OLTP, OLAP, search engines, document stores). La section sur les moteurs de recherche positionne Elasticsearch dans l'écosystème et clarifie son rôle de couche de serving.

[7] Kleppmann, M. (2017). *Designing Data-Intensive Applications*. O'Reilly Media.  
Le chapitre 3 (Storage and Retrieval) explique les index inversés, les B-trees, et les LSM-trees. La section sur Lucene et les full-text indexes fournit le fondement technique d'Elasticsearch. Le chapitre 2 (Data Models and Query Languages) discute les modèles documentaires vs. relationnels — directement pertinent pour comprendre pourquoi ES n'a pas de JOIN.

[8] Turnbull, D., & Berryman, J. (2016). *Relevant Search: With Applications for Solr and Elasticsearch*. Manning Publications.  
L'ouvrage de référence sur la pertinence de recherche (relevance engineering). Les chapitres sur le scoring (ch. 4-5), les analyseurs (ch. 6), et le tuning de la pertinence (ch. 7-8) sont directement applicables à notre projet. L'approche « test-driven relevancy » — définir des cas de test de recherche avant d'optimiser l'analyseur — est une pratique DataOps avant l'heure.

[9] Elastic. (2024). *Elasticsearch Reference — Nested Field Type*. https://www.elastic.co/guide/en/elasticsearch/reference/current/nested.html  
Documentation détaillée du type `nested` : fonctionnement interne (documents Lucene cachés), impact sur les performances, requêtes et agrégations nested. L'avertissement sur la limite `index.mapping.nested_fields.limit` (50 par défaut) est important pour les gros documents.

[10] Elastic. (2024). *Elasticsearch Reference — Join Field Type*. https://www.elastic.co/guide/en/elasticsearch/reference/current/parent-join.html  
Documentation du type `join` : contraintes de routing, requêtes `has_child`/`has_parent`, agrégations `children`. L'encadré « Using joins in Elasticsearch is often not needed » et la recommandation de privilégier la dénormalisation sont des mises en garde utiles.

[11] Manning, C.D., Raghavan, P., & Schütze, H. (2008). *Introduction to Information Retrieval*. Cambridge University Press.  
Le manuel de référence académique en recherche d'information. Les chapitres 2 (The term vocabulary and postings lists) et 6 (Scoring, term weighting, and the vector space model) fournissent les fondations théoriques de l'index inversé et du scoring TF-IDF/BM25. Disponible gratuitement en ligne : https://nlp.stanford.edu/IR-book/

[12] Elastic. (2024). *Elasticsearch Reference — Similarity Module*. https://www.elastic.co/guide/en/elasticsearch/reference/current/index-modules-similarity.html  
Documentation de la configuration du scoring dans Elasticsearch. Explique comment passer de BM25 (défaut) à d'autres modèles (DFR, DFI, IB, LM Dirichlet), et comment ajuster les paramètres `k1` et `b` de BM25. Le réglage de ces paramètres est un levier avancé d'optimisation de la pertinence.
