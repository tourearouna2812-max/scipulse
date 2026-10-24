# Jour 2 — Matin
# TP : Mapping optimisé et début d'ingestion Hacker News

> **Durée** : 2h45  
> **Niveau** : débutant — chaque étape est détaillée  
> **Prérequis** : avoir complété le Jour 1 (stack lancée, index `arxiv-papers-raw` peuplé avec ~100 000 documents)

---

## Ce que vous allez construire

À la fin de ce TP, vous aurez :

1. Un index `arxiv-papers` avec un mapping explicite optimisé pour la fouille de texte (analyseur custom, multi-fields, nested, term_vector, completion).
2. Réindexé les 100 000 articles depuis `arxiv-papers-raw` vers `arxiv-papers`.
3. Constaté la différence de comportement entre mapping dynamique et mapping explicite.
4. Un index `hn-items` avec un join field pour la relation post/commentaire.
5. Lancé le poller Hacker News et vérifié l'arrivée des premiers items.

---

## Vérifications préalables (5 min)

```bash
# La stack tourne ?
docker compose ps

# Elasticsearch OK ?
curl -s http://localhost:9200/_cluster/health | python -m json.tool

# L'index du Jour 1 est peuplé ?
curl -s http://localhost:9200/arxiv-papers-raw/_count
# → Doit afficher {"count":100000, ...}
```

Si le count est 0 ou l'index n'existe pas, retournez au TP du Jour 1 pour ingérer les données.

---

# PARTIE A — Mapping optimisé ArXiv (1h30)

## Étape 1 — Observer les limites du mapping dynamique (15 min)

Avant de créer notre mapping explicite, mettons en évidence les problèmes du mapping actuel. Ouvrez Kibana Dev Tools (http://localhost:5601 → menu ☰ → Management → Dev Tools).

### 1.1. Regarder le mapping auto-généré

Collez et exécutez cette requête :

```
GET /arxiv-papers-raw/_mapping
```

Parcourez la réponse. Repérez les champs suivants et notez leur type :

| Champ | Type deviné | Le type est-il bon ? |
|-------|-------------|---------------------|
| `arxiv_id` | ? | |
| `title` | ? | |
| `abstract` | ? | |
| `categories` | ? | |
| `primary_category` | ? | |
| `date_updated` | ? | |

Vous constaterez qu'Elasticsearch a créé des multi-fields automatiques (`text` + `keyword`) pour la plupart des chaînes. C'est raisonnable mais sous-optimal : pas de stemming, pas de synonymes, pas de nested pour les auteurs.

### 1.2. Tester le problème de stemming

Cherchons les articles qui contiennent le mot « network » (au singulier) :

```
GET /arxiv-papers-raw/_search
{
  "query": { "match": { "abstract": "network" } },
  "size": 1,
  "_source": ["title"]
}
```

Notez le `hits.total.value` : _____ résultats.

Maintenant cherchons « networks » (au pluriel) :

```
GET /arxiv-papers-raw/_search
{
  "query": { "match": { "abstract": "networks" } },
  "size": 1,
  "_source": ["title"]
}
```

Notez le `hits.total.value` : _____ résultats.

**Les deux nombres sont différents.** C'est le problème : sans stemming, « network » et « networks » sont des termes distincts. Un utilisateur qui cherche « network » ne voit pas les articles qui parlent de « networks ». En recherche d'information, c'est un faux négatif — on rate des résultats pertinents.

### 1.3. Tester le problème de synonymes

```
GET /arxiv-papers-raw/_search
{
  "query": { "match": { "abstract": "CNN" } },
  "size": 1,
  "_source": ["title"]
}
```

Résultats : _____.

```
GET /arxiv-papers-raw/_search
{
  "query": { "match": { "abstract": "convolutional neural network" } },
  "size": 1,
  "_source": ["title"]
}
```

Résultats : _____.

**Les résultats sont très différents.** Les articles qui utilisent l'acronyme « CNN » ne sont pas trouvés par « convolutional neural network », et vice versa. Sans synonymes, l'utilisateur doit deviner la formulation exacte utilisée par les auteurs.

### 1.4. Tester le problème d'agrégation sur les catégories

```
GET /arxiv-papers-raw/_search
{
  "size": 0,
  "aggs": {
    "categories": {
      "terms": { "field": "primary_category", "size": 10 }
    }
  }
}
```

Vous obtiendrez probablement une erreur :

```json
{
  "error": {
    "type": "illegal_argument_exception",
    "reason": "Text fields are not optimised for operations that require per-document field data..."
  }
}
```

Le champ `primary_category` est un `text` — on ne peut pas faire d'agrégation dessus directement. Il faut utiliser le sous-champ `.keyword` :

```
GET /arxiv-papers-raw/_search
{
  "size": 0,
  "aggs": {
    "categories": {
      "terms": { "field": "primary_category.keyword", "size": 10 }
    }
  }
}
```

Ça fonctionne, mais c'est un contournement. Avec un mapping explicite, on aurait directement `primary_category` en type `keyword` — sans besoin du suffixe `.keyword`.

**Bilan** : le mapping dynamique fonctionne pour de l'exploration rapide, mais il n'est pas adapté à une recherche de qualité. Nous allons maintenant créer un mapping explicite qui résout tous ces problèmes.

---

## Étape 2 — Créer l'index avec le mapping explicite (20 min)

### 2.1. Comprendre le mapping que nous allons créer

Le fichier `src/utils/mappings.py` permet la configuration des index :

```bash
code src/utils/mappings.py
```

Le mapping est défini dans la variable `ARXIV_SETTINGS`. Il contient deux parties :

- **`settings`** : la configuration de l'index (nombre de shards, réplicas, et surtout la définition de l'**analyseur custom**).
- **`mappings`** : la structure des champs (types, analyseurs, multi-fields).

Prenons le temps de comprendre chaque section.

### 2.2. Comprendre la section `settings` — l'analyseur custom

```python
"settings": {
    "number_of_shards": 1,
    "number_of_replicas": 0,
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
                    # ... autres synonymes
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
```

Rappel de ce que fait chaque composant (voir le bloc théorique pour les détails) :

| Composant | Rôle | Exemple |
|-----------|------|---------|
| `tokenizer: standard` | Découpe le texte en mots | « AI-based learning » → `[AI, based, learning]` |
| `lowercase` | Met en minuscules | `[AI, based, learning]` → `[ai, based, learning]` |
| `english_stop` | Supprime les mots vides | Supprime « the », « a », « of », « is »… |
| `scientific_synonyms` | Étend les acronymes | `cnn` → `[cnn, convolutional, neural, network]` |
| `english_stemmer` | Réduit à la racine | `learning` → `learn`, `networks` → `network` |

L'ordre des filtres est critique : les synonymes **avant** le stemmer, sinon les formes étendues ne seraient pas stemmées.

### 2.3. Comprendre la section `mappings` — les champs

```python
"mappings": {
    "properties": {
        "arxiv_id":         { "type": "keyword" },
        "title": {
            "type": "text",
            "analyzer": "scientific_english",
            "fields": {
                "keyword": { "type": "keyword" },
                "suggest": { "type": "completion", "analyzer": "simple" }
            }
        },
        "abstract": {
            "type": "text",
            "analyzer": "scientific_english",
            "term_vector": "with_positions_offsets"
        },
        "authors": {
            "type": "nested",
            "properties": {
                "name": { "type": "text", "fields": { "keyword": { "type": "keyword" } } },
                "affiliation": { "type": "text", "fields": { "keyword": { "type": "keyword" } } }
            }
        },
        "authors_flat":     { "type": "text", "fields": { "keyword": { "type": "keyword" } } },
        "categories":       { "type": "keyword" },
        "primary_category": { "type": "keyword" },
        "date_published":   { "type": "date", "format": "yyyy-MM-dd||..." },
        "date_updated":     { "type": "date", "format": "yyyy-MM-dd||..." },
        "doi":              { "type": "keyword" },
        "keywords":         { "type": "keyword" },
        "hn_score":         { "type": "integer" },
        "hn_comments_count":{ "type": "integer" },
        "hn_item_ids":      { "type": "keyword" }
    }
}
```

Champ par champ, voici les choix et leurs raisons :

| Champ | Type | Pourquoi ce type |
|-------|------|------------------|
| `arxiv_id` | `keyword` | Identifiant exact — pas de recherche textuelle |
| `title` | `text` + `keyword` + `completion` | Recherche full-text + agrégation + autocomplétion |
| `abstract` | `text` avec `term_vector` | Recherche full-text + highlighting rapide + more_like_this |
| `authors` | `nested` | Préserver l'association nom ↔ affiliation |
| `authors_flat` | `text` + `keyword` | Version texte simple pour la recherche rapide sans nested query |
| `categories` | `keyword` | Valeurs exactes multi-valuées (tableau) |
| `primary_category` | `keyword` | Facette principale, agrégations directes |
| `date_published` | `date` | Requêtes temporelles, histogrammes, decay |
| `hn_score` | `integer` | Champs prêts pour l'enrichissement croisé HN (Jour 3) |

### 2.4. Créer l'index via le script Python

Ecrivez le script Python `mappings.py`, puis exécutez-le :

```bash
python -m src.utils.mappings
```

Sortie attendue :

```
  ✅ Index 'arxiv-papers' créé.
  ✅ Index 'hn-items' créé.
  ✅ Index 'arxiv-hn-links' créé.

Mappings déployés avec succès.
```

### 2.5. Vérifier la création dans Kibana

Retournez dans Kibana Dev Tools et exécutez :

```
GET /arxiv-papers/_mapping
```

Parcourez la réponse et vérifiez que :

- `title` est en `text` avec l'analyseur `scientific_english` et un sous-champ `keyword` et `suggest`.
- `abstract` est en `text` avec `term_vector: with_positions_offsets`.
- `categories` est en `keyword` (pas `text`).
- `authors` est en `nested`.
- `hn_score` est en `integer`.

### 2.6. Tester l'analyseur custom

Avant d'indexer des données, vérifions que notre analyseur fonctionne correctement :

```
POST /arxiv-papers/_analyze
{
  "analyzer": "scientific_english",
  "text": "A new CNN-based approach to reinforcement learning for NLP tasks"
}
```

Vous devez voir les tokens produits. Vérifiez :

- [ ] « CNN » est étendu en tokens incluant `convolut`, `neural`, `network` (synonymes + stemming).
- [ ] « reinforcement learning » produit aussi `rl` (synonyme inverse).
- [ ] « A » et « to » et « for » ont disparu (stopwords).
- [ ] « learning » est devenu `learn` (stemming).
- [ ] « tasks » est devenu `task` (stemming).

**Exercice** : testez l'analyseur avec d'autres textes pour vérifier qu'il se comporte comme attendu. Essayez par exemple :

```
POST /arxiv-papers/_analyze
{
  "analyzer": "scientific_english",
  "text": "BERT and GPT are large language models used in natural language processing"
}
```

Vérifiez que `bert` est étendu, que `large language models` produit aussi `llm`, et que `natural language processing` produit aussi `nlp`.

---

## Étape 3 — Réindexer les données (25 min)

L'index `arxiv-papers` est créé avec le bon mapping, mais il est vide. Nous devons transférer les 100 000 articles depuis `arxiv-papers-raw` (mapping dynamique) vers `arxiv-papers` (mapping explicite).

Deux approches possibles : l'API `_reindex` d'Elasticsearch (simple mais limitée) ou un script Python (plus flexible).

### 3.1. Approche A — API `_reindex` (simple)

L'API `_reindex` copie les documents d'un index vers un autre, en leur appliquant le nouveau mapping de l'index de destination.

Dans Kibana Dev Tools :

```
POST /_reindex
{
  "source": {
    "index": "arxiv-papers-raw"
  },
  "dest": {
    "index": "arxiv-papers"
  }
}
```

**Attention** : cette opération prend du temps (1 à 5 minutes pour 100 000 documents). Kibana peut afficher un timeout. Ce n'est pas grave — la réindexation continue en arrière-plan.

Pour suivre la progression, ouvrez un terminal et lancez :

```bash
bash scripts/monitor_ingestion.sh arxiv-papers
```

Ou vérifiez manuellement :

```bash
curl -s http://localhost:9200/arxiv-papers/_count
```

**Limite de cette approche** : les documents sont copiés tels quels depuis l'index source. Si le champ `authors` est une chaîne brute dans `arxiv-papers-raw` (ex. `"Alice Martin, Bob Chen"`), il sera indexé tel quel dans `arxiv-papers` — il ne sera pas automatiquement transformé en tableau d'objets `nested`. Pour ça, il faut un script Python.

### 3.2. Approche B — Script Python de réindexation (recommandée)

Cette approche est plus puissante : elle permet de transformer les documents pendant la réindexation.

Créez le fichier `src/ingestion/reindex_arxiv.py` :

```python
"""
SciPulse — Réindexation ArXiv : de arxiv-papers-raw vers arxiv-papers
Transforme les documents pendant la copie (parsing des auteurs, etc.)

Usage :
    python -m src.ingestion.reindex_arxiv
"""

import re
import sys
import time

from elasticsearch import Elasticsearch, helpers

ES_HOST      = "http://localhost:9200"
SOURCE_INDEX = "arxiv-papers-raw"
DEST_INDEX   = "arxiv-papers"
BATCH_SIZE   = 500


def parse_authors(authors_str: str) -> tuple[list[dict], str]:
    """
    Parse la chaîne d'auteurs en une liste structurée.

    Le dump ArXiv a les auteurs sous différents formats :
    - "Firstname Lastname, Firstname Lastname, ..."
    - "Lastname, Firstname and Lastname, Firstname"

    On retourne à la fois la version structurée (pour le nested)
    et la version plate (pour la recherche simple).
    """
    if not authors_str:
        return [], ""

    # 1. Séparer par " and " ou par virgule suivie d'une majuscule
    # (heuristique simple — pas parfait mais fonctionnel pour le TP)
    # indice : utiliser une expression régulière

    authors = []
    # 2. Reconstruire un tableau propore des auteurs
    
    return authors, authors_str


def transform_for_reindex(hit: dict) -> dict:
    """
    Transforme un document source en document destination.
    """
    # 1. Récupérer la source du document

    # 2. Parser les auteurs (cf. parse_authors)

    # 3. Reconstruire la source di document
    # 
    return doc


def scroll_source(es: Elasticsearch) -> int:
    """Compte les documents dans l'index source."""
    return es.count(index=SOURCE_INDEX)["count"]


def generate_actions(es: Elasticsearch):
    """Générateur : lit l'index source et produit des actions pour l'index destination."""
    # 1. Scanner l'index entier de manière efficace (scroll API) pour transformer chaque document

        # 1.1 Transformer le document
        
        # 1.2  Ignorer l'article s'il ne possède pas d'id Arxiv

        # 1.3 Reteourner un documet ES
        # Attention ! On utilise ici un générateur


def main():
    print()
    print("╔══════════════════════════════════════════════════════════╗")
    print("║  SciPulse — Réindexation ArXiv (mapping optimisé)       ║")
    print("╚══════════════════════════════════════════════════════════╝")
    print()

    es = Elasticsearch(ES_HOST)

    # Vérifications
    try:
        es.info()
        print("✅ Connecté à Elasticsearch")
    except Exception as e:
        print(f"❌ Connexion impossible : {e}")
        sys.exit(1)

    # 1. Vérifier que l'index d'origine existe

    # 2. Vérifier que l'oindex de destination existe (sinon, sortir)
    
    source_count = scroll_source(es)
    print(f"📂 Source : '{SOURCE_INDEX}' — {source_count:,} documents")
    print(f"📂 Dest.  : '{DEST_INDEX}'")

    # Vider l'index destination s'il contient déjà des données
    dest_count = es.count(index=DEST_INDEX)["count"]
    if dest_count > 0:
        # 3. Que faire si l'index contient déjà des articles

    # Réindexation
    print(f"\n🔄 Réindexation en cours...\n")

    start_time = time.time()
    success = 0
    errors = 0

    for ok, result in helpers.streaming_bulk(
        es,
        generate_actions(es),
        chunk_size=BATCH_SIZE,
        raise_on_error=False,
    ):
        if ok:
            success += 1
        else:
            errors += 1
            if errors <= 3:
                print(f"   ❌ {result}")

        total = success + errors
        if total % 10_000 == 0:
            elapsed = time.time() - start_time
            rate = total / elapsed if elapsed > 0 else 0
            pct = total / source_count * 100 if source_count > 0 else 0
            print(f"   {pct:5.1f}%  {total:>7,} / {source_count:,}  ({rate:,.0f} docs/sec)")

    elapsed = time.time() - start_time
    es.indices.refresh(index=DEST_INDEX)
    final_count = es.count(index=DEST_INDEX)["count"]

    print(f"\n{'═' * 50}")
    print(f"  ✅ Réindexés  : {success:>10,}")
    print(f"  ❌ Erreurs    : {errors:>10,}")
    print(f"  ⏱  Durée      : {elapsed:>10.1f} sec")
    print(f"  🔍 Vérification : {final_count:,} documents dans '{DEST_INDEX}'")
    print(f"{'═' * 50}")


if __name__ == "__main__":
    main()
```

Sauvegardez et exécutez :

```bash
python -m src.ingestion.reindex_arxiv
```

Sortie attendue :

```
╔══════════════════════════════════════════════════════════╗
║  SciPulse — Réindexation ArXiv (mapping optimisé)       ║
╚══════════════════════════════════════════════════════════╝

✅ Connecté à Elasticsearch
📂 Source : 'arxiv-papers-raw' — 100,000 documents
📂 Dest.  : 'arxiv-papers'

🔄 Réindexation en cours...

    10.0%   10,000 / 100,000  (2,345 docs/sec)
    20.0%   20,000 / 100,000  (2,298 docs/sec)
    ...
   100.0%  100,000 / 100,000  (2,150 docs/sec)

══════════════════════════════════════════════════════════
  ✅ Réindexés  :    100,000
  ❌ Erreurs    :          0
  ⏱  Durée      :       46.5 sec
  🔍 Vérification : 100,000 documents dans 'arxiv-papers'
══════════════════════════════════════════════════════════
```

---

## Étape 4 — Constater la différence (15 min)

Maintenant vient le moment satisfaisant : vérifions que notre mapping explicite résout les problèmes identifiés à l'étape 1.

### 4.1. Test du stemming

Dans Kibana Dev Tools :

```
GET /arxiv-papers/_search
{
  "query": { "match": { "abstract": "network" } },
  "size": 0
}
```

Notez le `hits.total.value` : _____ résultats.

```
GET /arxiv-papers/_search
{
  "query": { "match": { "abstract": "networks" } },
  "size": 0
}
```

Notez le `hits.total.value` : _____ résultats.

**Les deux nombres sont maintenant identiques** (ou très proches). Le stemmer réduit « network » et « networks » au même radical `network` → même résultat. Le problème de faux négatifs est résolu.

### 4.2. Test des synonymes

```
GET /arxiv-papers/_search
{
  "query": { "match": { "abstract": "CNN" } },
  "size": 0
}
```

Résultats : _____.

```
GET /arxiv-papers/_search
{
  "query": { "match": { "abstract": "convolutional neural network" } },
  "size": 0
}
```

Résultats : _____.

**Les résultats sont maintenant beaucoup plus proches.** « CNN » et « convolutional neural network » sont traités comme synonymes par l'analyseur. Un utilisateur qui cherche l'un trouve aussi les articles qui utilisent l'autre.

### 4.3. Test des agrégations sur les catégories

```
GET /arxiv-papers/_search
{
  "size": 0,
  "aggs": {
    "top_categories": {
      "terms": { "field": "primary_category", "size": 10 }
    }
  }
}
```

**Cette fois, pas besoin de `.keyword`** — le champ `primary_category` est directement en type `keyword`. L'agrégation fonctionne naturellement et retourne les catégories complètes (`cs.AI`, `cs.LG`, etc.), pas des fragments tokenisés.

### 4.4. Test du multi_match avec boost

```
GET /arxiv-papers/_search
{
  "query": {
    "multi_match": {
      "query": "transformer attention mechanism",
      "fields": ["title^3", "abstract"],
      "type": "cross_fields"
    }
  },
  "size": 3,
  "_source": ["title", "primary_category"],
  "highlight": {
    "fields": {
      "title": {},
      "abstract": { "fragment_size": 150, "number_of_fragments": 2 }
    }
  }
}
```

Observez :

- Les résultats les plus pertinents apparaissent en premier (grâce au scoring BM25 + boost `^3` sur le titre).
- Le **highlighting** surligne les termes trouvés dans le titre et l'abstract. Les balises `<em>` entourent les termes matchés.
- Le `cross_fields` traite « transformer attention mechanism » comme si les trois mots pouvaient être répartis entre `title` et `abstract`.

### 4.5. Test de l'API `_analyze` sur un abstract réel

Prenons l'abstract d'un article réel et regardons comment l'analyseur le traite :

```
GET /arxiv-papers/_search
{
  "size": 1,
  "query": { "match": { "primary_category": "cs.AI" } },
  "_source": ["abstract"]
}
```

Copiez les 50 premiers mots de l'abstract retourné, puis testez l'analyseur :

```
POST /arxiv-papers/_analyze
{
  "analyzer": "scientific_english",
  "text": "<collez les 50 premiers mots ici>"
}
```

Examinez les tokens produits. Cherchez :

- Les stopwords ont-ils été supprimés ?
- Les termes techniques sont-ils bien stemmés ?
- Les acronymes connus (CNN, RNN, NLP…) sont-ils étendus en synonymes ?

---

# PARTIE B — Mapping HN et début du poller (45 min)

## Étape 5 — Comprendre le mapping Hacker News (10 min)

L'index `hn-items` a déjà été créé par le script `mappings.py` à l'étape 2.4. Examinons son mapping dans Kibana :

```
GET /hn-items/_mapping
```

Les points clés à repérer :

### Le champ `relation` (join field)

```json
"relation": {
  "type": "join",
  "relations": {
    "story": "comment"
  }
}
```

Ce champ établit une relation parent-enfant :

- Un document avec `"relation": { "name": "story" }` est un **parent** (post HN).
- Un document avec `"relation": { "name": "comment", "parent": "12345" }` est un **enfant** (commentaire rattaché au post 12345).

Les deux types de documents coexistent dans le même index.

### Le champ `arxiv_id_linked`

```json
"arxiv_id_linked": { "type": "keyword" }
```

Si un post HN pointe vers un article ArXiv (l'URL contient `arxiv.org/abs/...`), le poller extraira l'ID ArXiv et le stockera ici. C'est la base de l'enrichissement croisé du Jour 3.

---

## Étape 6 — Comprendre le poller HN (10 min)

Ouvrez le fichier `src/ingestion/hn_poller.py` :

```bash
code src/ingestion/hn_poller.py
```

Le script fait trois choses en boucle :

1. **Appelle l'API HN** (`https://hacker-news.firebaseio.com/v0/newstories.json`) pour récupérer les IDs des 500 dernières stories.
2. **Pour chaque story non encore indexée**, récupère le détail via `/v0/item/{id}.json` et le transforme en document ES.
3. **Détecte les liens ArXiv** dans les URLs des stories (regex sur `arxiv.org/abs/`).

Parcourez les fonctions clés :

**`fetch_new_story_ids()`** : un simple `GET` sur l'API Firebase. Retourne une liste de 500 entiers.

**`prepare_hn_doc(item)`** : transforme un item HN brut en document ES. Observez comment le champ `relation` est construit différemment selon le type :

```python
if item.get("type") == "comment" and item.get("parent"):
    doc["relation"] = {"name": "comment", "parent": str(item["parent"])}
else:
    doc["relation"] = {"name": "story"}
```

**`extract_arxiv_id(url)`** : regex qui détecte les URLs ArXiv et extrait l'ID :

```python
ARXIV_URL_PATTERN = re.compile(r"arxiv\.org/(?:abs|pdf)/(\d{4}\.\d{4,5}(?:v\d+)?)")
```

Par exemple, `https://arxiv.org/abs/2301.07041` → `"2301.07041"`.

---

## Étape 7 — Lancer le poller (15 min)

### 7.1. Exécuter un cycle de polling

```bash
python -m src.ingestion.hn_poller
```

Sortie attendue :

```
2024-01-15 14:30:00 [INFO] Connexion ES: scipulse-cluster
2024-01-15 14:30:01 [INFO] 📡 500 story IDs récupérés depuis l'API HN
2024-01-15 14:30:01 [INFO]   → 87 nouveaux items à indexer
2024-01-15 14:30:05 [INFO]   ✅ Batch 1: 50 documents indexés
2024-01-15 14:30:08 [INFO]   ✅ Batch 2: 37 documents indexés
2024-01-15 14:30:08 [INFO] 🏁 Cycle terminé — 87 documents indexés au total.
```

Le nombre exact varie — il dépend de l'activité sur HN au moment du polling.

### 7.2. Vérifier dans Elasticsearch

```bash
curl -s http://localhost:9200/hn-items/_count | python -m json.tool
```

Vous devriez voir quelques dizaines de documents.

### 7.3. Vérifier dans Kibana

Créez un Data View pour l'index HN :

1. Menu ☰ → Management → Stack Management → Kibana → Data Views.
2. Create data view :
   - **Name** : `hn-items`
   - **Index pattern** : `hn-items`
   - **Timestamp field** : `time`
3. Save.

Allez dans Discover, sélectionnez le data view `hn-items`, et élargissez la plage temporelle. Vous devriez voir les stories récentes.

Cliquez sur un document et vérifiez :

- [ ] Le champ `type` est `story`.
- [ ] Le champ `title` contient le titre du post.
- [ ] Le champ `score` est un nombre entier.
- [ ] Le champ `relation` contient `{"name": "story"}`.

### 7.4. Vérifier la détection de liens ArXiv

Cherchons si le poller a détecté des liens vers ArXiv :

```
GET /hn-items/_search
{
  "query": {
    "exists": { "field": "arxiv_id_linked" }
  },
  "size": 5,
  "_source": ["title", "url", "arxiv_id_linked", "score"]
}
```

S'il y a des résultats, vous verrez des posts HN qui pointent vers ArXiv — avec l'ID ArXiv extrait automatiquement. Si aucun résultat, c'est normal : les liens ArXiv sur HN ne sont pas si fréquents. On en trouvera davantage avec le temps.

Vérifiez aussi l'index des liens croisés :

```bash
curl -s http://localhost:9200/arxiv-hn-links/_count
```

### 7.5. Lancer un second cycle pour voir la déduplication

```bash
python -m src.ingestion.hn_poller
```

Cette fois, le message devrait indiquer beaucoup moins de nouveaux items :

```
📡 500 story IDs récupérés depuis l'API HN
  → 12 nouveaux items à indexer
```

Les items déjà indexés sont ignorés grâce à la vérification `item_exists()`. C'est l'idempotence en action.

### 7.6. (Optionnel) Mode continu

Si vous voulez voir le poller tourner en boucle :

```bash
python -m src.ingestion.hn_poller --continuous --interval 30
```

Il poliera l'API toutes les 30 secondes. Laissez-le tourner en arrière-plan pendant le reste du cours pour accumuler des données. `Ctrl+C` pour l'arrêter.

---

## Étape 8 — Requêtes de synthèse (10 min)

Pour conclure, exécutons quelques requêtes qui démontrent la puissance du nouveau mapping.

### 8.1. Recherche full-text avec highlighting

```
GET /arxiv-papers/_search
{
  "query": {
    "multi_match": {
      "query": "deep reinforcement learning robotics",
      "fields": ["title^3", "abstract"],
      "type": "cross_fields"
    }
  },
  "highlight": {
    "fields": {
      "abstract": { "fragment_size": 200, "number_of_fragments": 2 }
    },
    "pre_tags": [">>>"],
    "post_tags": ["<<<"]
  },
  "size": 3,
  "_source": ["title", "primary_category", "date_updated"]
}
```

### 8.2. Agrégation temporelle

```
GET /arxiv-papers/_search
{
  "size": 0,
  "aggs": {
    "publications_par_annee": {
      "date_histogram": {
        "field": "date_updated",
        "calendar_interval": "year"
      }
    }
  }
}
```

Observez la croissance exponentielle des publications au fil des années.

### 8.3. Top catégories avec sous-agrégation

```
GET /arxiv-papers/_search
{
  "size": 0,
  "aggs": {
    "top_categories": {
      "terms": { "field": "primary_category", "size": 5 },
      "aggs": {
        "avg_abstract_length": {
          "avg": {
            "script": "doc['abstract'].size() > 0 ? doc['abstract'].value.length() : 0"
          }
        }
      }
    }
  }
}
```

### 8.4. Filtrage par catégorie + recherche textuelle

```
GET /arxiv-papers/_search
{
  "query": {
    "bool": {
      "must": [
        { "match": { "abstract": "transformer" } }
      ],
      "filter": [
        { "term": { "primary_category": "cs.CL" } },
        { "range": { "date_updated": { "gte": "2022-01-01" } } }
      ]
    }
  },
  "size": 5,
  "_source": ["title", "primary_category", "date_updated"]
}
```

Cette requête combine :

- Une recherche textuelle (`must` → contribue au score).
- Un filtre par catégorie (`filter` → ne contribue pas au score, juste inclusion/exclusion).
- Un filtre temporel (`filter` → articles depuis 2022).

---

## Étape 9 — Commit (5 min)

```bash
git add src/ingestion/reindex_arxiv.py
git commit -m "feat: add reindexation script + optimized ES mapping"
```

---

## Checklist de fin de matinée

- [ ] Index `arxiv-papers` créé avec le mapping explicite (analyseur `scientific_english`, multi-fields, nested, term_vector)
- [ ] 100 000 documents réindexés dans `arxiv-papers`
- [ ] Constaté la différence de comportement (stemming, synonymes, agrégations) entre mapping dynamique et explicite
- [ ] Index `hn-items` créé avec le join field
- [ ] Poller HN exécuté, premiers items indexés
- [ ] Requêtes de synthèse exécutées avec succès (multi_match, highlight, agrégations, bool)
- [ ] Commit Git propre

## Ce qui vient cet après-midi

Cet après-midi, nous attaquons la **qualité des données** avec Great Expectations. Nous définirons des suites de tests automatisés qui valident que nos données respectent des contrats de qualité (pas de nulls, dates réalistes, abstracts suffisamment longs…) — un pilier fondamental du DataOps.
