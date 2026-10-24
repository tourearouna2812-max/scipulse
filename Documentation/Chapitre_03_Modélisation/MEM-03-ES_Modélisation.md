# Mémento — Fonctions utiles pour le TP J2 matin
# Mapping Elasticsearch, réindexation et poller Hacker News

> Classement par bibliothèque / module, avec une courte explication et un exemple d'usage tiré du code du TP.
> Ce mémento est autonome : toutes les fonctions utilisées dans le TP sont documentées.

---

## 1. `elasticsearch.Elasticsearch` — Client Python

### `Elasticsearch(host)`

Crée une connexion au cluster Elasticsearch.

```python
from elasticsearch import Elasticsearch
es = Elasticsearch("http://localhost:9200")
```

### `es.info()`

Retourne les informations du cluster (nom, version). Vérifie que la connexion fonctionne.

```python
info = es.info()
print(info["cluster_name"])
```

### `es.count(index)`

Compte les documents dans un index.

```python
count = es.count(index="arxiv-papers-raw")["count"]   # → 100000
```

### `es.exists(index, id)`

Vérifie si un document avec cet ID existe dans l'index. Retourne `True` ou `False`. Utilisé dans le poller HN pour ne pas réindexer les items déjà présents.

```python
if es.exists(index="hn-items", id="12345678"):
    print("Item déjà indexé")
```

### `es.indices.exists(index)`

Vérifie si un index existe.

```python
if not es.indices.exists(index="arxiv-papers"):
    print("Index non trouvé — exécutez python -m src.utils.mappings")
```

### `es.indices.create(index, body)`

Crée un index avec un mapping et des settings explicites. C'est l'opération centrale du TP — on passe du mapping auto-généré au mapping optimisé.

```python
es.indices.create(index="arxiv-papers", body={
    "settings": {
        "number_of_shards": 1,
        "analysis": {
            "analyzer": {
                "scientific_english": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": ["lowercase", "english_stop", "scientific_synonyms", "english_stemmer"]
                }
            }
        }
    },
    "mappings": {
        "properties": {
            "arxiv_id": {"type": "keyword"},
            "title": {"type": "text", "analyzer": "scientific_english"},
            "abstract": {"type": "text", "analyzer": "scientific_english"},
            ...
        }
    }
})
```

### `es.indices.delete(index)`

Supprime un index et toutes ses données. Irréversible.

```python
es.indices.delete(index="arxiv-papers")
```

### `es.indices.get(index)`

Retourne le mapping, les settings et les alias d'un index. Utilisé pour sauvegarder le mapping avant de recréer un index.

```python
index_info = es.indices.get(index="arxiv-papers")
mapping = index_info["arxiv-papers"]["mappings"]
settings = index_info["arxiv-papers"]["settings"]["index"]
```

### `es.indices.get_mapping(index)`

Retourne uniquement le mapping d'un index (sans les settings).

```python
mapping = es.indices.get_mapping(index="arxiv-papers-raw")
properties = mapping["arxiv-papers-raw"]["mappings"]["properties"]
# → {"arxiv_id": {"type": "text"}, ...}   ← mapping auto-généré (souvent sous-optimal)
```

### `es.indices.refresh(index)`

Force ES à rendre les documents récemment indexés cherchables immédiatement.

```python
es.indices.refresh(index="arxiv-papers")
```

### `es.search(index, query, size, sort, _source)`

Exécute une requête de recherche.

```python
result = es.search(
    index="arxiv-papers",
    query={"match": {"abstract": "transformer attention"}},
    size=3,
    _source=["arxiv_id", "title"],
)
for hit in result["hits"]["hits"]:
    print(hit["_source"]["title"])
```

---

## 2. `elasticsearch.helpers` — Opérations en masse

### `helpers.scan(es, index, query, size)`

Parcourt un index entier via l'API scroll. Retourne un itérateur de hits. Indispensable pour lire 100 000+ documents (la limite de `es.search()` est 10 000).

```python
from elasticsearch import helpers

for hit in helpers.scan(
    es,
    index="arxiv-papers-raw",
    query={"query": {"match_all": {}}},
    size=500,       # Taille des lots internes (pas le nombre total)
):
    doc = hit["_source"]
```

### `helpers.streaming_bulk(es, actions, chunk_size, raise_on_error)`

Indexe des documents en masse. Consomme un générateur d'actions et les envoie par lots. Retourne `(ok, result)` pour chaque document.

```python
for ok, result in helpers.streaming_bulk(
    es,
    generate_actions(es),
    chunk_size=500,
    raise_on_error=False,
):
    if ok:
        success += 1
```

### `helpers.bulk(es, actions, raise_on_error)`

Version simplifiée de `streaming_bulk`. Retourne `(success_count, errors_list)`. Utilisé dans le poller HN.

```python
success, errors = helpers.bulk(es, all_actions, raise_on_error=False)
```

### Format d'une action d'indexation

```python
yield {
    "_index":  "arxiv-papers",
    "_id":     doc["arxiv_id"],       # Clé de déduplication → idempotence
    "_source": doc,                    # Le document lui-même
}
```

### Routing pour les relations parent-child (poller HN)

Les documents child doivent être routés vers le même shard que leur parent.

```python
action = {
    "_index": "hn-items",
    "_id": doc["hn_id"],
    "_source": doc,
}
if doc["type"] == "comment" and doc.get("parent"):
    action["_routing"] = doc["parent"]     # ← Force le même shard que le parent
```

---

## 3. Types de champs Elasticsearch — Le mapping

### Types utilisés dans le mapping ArXiv

| Type ES | Usage | Pourquoi ce type |
|---------|-------|-----------------|
| `keyword` | `arxiv_id`, `categories`, `primary_category`, `doi` | Valeurs exactes — agrégations (`terms`), filtres (`term`), tri |
| `text` | `title`, `abstract`, `authors_flat` | Texte libre — analysé, tokenisé, cherchable en full-text |
| `date` | `date_published`, `date_updated` | Dates — requêtes `range`, histogrammes, tri chronologique |
| `integer` | `hn_score`, `hn_comments_count` | Nombres entiers — filtres `range`, `field_value_factor` |
| `nested` | `authors` | Objets imbriqués indépendants — requêtes sur des sous-champs liés |
| `completion` | `title.suggest` | Autocomplétion rapide (structure de données optimisée) |
| `join` | `relation` (dans hn-items) | Relations parent-child entre documents du même index |

### `keyword` vs `text` — La distinction fondamentale

C'est le concept le plus important du TP. Un champ `text` est **analysé** (tokenisé, mis en minuscules, stemmé) — optimisé pour la recherche full-text. Un champ `keyword` est stocké **tel quel** — optimisé pour le filtrage exact et les agrégations.

```
"cs.AI cs.LG stat.ML" indexé en tant que text :
  → Tokens : ["cs.ai", "cs.lg", "stat.ml"]   (analysé, minuscules)
  → Recherche "cs.AI" → match ✅
  → Agrégation terms → PROBLÈME (tokens au lieu de valeurs exactes)

"cs.AI" indexé en tant que keyword :
  → Stocké : "cs.AI"   (tel quel)
  → Filtre term "cs.AI" → match ✅
  → Agrégation terms → "cs.AI": 15000 ✅
```

**Règle pratique** :
- Si vous cherchez *dans* le champ → `text`
- Si vous filtrez/agrégez *par* le champ → `keyword`
- Si vous avez besoin des deux → multi-field (`text` + sous-champ `keyword`)

### Multi-fields — Le meilleur des deux mondes

Un champ peut avoir plusieurs sous-champs avec des types différents :

```json
"title": {
    "type": "text",
    "analyzer": "scientific_english",
    "fields": {
        "keyword": {"type": "keyword"},
        "suggest": {"type": "completion", "analyzer": "simple"}
    }
}
```

- `title` → recherche full-text (analysé)
- `title.keyword` → agrégation, tri, filtre exact
- `title.suggest` → autocomplétion

### `term_vector: "with_positions_offsets"`

Active le stockage des term vectors pour un champ. Nécessaire pour `more_like_this` (Jour 3) et le highlighting avancé. Augmente la taille de l'index.

```json
"abstract": {
    "type": "text",
    "analyzer": "scientific_english",
    "term_vector": "with_positions_offsets"
}
```

### Type `nested` — Objets imbriqués indépendants

Par défaut, les objets dans un tableau ES sont « aplatis » : une requête sur `authors.name = "Alice"` ET `authors.affiliation = "MIT"` matcherait un document où Alice est à Stanford et Bob est au MIT. Le type `nested` préserve les associations.

```json
"authors": {
    "type": "nested",
    "properties": {
        "name": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
        "affiliation": {"type": "text", "fields": {"keyword": {"type": "keyword"}}}
    }
}
```

### Type `join` — Relations parent-child

Permet de modéliser des relations hiérarchiques (stories → comments) dans un seul index.

```json
"relation": {
    "type": "join",
    "relations": {
        "story": "comment"
    }
}
```

Les documents child doivent être sur le même shard que leur parent (routing obligatoire).

### Format de date — `format`

Déclare les formats de date acceptés. ES essaie chaque format dans l'ordre.

```json
"date_published": {
    "type": "date",
    "format": "yyyy-MM-dd||yyyy-MM-dd'T'HH:mm:ss||epoch_millis"
}

"time": {
    "type": "date",
    "format": "epoch_second"
}
```

---

## 4. Analyseurs Elasticsearch — Le pipeline d'analyse de texte

### Structure d'un analyseur custom

Un analyseur se compose d'un tokenizer et d'une chaîne de filtres de tokens.

```
Texte brut : "CNNs for Image Classification"
      │
      ▼ tokenizer: standard
["CNNs", "for", "Image", "Classification"]
      │
      ▼ filter: lowercase
["cnns", "for", "image", "classification"]
      │
      ▼ filter: english_stop (supprime stopwords)
["cnns", "image", "classification"]
      │
      ▼ filter: scientific_synonyms
["cnns", "convolutional neural network", "image", "classification"]
      │
      ▼ filter: english_stemmer
["cnn", "convolut", "neural", "network", "imag", "classif"]
```

### Filtres de tokens utilisés

| Filtre | Rôle | Configuration |
|--------|------|---------------|
| `lowercase` | Convertit en minuscules | Intégré (pas de config) |
| `english_stop` | Supprime les stopwords anglais | `{"type": "stop", "stopwords": "_english_"}` |
| `scientific_synonyms` | Étend les acronymes scientifiques | `{"type": "synonym", "synonyms": ["cnn, convolutional neural network", ...]}` |
| `english_stemmer` | Réduit les mots à leur racine | `{"type": "stemmer", "language": "english"}` |

### API `_analyze` — Tester un analyseur

Permet de voir comment un texte est décomposé par un analyseur. Indispensable pour le debugging.

```
POST /arxiv-papers/_analyze
{
  "analyzer": "scientific_english",
  "text": "CNNs for Image Classification"
}
```

Résultat : liste des tokens avec leur position et offset.

---

## 5. Requêtes Kibana Dev Tools utilisées dans le TP

### Inspection du mapping

```
GET /arxiv-papers-raw/_mapping
```

Montre le mapping auto-généré par ES (souvent sous-optimal).

### Inspection du mapping optimisé

```
GET /arxiv-papers/_mapping
```

Montre le mapping explicite créé par `mappings.py`.

### Test de l'analyseur

```
POST /arxiv-papers/_analyze
{
  "analyzer": "scientific_english",
  "text": "Deep Reinforcement Learning for Robotics"
}
```

### Recherche full-text (profite de l'analyseur)

```
GET /arxiv-papers/_search
{
  "query": {
    "match": {
      "abstract": "CNN image classification"
    }
  },
  "size": 3,
  "_source": ["title", "primary_category"]
}
```

### Agrégation sur un champ keyword

```
GET /arxiv-papers/_search
{
  "size": 0,
  "aggs": {
    "top_categories": {
      "terms": {"field": "primary_category", "size": 10}
    }
  }
}
```

Cette agrégation ne fonctionne que si `primary_category` est typé `keyword` (pas `text`). C'est la raison principale de la réindexation.

### Réindexation via l'API ES (alternative au script Python)

```
POST /_reindex
{
  "source": {"index": "arxiv-papers-raw"},
  "dest":   {"index": "arxiv-papers"}
}
```

Copie tous les documents d'un index à l'autre. Le mapping de l'index destination est appliqué. Limité : pas de transformation des documents (le script Python est plus flexible).

### Requête `has_child` (relations parent-child HN)

```
GET /hn-items/_search
{
  "query": {
    "has_child": {
      "type": "comment",
      "query": {"match": {"text": "interesting"}},
      "score_mode": "max"
    }
  },
  "size": 5,
  "_source": ["title", "score", "by"]
}
```

Trouve les stories qui ont des commentaires contenant « interesting ».

---

## 6. `requests` — Appels HTTP (poller HN)

### `requests.get(url, timeout)`

Effectue une requête HTTP GET. Utilisé pour appeler l'API Firebase de Hacker News.

```python
import requests

resp = requests.get("https://hacker-news.firebaseio.com/v0/newstories.json", timeout=10)
resp.raise_for_status()     # Lève une exception si HTTP 4xx/5xx
story_ids = resp.json()     # → [41234567, 41234566, 41234565, ...]
```

### `resp.raise_for_status()`

Lève une `HTTPError` si le code de statut HTTP est 4xx ou 5xx. Sans cet appel, une erreur serveur passerait silencieusement.

### `resp.json()`

Parse la réponse HTTP comme JSON et retourne un dict ou une list Python.

```python
item = requests.get(f"https://hacker-news.firebaseio.com/v0/item/{item_id}.json", timeout=10).json()
# → {"id": 41234567, "type": "story", "title": "...", "url": "...", "score": 42, ...}
```

---

## 7. `re` — Expressions régulières (bibliothèque standard)

### `re.compile(pattern)`

Compile une expression régulière pour une utilisation répétée (plus efficace que `re.search()` à chaque appel dans une boucle).

```python
import re
ARXIV_URL_PATTERN = re.compile(r"arxiv\.org/(?:abs|pdf)/(\d{4}\.\d{4,5}(?:v\d+)?)")
```

### `pattern.search(string)`

Cherche le pattern dans la chaîne. Retourne un objet `Match` ou `None`.

```python
match = ARXIV_URL_PATTERN.search("https://arxiv.org/abs/2301.07041v2")
if match:
    arxiv_id = match.group(1)    # → "2301.07041v2"
```

### `re.split(pattern, string)`

Découpe une chaîne selon un pattern regex. Utilisé pour parser la liste d'auteurs.

```python
# Séparer par ", " suivi d'une majuscule OU par " and "
parts = re.split(r",\s+(?=[A-Z])|\s+and\s+", "Alice Martin, Bob Chen and Carol White")
# → ["Alice Martin", "Bob Chen", "Carol White"]
```

| Élément du pattern | Signification |
|-------------------|---------------|
| `\s+` | Un ou plusieurs espaces |
| `(?=[A-Z])` | Lookahead : suivi d'une majuscule (ne consomme pas) |
| `\|` | OU |
| `\d{4}` | Exactement 4 chiffres |
| `\d{4,5}` | 4 ou 5 chiffres |
| `(?:...)` | Groupe non-capturant |
| `(...)` | Groupe capturant (accessible via `match.group(1)`) |
| `?` | Optionnel (0 ou 1 occurrence) |

---

## 8. `logging` — Journalisation (bibliothèque standard)

### Configuration de base

```python
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)
```

### Niveaux de log

```python
logger.debug("Détail fin")           # Masqué avec level=INFO
logger.info("📡 500 stories récupérés")
logger.warning("⚠️  3 erreurs d'indexation")
logger.error("❌ Connexion ES impossible")
```

| Niveau | Quand l'utiliser |
|--------|-----------------|
| `DEBUG` | Détails de développement (off en production) |
| `INFO` | Événements normaux (progression, compteurs) |
| `WARNING` | Problème non bloquant (erreur individuelle, retry) |
| `ERROR` | Problème bloquant (connexion impossible) |

---

## 9. `datetime` — Dates et horodatage (bibliothèque standard)

### `datetime.now(timezone.utc).isoformat()`

Retourne l'horodatage courant en UTC au format ISO 8601. Utilisé dans le poller HN pour le champ `detected_at` des liens ArXiv.

```python
from datetime import datetime, timezone

ts = datetime.now(timezone.utc).isoformat()
# → "2024-06-15T14:30:00.123456+00:00"
```

---

## 10. Fonctions Python natives

### `str.strip()` / `str.replace(old, new)`

Nettoyage de texte (espaces, retours à la ligne).

```python
title = source.get("title", "").strip()
abstract = source.get("abstract", "").replace("\n", " ").strip()
```

### `str.get(key, default)` pour les dicts

Accès sûr — retourne `default` si la clé est absente.

```python
authors_raw = source.get("authors", "")
score = item.get("score", 0)
```

### `str(value)`

Conversion en chaîne. Utilisé pour les IDs HN (integers dans l'API, keywords dans ES).

```python
doc["hn_id"] = str(item["id"])              # 41234567 → "41234567"
doc["parent"] = str(item["parent"]) if item.get("parent") else None
```

---

## 11. Pattern « générateur » pour la réindexation

Le script `reindex_arxiv.py` utilise un générateur pour transformer et réindexer les documents un par un sans les charger tous en mémoire :

```python
def generate_actions(es):
    for hit in helpers.scan(es, index="arxiv-papers-raw", query={"query": {"match_all": {}}}, size=500):
        doc = transform_for_reindex(hit)    # Transformation à la volée
        if not doc.get("arxiv_id"):
            continue
        yield {
            "_index":  "arxiv-papers",
            "_id":     doc["arxiv_id"],
            "_source": doc,
        }
```

Ce générateur est ensuite consommé par `helpers.streaming_bulk`.

---

## 12. Architecture du poller HN — Flux de données

```
API Firebase HN (/v0/newstories.json)
        │
   fetch_new_story_ids()          → [41234567, 41234566, ...]
        │
   item_exists() pour chacun      → Filtrer les déjà indexés
        │
   fetch_item() pour les nouveaux  → Détail de chaque item
        │
   prepare_hn_doc()                → Transformer en document ES
        │                              + détecter les liens ArXiv
        │                              + préparer la relation parent-child
        │
   index_batch()                   → helpers.bulk()
        │
        ├──▶ hn-items (index principal)
        │
        └──▶ arxiv-hn-links (si lien ArXiv détecté)
```

### Fonctions du poller

| Fonction | Rôle | Retourne |
|----------|------|----------|
| `fetch_new_story_ids()` | Récupère les 500 derniers IDs depuis l'API | `list[int]` |
| `fetch_item(item_id)` | Récupère le détail d'un item | `dict` ou `None` |
| `extract_arxiv_id(url)` | Extrait l'ID ArXiv d'une URL | `str` ou `None` |
| `item_exists(es, item_id)` | Vérifie si l'item est déjà indexé | `bool` |
| `prepare_hn_doc(item)` | Transforme un item brut en document ES | `dict` |
| `index_batch(es, items)` | Indexe un lot de documents (+ liens) | `int` (nb indexés) |
| `poll_once(es)` | Un cycle complet : fetch → filter → index | `int` (nb indexés) |

---

## 13. Commandes du TP

### Créer les index avec mapping optimisé

```bash
python -m src.utils.mappings
```

Crée les 3 index (`arxiv-papers`, `hn-items`, `arxiv-hn-links`) avec leurs mappings explicites.

### Réindexer les articles avec transformation

```bash
python -m src.ingestion.reindex_arxiv
```

Copie les documents de `arxiv-papers-raw` vers `arxiv-papers` en parsant les auteurs et en appliquant le nouveau mapping.

### Lancer le poller HN

```bash
python -m src.ingestion.hn_poller                # Un seul cycle
python -m src.ingestion.hn_poller --continuous    # Boucle toutes les 60s
python -m src.ingestion.hn_poller --interval 30   # Boucle toutes les 30s
```

### Vérifier les résultats

```bash
# Compter les documents dans chaque index
curl -s http://localhost:9200/arxiv-papers/_count | python -m json.tool
curl -s http://localhost:9200/hn-items/_count | python -m json.tool

# Vérifier le mapping
curl -s http://localhost:9200/arxiv-papers/_mapping | python -m json.tool

# Tester l'analyseur
curl -s -X POST http://localhost:9200/arxiv-papers/_analyze \
  -H 'Content-Type: application/json' \
  -d '{"analyzer": "scientific_english", "text": "CNN for image recognition"}'
```

---

## 14. Récapitulatif des 3 index créés

| Index | Créé par | Documents | Mapping clé |
|-------|----------|-----------|-------------|
| `arxiv-papers` | `mappings.py` + `reindex_arxiv.py` | ~100 000 articles | Analyseur `scientific_english`, multi-fields, nested authors |
| `hn-items` | `mappings.py` + `hn_poller.py` | Variable (polling continu) | Join field `relation` (story ↔ comment), date en `epoch_second` |
| `arxiv-hn-links` | `mappings.py` + `hn_poller.py` | Variable (liens détectés) | Liaison ArXiv↔HN avec scores et timestamps |
