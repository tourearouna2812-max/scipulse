# Mémento — Fonctions utiles pour le TP J1 après-midi
# Ingestion batch ArXiv (Logstash + Python)

> Classement par bibliothèque / module, avec une courte explication et un exemple d'usage tiré du code du TP.

---

## 1. `elasticsearch` — Client Python Elasticsearch

### `Elasticsearch(host)`

Crée une connexion au cluster Elasticsearch.

```python
from elasticsearch import Elasticsearch
es = Elasticsearch("http://localhost:9200")
```

### `es.info()`

Retourne les informations du cluster (nom, version). Utile pour vérifier que la connexion fonctionne.

```python
info = es.info()
print(info["cluster_name"])    # → "scipulse-cluster"
print(info["version"]["number"])  # → "8.13.4"
```

### `es.index(index, id, document)`

Indexe un seul document. Crée le document s'il n'existe pas, le remplace s'il existe (quand `id` est fourni).

```python
es.index(index="arxiv-papers-raw", id="2301.07041", document={"title": "Mon article"})
```

### `es.get(index, id)`

Récupère un document par son identifiant. Lève une exception `NotFoundError` si le document n'existe pas.

```python
result = es.get(index="arxiv-papers-raw", id="2301.07041")
doc = result["_source"]
```

### `es.search(index, query, size, aggs, highlight, sort)`

Exécute une requête de recherche. Retourne les documents matchés, les agrégations, et les highlights.

```python
result = es.search(
    index="arxiv-papers-raw",
    size=5,
    query={"multi_match": {"query": "transformer", "fields": ["title^3", "abstract"]}},
    highlight={"fields": {"abstract": {"fragment_size": 150}}},
)
total = result["hits"]["total"]["value"]
hits  = result["hits"]["hits"]           # Liste de documents
```

### `es.count(index)`

Retourne le nombre de documents dans un index.

```python
count = es.count(index="arxiv-papers-raw")["count"]  # → 100000
```

### `es.indices.exists(index)`

Vérifie si un index existe. Retourne `True` ou `False`.

```python
if es.indices.exists(index="arxiv-papers-raw"):
    print("L'index existe")
```

### `es.indices.delete(index)`

Supprime un index et toutes ses données. Irréversible.

```python
es.indices.delete(index="arxiv-papers-raw-py")
```

### `es.indices.refresh(index)`

Force Elasticsearch à rendre les documents récemment indexés cherchables immédiatement. Sans ce refresh, il y a un délai d'environ 1 seconde (le refresh automatique par défaut).

```python
es.indices.refresh(index="arxiv-papers-raw-py")
```

### `es.indices.stats(index)`

Retourne des statistiques détaillées sur un index (taille en octets, nombre de documents, segments, etc.).

```python
stats = es.indices.stats(index="arxiv-papers-raw")
size_bytes = stats["indices"]["arxiv-papers-raw"]["total"]["store"]["size_in_bytes"]
```

### `es.indices.get_mapping(index)`

Retourne le mapping complet d'un index (types des champs, analyseurs, etc.).

```python
mapping = es.indices.get_mapping(index="arxiv-papers-raw")
properties = mapping["arxiv-papers-raw"]["mappings"]["properties"]
```

### `es.cat.indices(format)`

Liste tous les index du cluster avec leurs statistiques sommaires.

```python
indices = es.cat.indices(format="json")
for idx in indices:
    print(f"{idx['index']} — {idx['docs.count']} docs")
```

---

## 2. `elasticsearch.helpers` — Opérations en masse (bulk)

### `helpers.streaming_bulk(es, actions, chunk_size, raise_on_error)`

Indexe des documents en masse par lots. Consomme un générateur d'actions et les envoie à ES par paquets de `chunk_size`. Retourne un itérateur `(ok, result)` pour chaque document, ce qui permet de suivre la progression.

```python
from elasticsearch import helpers

for ok, result in helpers.streaming_bulk(
    es,
    generate_actions("data.json"),
    chunk_size=1000,
    raise_on_error=False,   # Ne pas planter sur les erreurs individuelles
):
    if ok:
        success += 1
    else:
        errors += 1
```

**Différence avec `helpers.bulk`** : `bulk` retourne `(success_count, errors_list)` à la fin — plus simple mais sans suivi de progression. `streaming_bulk` retourne un itérateur — permet d'afficher un compteur pendant l'ingestion.

### `helpers.bulk(es, actions, chunk_size, raise_on_error)`

Version simplifiée de `streaming_bulk`. Retourne directement le compteur de succès et la liste des erreurs.

```python
success, errors = helpers.bulk(es, actions, chunk_size=500, raise_on_error=False)
```

### Format d'une action bulk

Chaque action est un dictionnaire avec `_index`, `_id` et `_source` :

```python
action = {
    "_index":  "arxiv-papers-raw-py",
    "_id":     "2301.07041",          # Clé de déduplication → idempotence
    "_source": {"title": "...", "abstract": "...", ...}
}
```

---

## 3. `json` — Parsing JSON (bibliothèque standard)

### `json.loads(string)`

Parse une chaîne JSON en dictionnaire Python. Lève `json.JSONDecodeError` si le JSON est invalide.

```python
import json
raw = json.loads('{"id": "2301.07041", "title": "Mon article"}')
print(raw["id"])  # → "2301.07041"
```

### `json.load(file_object)`

Comme `json.loads` mais lit directement depuis un fichier ouvert. **Attention** : charge tout le fichier en mémoire d'un coup. Pour un fichier de 100 000 lignes JSON lines, préférer `json.loads` ligne par ligne.

```python
with open("data.json") as f:
    data = json.load(f)  # ⚠️ Uniquement pour les petits fichiers
```

---

## 4. `os` — Interaction avec le système (bibliothèque standard)

### `os.getenv(name, default)`

Lit une variable d'environnement. Retourne `default` si la variable n'est pas définie. Utilisé pour la dualité hôte/conteneur Docker.

```python
import os
ES_HOST = os.getenv("ES_HOST", "http://localhost:9200")
# Depuis l'hôte : pas de variable → "http://localhost:9200"
# Depuis Docker  : ES_HOST=http://elasticsearch:9200 → utilise cette valeur
```

### `os.path.exists(path)`

Vérifie si un fichier ou dossier existe. Retourne `True` ou `False`.

```python
if not os.path.exists("data/arxiv-raw/arxiv-cs-subset-100k.json"):
    print("Fichier non trouvé !")
```

### `os.path.getsize(path)`

Retourne la taille d'un fichier en octets.

```python
size_mb = os.path.getsize("data.json") / (1024 * 1024)
```

### `os.makedirs(path, exist_ok)`

Crée un répertoire et tous ses parents. Avec `exist_ok=True`, ne lève pas d'erreur si le dossier existe déjà.

```python
os.makedirs("data/arxiv-raw", exist_ok=True)
```

---

## 5. `sys` — Paramètres du programme (bibliothèque standard)

### `sys.argv`

Liste des arguments passés au script en ligne de commande. `sys.argv[0]` est le nom du script, `sys.argv[1]` le premier argument.

```python
import sys
filepath = sys.argv[1] if len(sys.argv) > 1 else "data/arxiv-raw/default.json"
```

### `sys.exit(code)`

Quitte le programme avec un code de sortie. `0` = succès, `1` = erreur.

```python
sys.exit(1)  # Quitter en signalant une erreur
```

---

## 6. `argparse` — Parsing des arguments en ligne de commande (bibliothèque standard)

### `argparse.ArgumentParser(description, epilog, formatter_class)`

Crée un parseur d'arguments avec une description et un texte d'aide.

```python
import argparse
parser = argparse.ArgumentParser(
    description="SciPulse — Ingestion ArXiv",
    formatter_class=argparse.RawDescriptionHelpFormatter,
    epilog="Exemples :\n  python script.py data.json\n  python script.py --max 50000",
)
```

### `parser.add_argument(name, type, default, help, nargs)`

Déclare un argument attendu. Peut être positionnel (sans `--`) ou optionnel (avec `--`).

```python
parser.add_argument("filepath", nargs="?", default="data.json", help="Fichier JSON")
parser.add_argument("--batch-size", type=int, default=1000, help="Taille des lots")
parser.add_argument("--categories", default="cs. stat.ML", help="Préfixes de catégories")
```

### `parser.parse_args()`

Parse les arguments et retourne un objet avec les valeurs.

```python
args = parser.parse_args()
print(args.filepath)      # → "data.json"
print(args.batch_size)    # → 1000
```

---

## 7. `time` — Mesure du temps (bibliothèque standard)

### `time.time()`

Retourne le timestamp Unix en secondes (float). Utilisé pour mesurer les durées.

```python
import time
start = time.time()
# ... travail ...
elapsed = time.time() - start
print(f"Durée : {elapsed:.1f} secondes")
```

---

## 8. `textwrap` — Mise en forme de texte (bibliothèque standard)

### `textwrap.fill(text, width, initial_indent, subsequent_indent)`

Découpe un texte long en lignes de largeur maximale `width`, avec indentation optionnelle.

```python
import textwrap
wrapped = textwrap.fill(
    abstract[:300],
    width=55,
    initial_indent="  │ ",
    subsequent_indent="  │ ",
)
print(wrapped)
```

---

## 9. Fonctions Python natives utilisées

### `str.split(sep)`

Découpe une chaîne en liste, en utilisant `sep` comme séparateur. Sans argument, découpe sur les espaces.

```python
"cs.AI cs.LG stat.ML".split()       # → ["cs.AI", "cs.LG", "stat.ML"]
"cs.AI cs.LG stat.ML".split(" ")    # → idem
```

### `str.replace(old, new)`

Remplace toutes les occurrences de `old` par `new`.

```python
abstract = abstract.replace("\n", " ")    # Sauts de ligne → espaces
```

### `str.strip()`

Supprime les espaces en début et fin de chaîne.

```python
title = "  A Great Paper  ".strip()    # → "A Great Paper"
```

### `str.startswith(prefix)`

Vérifie si la chaîne commence par `prefix`. Accepte aussi un tuple de préfixes.

```python
"cs.AI".startswith("cs.")       # → True
"cs.AI".startswith(("cs.", "stat."))  # → True (l'un des préfixes)
```

### `dict.get(key, default)`

Récupère une valeur du dictionnaire. Retourne `default` si la clé n'existe pas (au lieu de lever `KeyError`).

```python
raw.get("doi")                # → None si "doi" absent
raw.get("categories", "")    # → "" si "categories" absent
```

### `enumerate(iterable, start)`

Ajoute un compteur à chaque élément d'un itérable. Indispensable pour afficher un numéro de ligne lors du parcours d'un fichier.

```python
for line_number, line in enumerate(f, 1):
    print(f"Ligne {line_number} : {line}")
```

### `open(path, mode, encoding)`

Ouvre un fichier. `"r"` pour lecture, `"w"` pour écriture. Toujours spécifier `encoding="utf-8"` pour les données internationales.

```python
with open("data.json", "r", encoding="utf-8") as f:
    for line in f:
        ...
```

---

## 10. Pattern « générateur » (yield)

Le mot-clé `yield` transforme une fonction en **générateur** : au lieu de retourner une liste complète, elle produit les éléments un par un à la demande. C'est le pattern central du script d'ingestion — il évite de charger 100 000 articles en mémoire.

```python
def generate_actions(filepath):
    with open(filepath) as f:
        for line in f:
            raw = json.loads(line)
            doc = transform_article(raw)
            yield {                        # ← yield au lieu de append dans une liste
                "_index": "arxiv-papers-raw-py",
                "_id":    doc["arxiv_id"],
                "_source": doc,
            }
```

Ce générateur est ensuite consommé par `helpers.streaming_bulk` qui tire les actions une par une, les regroupe en lots de `chunk_size`, et les envoie à ES. À aucun moment les 100 000 actions ne sont toutes en mémoire en même temps.

---

## 11. Logstash — Plugins utilisés dans `arxiv.conf`

### Plugin `input > file`

| Paramètre | Valeur | Rôle |
|-----------|--------|------|
| `path` | `"/data/arxiv-raw/*.json"` | Chemin des fichiers à lire (dans le conteneur Docker) |
| `start_position` | `"beginning"` | Lire depuis le début du fichier (pas la fin) |
| `sincedb_path` | `"/dev/null"` | Désactiver le suivi de position (relecture complète à chaque restart) |
| `codec` | `"json"` | Interpréter chaque ligne comme un objet JSON |
| `mode` | `"read"` | Lire le fichier en entier puis s'arrêter |
| `file_completed_action` | `"log"` | Après lecture complète, loguer un message (ne pas supprimer le fichier) |

### Plugin `filter > mutate`

| Opération | Syntaxe | Équivalent Python |
|-----------|---------|-------------------|
| `rename` | `rename => { "id" => "arxiv_id" }` | `doc["arxiv_id"] = raw["id"]` |
| `remove_field` | `remove_field => ["@version", "host"]` | (pas de champ équivalent en Python — on ne les crée pas) |
| `split` | `split => { "categories" => " " }` | `categories.split()` |
| `gsub` | `gsub => ["abstract", "\n", " "]` | `abstract.replace("\n", " ")` |
| `strip` | `strip => ["abstract", "title"]` | `abstract.strip()` |
| `convert` | `convert => { "score" => "integer" }` | `int(raw["score"])` |
| `add_field` | `add_field => { "pipeline" => "batch" }` | `doc["pipeline"] = "batch"` |

### Plugin `filter > date`

Parse une chaîne en objet date Elasticsearch.

```ruby
date {
  match  => ["date_updated", "yyyy-MM-dd"]    # Format attendu
  target => "date_updated"                     # Champ de destination (écrase l'original)
}
```

Équivalent Python : pas besoin — Elasticsearch parse automatiquement les chaînes ISO 8601 si le mapping déclare le type `date`.

### Plugin `filter > ruby`

Exécute du code Ruby arbitraire pour les transformations impossibles avec les filtres natifs.

```ruby
ruby {
  code => '
    cats = event.get("categories")
    if cats.is_a?(Array) && cats.length > 0
      event.set("primary_category", cats[0])
    end
  '
}
```

Équivalent Python : `categories[0] if categories else None`

### Plugin `output > elasticsearch`

| Paramètre | Valeur | Rôle |
|-----------|--------|------|
| `hosts` | `["http://elasticsearch:9200"]` | Adresse ES (nom Docker, pas localhost) |
| `index` | `"arxiv-papers-raw"` | Index de destination |
| `document_id` | `"%{arxiv_id}"` | Clé d'idempotence (pas de doublons au réindexage) |
| `action` | `"index"` | Créer ou remplacer le document |

---

## 12. Commandes shell utilisées dans `monitor_ingestion.sh`

### `curl -s URL`

Requête HTTP silencieuse (sans barre de progression). `-s` = silent.

```bash
curl -s http://localhost:9200/arxiv-papers-raw/_count
```

### `python3 -c "code"`

Exécute une ligne de Python en one-liner. Utilisé dans les scripts shell pour parser du JSON.

```bash
curl -s http://localhost:9200/arxiv-papers-raw/_count | \
  python3 -c "import sys,json; print(json.load(sys.stdin).get('count',0))"
```

### `docker exec -it conteneur commande`

Exécute une commande à l'intérieur d'un conteneur Docker en cours d'exécution.

```bash
docker exec scipulse-logstash ls /data/arxiv-raw/       # Lister les fichiers dans le conteneur
docker exec scipulse-logstash cat /usr/share/logstash/pipeline/arxiv.conf  # Lire la config
```

### `docker compose restart service`

Redémarre un service Docker Compose sans toucher aux autres.

```bash
docker compose restart logstash    # Relance l'ingestion
```

### `docker compose logs -f service`

Affiche les logs d'un service en temps réel. `-f` = follow (comme `tail -f`).

```bash
docker compose logs -f logstash    # Suivre l'ingestion en direct
```
