# Jour 1 — Après-midi
# TP : Pipeline d'ingestion batch ArXiv

> **Durée** : 2h45  
> **Niveau** : débutant — chaque étape est détaillée, avec les commandes exactes à exécuter  
> **Prérequis** : avoir complété le TP du matin (stack Docker lancée, données ArXiv dans `data/arxiv-raw/`)

---

## Ce que vous allez construire

À la fin de ce TP, vous aurez :

1. Un pipeline Logstash fonctionnel qui ingère 100 000 articles ArXiv dans Elasticsearch.
2. Un script Python alternatif qui fait la même chose avec `elasticsearch-py`.
3. Validé les résultats dans Kibana Discover.
4. Compris les différences entre les deux approches.

---

## Vérifications préalables (5 min)

Avant de commencer, assurons-nous que tout est en ordre. Ouvrez un terminal à la racine du projet `scipulse/`.

### La stack Docker tourne-t-elle ?

```bash
docker compose ps
```

Vous devez voir au minimum ces services en `running` ou `healthy` :

```
NAME                    STATUS
scipulse-es             running (healthy)
scipulse-logstash       running
scipulse-kibana         running
scipulse-postgres       running (healthy)
scipulse-minio          running
```

Si ce n'est pas le cas, relancez la stack :

```bash
docker compose up -d
```

Attendez ~30 secondes, puis revérifiez.

### Elasticsearch répond-il ?

```bash
curl -s http://localhost:9200/_cluster/health | python -m json.tool
```

Vous devez voir `"status": "green"` (ou `"yellow"`, c'est normal en single-node).

**Si ça ne fonctionne pas** : attendez encore un peu. Elasticsearch peut mettre jusqu'à 60 secondes à démarrer complètement.

### Les données ArXiv sont-elles présentes ?

```bash
ls -lh data/arxiv-raw/
```

Vous devez voir au moins un fichier `.json` (par exemple `arxiv-cs-subset-100k.json`).

**Si le fichier n'est pas là** : retournez à l'étape 4 du TP du matin pour télécharger le dataset.

### Combien d'articles avons-nous ?

```bash
wc -l data/arxiv-raw/arxiv-cs-subset-100k.json
```

Vous devez voir un nombre proche de 100 000. Chaque ligne du fichier est un article au format JSON.

Regardons un article pour nous rafraîchir la mémoire :

```bash
head -1 data/arxiv-raw/arxiv-cs-subset-100k.json | python -m json.tool
```

Repérez les champs : `id`, `title`, `abstract`, `authors`, `categories`, `update_date`. Ce sont ces champs que nous allons parser et indexer.

---

# PARTIE A — Pipeline Logstash (1h15)

## Étape 1 — Comprendre le pipeline fourni (15 min)

Le starter kit contient déjà un pipeline Logstash dans `docker/logstash/pipeline/arxiv.conf`. Ouvrez ce fichier dans votre éditeur :

```bash
# Avec VS Code :
code docker/logstash/pipeline/arxiv.conf

# Ou avec nano :
nano docker/logstash/pipeline/arxiv.conf
```

Prenons le temps de lire chaque bloc en détail. Ne sautez pas cette étape — comprendre ce fichier est l'objectif principal du TP.

### Le bloc `input`

```ruby
input {
  file {
    path => "/data/arxiv-raw/*.json"
    start_position => "beginning"
    sincedb_path => "/dev/null"
    codec => "json"
    mode => "read"
    file_completed_action => "log"
    file_completed_log_path => "/dev/null"
  }
}
```

Ligne par ligne :

| Ligne | Ce qu'elle fait | Pourquoi |
|-------|----------------|----------|
| `path => "/data/arxiv-raw/*.json"` | Lit tous les fichiers `.json` dans ce dossier | Le chemin est celui **à l'intérieur du conteneur Docker** — le volume monté dans `docker-compose.yml` mappe `./data` vers `/data` |
| `start_position => "beginning"` | Commence la lecture au début du fichier | Par défaut, Logstash ne lit que les nouvelles lignes (comme `tail -f`). Pour un chargement batch initial, on veut tout lire depuis le début |
| `sincedb_path => "/dev/null"` | Désactive le suivi de position | Logstash mémorise normalement où il en est dans chaque fichier. En mettant `/dev/null`, on force la relecture complète à chaque redémarrage. Pratique en développement |
| `codec => "json"` | Interprète chaque ligne comme du JSON | Sans ce codec, chaque ligne serait traitée comme du texte brut dans un champ `message` |
| `mode => "read"` | Lit le fichier puis s'arrête | Alternative : `"tail"` pour surveiller le fichier en continu (comme `tail -f`) |
| `file_completed_action => "log"` | Quand le fichier est entièrement lu, loguer un message | Ne pas supprimer ni déplacer le fichier source |

### Le bloc `filter`

```ruby
filter {
  mutate {
    rename => {
      "id"          => "arxiv_id"
      "update_date" => "date_updated"
    }
    remove_field => ["@version", "host", "path", "log", "event"]
  }

  date {
    match => ["date_updated", "yyyy-MM-dd"]
    target => "date_updated"
  }

  mutate {
    split => { "categories" => " " }
  }

  ruby {
    code => '
      cats = event.get("categories")
      if cats.is_a?(Array) && cats.length > 0
        event.set("primary_category", cats[0])
      end
    '
  }

  mutate {
    gsub => ["abstract", "\n", " "]
    strip => ["abstract", "title"]
  }
}
```

Chaque filtre s'exécute dans l'ordre, de haut en bas. Suivons le parcours d'un article :

**Filtre 1 — `mutate` (renommage et nettoyage)**

- `rename` : le champ `id` du JSON ArXiv est renommé en `arxiv_id` (plus explicite et évite la collision avec le `_id` interne d'Elasticsearch). De même, `update_date` devient `date_updated`.
- `remove_field` : supprime les métadonnées ajoutées automatiquement par Logstash (`@version`, `host`, `path`…). Ces champs polluent l'index sans apporter de valeur pour notre cas d'usage.

**Filtre 2 — `date` (parsing de la date)**

Le champ `date_updated` arrive comme une chaîne de texte `"2023-02-07"`. Ce filtre le convertit en objet date qu'Elasticsearch pourra exploiter pour les requêtes temporelles (range, histogrammes, tri par date).

- `match` : le format attendu. Logstash utilise la syntaxe Joda Time (`yyyy-MM-dd`).
- `target` : le champ où stocker la date parsée. On écrase le champ d'origine.

**Filtre 3 — `mutate` (éclatement des catégories)**

Le champ `categories` arrive sous forme `"cs.AI cs.LG stat.ML"` — une seule chaîne avec des catégories séparées par des espaces. Le `split` la transforme en tableau `["cs.AI", "cs.LG", "stat.ML"]`.

Pourquoi un tableau ? Parce que dans Elasticsearch, un champ `keyword` multi-valué permet de filtrer et d'agréger par catégorie : « tous les articles de cs.AI », « combien d'articles par catégorie », etc.

**Filtre 4 — `ruby` (extraction de la catégorie primaire)**

Ce petit script Ruby prend le premier élément du tableau `categories` et le stocke dans un nouveau champ `primary_category`. C'est utile pour les visualisations où on veut « la » catégorie principale d'un article.

Pourquoi Ruby et pas un autre filtre ? Parce qu'il n'existe pas de filtre Logstash natif pour « prendre le premier élément d'un tableau ». Le filtre `ruby` est le recours pour les opérations qui dépassent les capacités des filtres standard.

**Filtre 5 — `mutate` (nettoyage du texte)**

- `gsub` : remplace les retours à la ligne (`\n`) par des espaces dans le champ `abstract`. Les abstracts ArXiv contiennent souvent des sauts de ligne indésirables qui gêneraient l'affichage et la recherche.
- `strip` : supprime les espaces en début et fin des champs `abstract` et `title`.

### Le bloc `output`

```ruby
output {
  elasticsearch {
    hosts => ["http://elasticsearch:9200"]
    index => "arxiv-papers-raw"
    document_id => "%{arxiv_id}"
    action => "index"
  }
}
```

| Ligne | Ce qu'elle fait | Pourquoi |
|-------|----------------|----------|
| `hosts` | Adresse d'Elasticsearch | On utilise le nom du service Docker (`elasticsearch`), pas `localhost` — le conteneur Logstash accède à ES via le réseau Docker interne |
| `index` | Nom de l'index de destination | Suffixé `-raw` pour indiquer que c'est la version brute, avant le mapping optimisé |
| `document_id` | Identifiant unique du document | En utilisant `arxiv_id`, on rend l'ingestion **idempotente** : réindexer un article existant l'écrase au lieu de créer un doublon |
| `action` | Type d'opération | `index` = créer ou remplacer. `create` échouerait si le document existe |

---

## Étape 2 — Lancer l'ingestion Logstash (20 min)

### 2.1. Vérifier que l'index n'existe pas encore

Dans un terminal :

```bash
curl -s http://localhost:9200/arxiv-papers-raw/_count 2>/dev/null || echo "Index inexistant (c'est normal)"
```

Si l'index existe déjà (par exemple d'un essai précédent), supprimez-le pour repartir de zéro :

```bash
curl -X DELETE http://localhost:9200/arxiv-papers-raw
```

### 2.2. S'assurer que Logstash voit les fichiers

Le problème le plus courant est que Logstash ne trouve pas les fichiers. Vérifions que le volume Docker est correctement monté.

Regardons la section Logstash dans `docker-compose.yml` :

```yaml
logstash:
  volumes:
    - ./data:/data:ro
```

Cela signifie que le dossier `./data` de votre machine est monté en lecture seule (`ro`) sur `/data` dans le conteneur. Le pipeline Logstash lit `/data/arxiv-raw/*.json` — ce qui correspond à `./data/arxiv-raw/*.json` sur votre machine.

Vérifions depuis l'intérieur du conteneur :

```bash
docker exec scipulse-logstash ls -lh /data/arxiv-raw/
```

Vous devez voir votre fichier JSON. Si le dossier est vide, c'est que le fichier n'est pas au bon endroit sur votre machine — vérifiez `data/arxiv-raw/`.

### 2.3. Redémarrer Logstash pour déclencher l'ingestion

Logstash a démarré avec le `docker compose up` du matin, mais il n'a peut-être pas trouvé de fichiers à ce moment-là (si vous avez téléchargé les données après). Redémarrons-le :

```bash
docker compose restart logstash
```

### 2.4. Suivre la progression

Ouvrez un second terminal et suivez les logs de Logstash en temps réel :

```bash
docker compose logs -f logstash
```

Vous devriez voir des messages comme :

```
[INFO ] Pipeline started successfully
[INFO ] Starting file input {:path=>"/data/arxiv-raw/*.json"}
```

**Combien de temps ça prend ?** L'ingestion de 100 000 articles prend généralement entre 3 et 10 minutes selon la puissance de votre machine. Logstash n'affiche pas de barre de progression par défaut.

### 2.5. Surveiller le nombre de documents indexés

Pendant que Logstash travaille, ouvrez un troisième terminal et lancez cette commande qui vérifie le compteur toutes les 10 secondes :

```bash
while true; do
  COUNT=$(curl -s http://localhost:9200/arxiv-papers-raw/_count | python -c "import sys,json; print(json.load(sys.stdin).get('count',0))" 2>/dev/null)
  echo "$(date +%H:%M:%S) — $COUNT documents indexés"
  sleep 10
done
```

Appuyez sur `Ctrl+C` pour arrêter la boucle quand le compteur n'augmente plus.

Vous devriez voir le compteur monter progressivement :

```
14:32:10 — 0 documents indexés
14:32:20 — 4523 documents indexés
14:32:30 — 12847 documents indexés
...
14:38:50 — 99872 documents indexés
14:39:00 — 100000 documents indexés
14:39:10 — 100000 documents indexés     ← stabilisé, c'est terminé
```

### 2.6. Vérification finale

Une fois le compteur stabilisé :

```bash
curl -s http://localhost:9200/arxiv-papers-raw/_count | python -m json.tool
```

Résultat attendu :

```json
{
    "count": 100000,
    "_shards": {
        "total": 1,
        "successful": 1,
        "failed": 0
    }
}
```

Si le compte est à 0 et que Logstash ne montre pas d'erreur, le problème est probablement le `sincedb`. Logstash croit avoir déjà lu le fichier. La solution :

```bash
# Forcer la relecture complète
docker compose restart logstash
```

Notre pipeline a `sincedb_path => "/dev/null"`, donc chaque redémarrage force une relecture. Si le fichier a été ajouté après le premier démarrage de Logstash, un simple restart suffit.

---

## Étape 3 — Explorer les données dans Kibana Discover (15 min)

Maintenant que les données sont indexées, explorons-les visuellement dans Kibana.

### 3.1. Créer un Data View

Ouvrez http://localhost:5601 dans votre navigateur.

1. Cliquez sur le menu hamburger (☰) en haut à gauche.
2. Allez dans **Management** > **Stack Management**.
3. Dans le menu de gauche, cliquez sur **Kibana** > **Data Views**.
4. Cliquez sur **Create data view**.
5. Remplissez :
   - **Name** : `arxiv-papers-raw`
   - **Index pattern** : `arxiv-papers-raw`
   - **Timestamp field** : `date_updated`
6. Cliquez sur **Save data view to Kibana**.

### 3.2. Ouvrir Discover

1. Cliquez sur le menu hamburger (☰).
2. Allez dans **Analytics** > **Discover**.
3. En haut à gauche, vérifiez que le data view sélectionné est `arxiv-papers-raw`.
4. **Élargissez la plage temporelle** : en haut à droite, cliquez sur le sélecteur de dates et choisissez **Last 30 years** (les articles ArXiv remontent à 1991).

Vous devriez voir un histogramme de la distribution temporelle et la liste des documents en dessous.

### 3.3. Inspecter un document

Cliquez sur le petit chevron `>` à gauche de n'importe quel document pour l'ouvrir. Vérifiez que les champs sont correctement parsés :

| Champ | Valeur attendue | ✅ ou ❌ |
|-------|----------------|---------|
| `arxiv_id` | Une chaîne type `"2301.07041"` | |
| `title` | Titre de l'article, sans espaces superflus | |
| `abstract` | Texte long, sans retours à la ligne (`\n`) | |
| `categories` | Un **tableau** (pas une chaîne), ex. `["cs.AI", "cs.LG"]` | |
| `primary_category` | Une seule catégorie, ex. `"cs.AI"` | |
| `date_updated` | Un objet date (pas une chaîne), ex. `Feb 7, 2023` | |
| `authors` | Chaîne brute des auteurs | |

**Si `categories` est une chaîne au lieu d'un tableau** : le filtre `split` n'a pas fonctionné. Vérifiez que le fichier `arxiv.conf` est bien monté dans le conteneur (`docker exec scipulse-logstash cat /usr/share/logstash/pipeline/arxiv.conf`).

**Si `date_updated` est une chaîne au lieu d'une date** : le filtre `date` n'a pas matché. Vérifiez le format de date dans vos données (`head -1 data/arxiv-raw/arxiv-cs-subset-100k.json | python -c "import sys,json; print(json.load(sys.stdin)['update_date'])")`).

### 3.4. Premières explorations

Maintenant, amusons-nous un peu. Dans la barre de recherche de Discover (la barre KQL en haut), essayez ces requêtes :

**Recherche simple** : tapez un terme et appuyez sur Entrée.

```
transformer
```

Cela cherche le mot « transformer » dans tous les champs texte. Observez combien de résultats apparaissent.

**Recherche sur un champ précis** :

```
title : "attention mechanism"
```

Cela ne cherche que dans le titre. Comparez le nombre de résultats avec la recherche précédente.

**Filtre par catégorie** :

```
primary_category : "cs.AI"
```

Observez comment l'histogramme temporel change — vous voyez l'évolution des publications en cs.AI au fil du temps.

**Combinaison** :

```
primary_category : "cs.AI" and abstract : "reinforcement learning"
```

Articles d'IA qui parlent de reinforcement learning dans leur abstract.

### 3.5. Ajouter des colonnes

Par défaut, Discover montre le document brut. Pour une vue plus lisible :

1. Dans le panneau de gauche (liste des champs), survolez `title` et cliquez sur le `+` pour l'ajouter comme colonne.
2. Faites de même pour `primary_category` et `date_updated`.
3. Supprimez la colonne `_source` en cliquant sur le `×` dans l'en-tête de colonne.

Vous obtenez un tableau lisible avec titre, catégorie et date.

---

# PARTIE B — Pipeline Python alternatif (1h)

## Pourquoi un pipeline Python ?

Logstash est excellent pour du parsing déclaratif (configuration plutôt que code), mais il a des limites :

- Difficile à tester unitairement.
- Logique complexe = code Ruby dans une config, peu lisible.
- Consommation mémoire élevée (~500 Mo – 1 Go).
- Écosystème Python plus familier pour les data engineers.

Le pipeline Python est l'approche complémentaire : plus de contrôle, plus de testabilité, mais plus de code à écrire.

---

## Étape 4 — Comprendre l'API `elasticsearch-py` (10 min)

La bibliothèque `elasticsearch-py` est le client Python officiel pour Elasticsearch. Elle expose deux niveaux d'API :

**Niveau bas** — opérations unitaires :

```python
from elasticsearch import Elasticsearch

es = Elasticsearch("http://localhost:9200")

# Indexer un document
es.index(index="test", id="1", document={"title": "Hello"})

# Chercher
es.search(index="test", query={"match": {"title": "Hello"}})

# Compter
es.count(index="test")
```

**Niveau haut** — opérations en masse (bulk) :

```python
from elasticsearch import helpers

# Indexer des milliers de documents d'un coup
actions = [
    {"_index": "test", "_id": "1", "_source": {"title": "Article 1"}},
    {"_index": "test", "_id": "2", "_source": {"title": "Article 2"}},
    # ...
]

helpers.bulk(es, actions)
```

L'API `bulk` est **indispensable** pour l'ingestion de volume. Indexer 100 000 documents un par un prendrait des heures (une requête HTTP par document). En bulk, on envoie des lots de plusieurs centaines de documents à la fois — c'est 10 à 100 fois plus rapide.

---

## Étape 5 — Écrire le script d'ingestion Python (30 min)

Créez le fichier `src/ingestion/arxiv_bulk_ingest.py`. Nous allons l'écrire ensemble, bloc par bloc.

### 5.1. Imports et configuration

Créez le fichier et commencez par les imports :

```bash
# Avec VS Code :
code src/ingestion/arxiv_bulk_ingest.py

# Ou avec nano :
nano src/ingestion/arxiv_bulk_ingest.py
```

Écrivez :

```python
"""
SciPulse — Ingestion batch ArXiv via Python + elasticsearch-py
Alternative au pipeline Logstash pour comparer les approches.

Usage :
    python -m src.ingestion.arxiv_bulk_ingest data/arxiv-raw/arxiv-cs-subset-100k.json
"""

import json
import os
import sys
import time

from elasticsearch import Elasticsearch, helpers

# ── Configuration ────────────────────────────────────────────────────────

ES_HOST    = os.getenv("ES_HOST", "http://localhost:9200")
INDEX_NAME = "arxiv-papers-raw-py"   # Index séparé pour comparer avec Logstash
BATCH_SIZE = 1000                    # Nombre de documents par requête bulk
```

**Pourquoi `arxiv-papers-raw-py` et pas `arxiv-papers-raw` ?**

Pour ne pas écraser les données ingérées par Logstash. Comme les deux pipelines produisent le même résultat, on pourra les comparer côte à côte.

### 5.2. Fonction de transformation d'un article

Ajoutez cette fonction qui fait le même travail que les filtres Logstash, mais en Python :

```python
def transform_article(raw: dict) -> dict:
    """
    Transforme un article ArXiv brut en document prêt à indexer.
    Équivalent Python des filtres Logstash (mutate, date, split, ruby).
    """
    # 1. Éclatement des catégories (équivalent du mutate split)

    # 2. Nettoyage de l'abstract (équivalent du mutate gsub + strip)

    # 3. Nettoyage du titre

    # 4. Construction du document transformé

    return doc
```

**Exercice mental** : comparez cette fonction avec les filtres Logstash dans `arxiv.conf`. Chaque opération a son équivalent :

| Logstash | Python |
|----------|--------|
| `mutate { rename => { "id" => "arxiv_id" } }` | `"arxiv_id": raw.get("id")` |
| `mutate { split => { "categories" => " " } }` | `categories_str.split()` |
| `ruby { code => '...' }` | `categories[0] if categories else None` |
| `mutate { gsub => ["abstract", "\n", " "] }` | `abstract.replace("\n", " ")` |
| `mutate { strip => ["abstract", "title"] }` | `.strip()` |

Le code Python est plus lisible et plus facile à tester. Par contre, Logstash ne nécessite aucune ligne de code — uniquement de la configuration.

### 5.3. Générateur d'actions bulk

Ajoutez cette fonction qui lit le fichier JSON ligne par ligne et produit des « actions » bulk :

```python
def generate_actions(filepath: str):
    """
    Générateur qui lit un fichier JSON ligne par ligne et produit des actions bulk ES.
    Utilise un générateur (yield) pour ne pas charger tout le fichier en mémoire.
    """
    with open(filepath, "r", encoding="utf-8") as f:
        for line_number, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue

            # 1. Lire la ligne JSON
            # en traitant les erreurs 
            try:
                pass
            except json.JSONDecodeError as e:
                pass
             
            # 2. Normaliser les données de l'article
            # avec la fonction transform_article   

            # 3. Vérification minimale : l'arxiv_id doit exister
            # Sinon l est simplement ignoré
            
            # Action bulk au format ES
            # Attention ! `generate_actions est un générateur
```

**Points importants à comprendre :**

**Le mot-clé `yield`** : cette fonction est un *générateur*. Au lieu de construire une liste géante de 100 000 actions en mémoire, elle les produit une par une à la demande. C'est crucial pour les gros fichiers — charger 100 000 articles en mémoire d'un coup consommerait plusieurs Go de RAM.

**La gestion des erreurs** : si une ligne du fichier n'est pas du JSON valide, on l'ignore et on continue. En production, on loguerait ces erreurs dans un fichier séparé (dead letter file).

**L'idempotence** : comme avec Logstash, on utilise `arxiv_id` comme `_id` du document. Relancer le script écrase les documents existants au lieu de créer des doublons.

### 5.4. Fonction principale d'ingestion

Ajoutez la fonction qui orchestre le tout :

```python
def ingest(filepath: str):
    """Ingestion bulk du fichier ArXiv dans Elasticsearch."""

    # 1. Connexion à Elasticsearch
    
    # 2. Vérification de la connexion
    try:
        pass
    except Exception as e:
        pass
        
    # 3. Suppression de l'index existant (pour repartir de zéro)

    # 4. Ingestion via l'API bulk
    print(f"\n📥 Ingestion de {filepath} → index '{INDEX_NAME}'")
    print(f"   Taille des lots : {BATCH_SIZE} documents\n")
    
    # 4.1 Variables de traçage (optionnel) : temps, nombre de lots, erreurs

    # 4.2 Ingestion
    # helpers.streaming_bulk gère automatiquement le découpage en lots
    # Pour chaque lot, exécuter la fonction generate_actions
    for ok, result in helpers.streaming_bulk(
        pass

    # 5. Bilan
    elapsed = time.time() - start_time
    rate = success_count / elapsed if elapsed > 0 else 0

    print(f"\n{'═' * 50}")
    print(f"  ✅ Succès  : {success_count:>7,}")
    print(f"  ❌ Erreurs : {error_count:>7,}")
    print(f"  ⏱  Durée   : {elapsed:.1f} secondes")
    print(f"  🚀 Débit   : {rate:,.0f} documents/seconde")
    print(f"{'═' * 50}")

    # 6. Forcer le rafraîchissement de l'index pour que les documents soient cherchables

    # 7. Vérification finale
    count = es.count(index=INDEX_NAME)["count"]
    print(f"\n🔍 Vérification : {count:,} documents dans l'index '{INDEX_NAME}'")
```

**Points importants à comprendre :**

**`helpers.streaming_bulk`** : c'est la version « streaming » de `helpers.bulk`. Elle consomme le générateur progressivement et envoie les documents par lots de `chunk_size`. C'est l'API recommandée pour les gros volumes.

La différence avec `helpers.bulk` (sans `streaming_`) :
- `helpers.bulk` : retourne `(success_count, errors_list)` à la fin. Simple mais ne permet pas de suivre la progression.
- `helpers.streaming_bulk` : retourne un itérateur de résultats `(ok, result)`. Permet un suivi document par document.

**`raise_on_error=False`** : sans cette option, une seule erreur d'indexation planterait tout le script. Avec `False`, les erreurs sont reportées mais le traitement continue. C'est plus robuste.

**`es.indices.refresh`** : par défaut, Elasticsearch ne rend pas les documents cherchables immédiatement — il les bufferise et les rafraîchit toutes les secondes. Le `refresh` force la mise à jour pour que notre vérification finale voie tous les documents.

### 5.5. Point d'entrée du script

Ajoutez à la fin du fichier :

```python
if __name__ == "__main__":
    filepath = sys.argv[1] if len(sys.argv) > 1 else "data/arxiv-raw/arxiv-cs-subset-100k.json"

    if not os.path.exists(filepath):
        print(f"❌ Fichier non trouvé : {filepath}")
        print(f"   Usage : python -m src.ingestion.arxiv_bulk_ingest <chemin_fichier>")
        sys.exit(1)

    ingest(filepath)
```

### 5.6. Fichier complet

Sauvegardez. Votre fichier `src/ingestion/arxiv_bulk_ingest.py` doit faire environ 130 lignes. Vérifiez la syntaxe :

```bash
python -c "import ast; ast.parse(open('src/ingestion/arxiv_bulk_ingest.py').read()); print('✅ Syntaxe OK')"
```

---

## Étape 6 — Lancer l'ingestion Python (10 min)

### 6.1. Exécution

```bash
python -m src.ingestion.arxiv_bulk_ingest data/arxiv-raw/arxiv-cs-subset-100k.json
```

Sortie attendue :

```
✅ Connecté à Elasticsearch (scipulse-cluster)
🗑  Index 'arxiv-papers-raw-py' supprimé

📥 Ingestion de data/arxiv-raw/arxiv-cs-subset-100k.json → index 'arxiv-papers-raw-py'
   Taille des lots : 1000 documents

  📊  10,000 documents traités (3,245 docs/sec)
  📊  20,000 documents traités (3,187 docs/sec)
  📊  30,000 documents traités (3,102 docs/sec)
  ...
  📊 100,000 documents traités (2,890 docs/sec)

══════════════════════════════════════════════════
  ✅ Succès  : 100,000
  ❌ Erreurs :       0
  ⏱  Durée   : 34.6 secondes
  🚀 Débit   : 2,890 documents/seconde
══════════════════════════════════════════════════

🔍 Vérification : 100,000 documents dans l'index 'arxiv-papers-raw-py'
```

### 6.2. Dépannage

| Problème | Solution |
|----------|----------|
| `ConnectionError: Connection refused` | Elasticsearch ne tourne pas → `docker compose up -d elasticsearch` |
| `json.JSONDecodeError` sur beaucoup de lignes | Le fichier n'est pas du JSON lines — vérifiez le format |
| Débit très faible (< 500 docs/sec) | Augmentez `BATCH_SIZE` à 2000 ou 5000 |
| `MemoryError` | Réduisez `BATCH_SIZE` à 500 |

---

## Étape 7 — Comparer les deux approches (10 min)

Nous avons maintenant deux index identiques (ou presque) :

- `arxiv-papers-raw` — ingéré par Logstash
- `arxiv-papers-raw-py` — ingéré par Python

### 7.1. Comparer les compteurs

```bash
echo "=== Logstash ==="
curl -s http://localhost:9200/arxiv-papers-raw/_count | python -m json.tool

echo "=== Python ==="
curl -s http://localhost:9200/arxiv-papers-raw-py/_count | python -m json.tool
```

Les deux doivent afficher le même nombre.

### 7.2. Comparer un document

```bash
# Le même article dans les deux index
ARTICLE_ID="2301.07041"  # Remplacez par un ID présent dans votre dataset

echo "=== Logstash ==="
curl -s "http://localhost:9200/arxiv-papers-raw/_doc/$ARTICLE_ID" | python -m json.tool

echo "=== Python ==="
curl -s "http://localhost:9200/arxiv-papers-raw-py/_doc/$ARTICLE_ID" | python -m json.tool
```

Les champs doivent être identiques (mêmes noms, mêmes valeurs, mêmes types).

### 7.3. Synthèse comparative

Discutons des avantages et inconvénients observés :

| Critère | Logstash | Python |
|---------|----------|--------|
| Lignes de code/config | ~40 lignes de config | ~130 lignes de code |
| Vitesse d'ingestion | Variable (dépend des workers) | ~2 000 – 4 000 docs/sec typiquement |
| Suivi de la progression | Pas natif (juste des logs) | Intégré (compteur, débit) |
| Gestion des erreurs | Logs Logstash (difficile à parser) | Contrôle total (try/except, compteurs) |
| Testabilité | Difficile | Facile (`pytest`) |
| Dépendance | JVM, 500 Mo de RAM | Python, ~50 Mo de RAM |
| Courbe d'apprentissage | Syntaxe Logstash spécifique | Python standard |
| Parsing de logs non-structurés | Excellent (grok) | Fastidieux (regex manuelles) |

**Conclusion** : il n'y a pas de « meilleur » outil dans l'absolu. Logstash excelle pour le parsing de logs et les configurations simples. Python excelle pour la logique métier complexe, les tests et l'intégration dans un écosystème existant. En production, les deux coexistent souvent.

---

## Étape 8 — Commit (5 min)

```bash
git add src/ingestion/arxiv_bulk_ingest.py
git commit -m "feat: add Python bulk ingestion script for ArXiv"
```

---

## Checklist de fin de journée

Vérifiez que vous avez :

- [ ] Compris le fonctionnement du pipeline Logstash (input → filter → output)
- [ ] 100 000 documents dans l'index `arxiv-papers-raw` (Logstash)
- [ ] 100 000 documents dans l'index `arxiv-papers-raw-py` (Python)
- [ ] Exploré les données dans Kibana Discover (data view, recherche KQL, colonnes)
- [ ] Compris les différences entre Logstash et Python pour l'ingestion
- [ ] Commité votre travail dans Git

## Ce qui vient demain

Demain matin (Jour 2), nous allons **optimiser le mapping Elasticsearch** pour la fouille de texte. L'index `arxiv-papers-raw` utilise un mapping auto-généré, qui fait des choix sous-optimaux (par exemple, `categories` en `text` au lieu de `keyword`). Nous créerons un index `arxiv-papers` avec un mapping explicite incluant un analyseur anglais custom, des synonymes scientifiques, et des multi-fields — tout ce qu'il faut pour exploiter pleinement les capacités de recherche d'Elasticsearch.

Nous commencerons aussi l'ingestion du flux **Hacker News** via le script `hn_poller.py`.
