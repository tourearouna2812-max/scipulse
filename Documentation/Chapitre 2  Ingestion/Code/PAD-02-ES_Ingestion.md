# Jour 1 — Après-midi
# Bloc théorique : Ingestion et collecte de données

> **Durée** : 45 minutes  
> **Positionnement** : première étape du cycle de vie de la donnée  
> **Objectif** : comprendre les patterns d'ingestion, l'architecture de Logstash, le rôle de Filebeat, et savoir choisir le bon outil selon le contexte

## 1. L'ingestion dans le cycle de vie de la donnée

### Définition

L'ingestion est le processus par lequel les données sont **collectées depuis des sources hétérogènes** et **acheminées vers un système de stockage ou de traitement**. C'est la porte d'entrée du pipeline ; si elle est mal conçue, tout ce qui suit en souffre.

En apparence, l'ingestion semble triviale : « lire des données, les écrire ailleurs ». En pratique, c'est l'une des étapes les plus délicates de l'ingénierie de données, parce qu'elle doit composer avec la réalité désordonnée des sources :

- Des formats hétérogènes (CSV, JSON, XML, logs non structurés, binaires).
- Des débits variables (un dump quotidien de 3 Go vs. un flux continu de 500 événements/seconde).
- Des sources instables (API avec rate limiting, fichiers déposés manuellement, bases de production sous charge).
- Des données sales dès l'origine (encodages cassés, champs manquants, doublons).

Le rôle de la couche d'ingestion est d'**absorber cette complexité** pour que les étapes suivantes (stockage, transformation, indexation) travaillent sur des données propres, homogènes et fiables.

### Positionnement dans l'architecture SciPulse

Dans notre projet, l'ingestion concerne deux flux distincts :

```
  ┌─────────────────────────────────────────────────┐
  │                   SOURCES                        │
  │                                                  │
  │   ArXiv dump (JSON lines, 100k articles)         │
  │   Hacker News API (Firebase, ~500 items/h)       │
  └──────────────┬────────────────┬──────────────────┘
                 │                │
         ┌───────▼──────┐  ┌─────▼───────────┐
         │   LOGSTASH   │  │  PYTHON SCRIPT   │
         │  (batch)     │  │  (micro-batch)   │
         │              │  │                  │
         │ file → json  │  │ requests →       │
         │ → mutate →   │  │ transform →      │
         │ → date →     │  │ bulk index       │
         │ → ES output  │  │                  │
         └───────┬──────┘  └─────┬────────────┘
                 │               │
                 ▼               ▼
         ┌───────────────────────────────┐
         │       ELASTICSEARCH           │
         │  arxiv-papers-raw   hn-items  │
         └───────────────────────────────┘
```

Ce double flux illustre un principe important : **il n'existe pas un seul bon outil d'ingestion**. Le choix dépend de la nature de la source, du débit, de la complexité du parsing, et du contexte opérationnel.

## 2. Les trois patterns d'ingestion

La littérature en ingénierie de données distingue trois grands patterns d'ingestion, qui se différencient par leur **latence** (le délai entre la production de la donnée et sa disponibilité en aval) et leur **modèle de traitement**.

### 2.1. Batch (traitement par lots)

**Principe** : les données sont collectées en bloc à intervalle régulier — toutes les heures, tous les jours, toutes les semaines. Le traitement s'applique à l'intégralité du lot d'un coup.

**Caractéristiques** :

- Latence élevée : minutes à heures entre la production et la disponibilité.
- Volumétrie par exécution potentiellement très grande (Go à To).
- Traitement prévisible et planifiable.
- Gestion des erreurs simple : en cas d'échec, on relance le lot entier.

**Cas d'usage typiques** :

- Chargement d'un dump de base de données (export nightly).
- Traitement d'un fichier CSV déposé par un partenaire.
- Ingestion d'un dataset statique (notre dump ArXiv).
- Rapports périodiques, consolidation comptable.

**Outils courants** : Logstash (plugin `file`), Apache Spark, scripts Python + cron, Airflow pour l'orchestration.

**Dans SciPulse** : le dump ArXiv est un fichier JSON lines statique de 100 000 articles. C'est un cas batch pur — le fichier est disponible en entier, on le traite d'un bloc.

### 2.2. Streaming (traitement en flux continu)

**Principe** : chaque événement est traité individuellement, dès qu'il se produit. Le système fonctionne en continu, sans notion de « lot ».

**Caractéristiques** :

- Latence très faible : millisecondes à secondes.
- Traitement événement par événement.
- Infrastructure plus complexe (brokers de messages, garanties de livraison).
- Gestion des erreurs sophistiquée (dead letter queues, replay, exactly-once semantics).

**Cas d'usage typiques** :

- Détection de fraude en temps réel.
- Monitoring d'infrastructure (métriques serveur, alertes).
- Systèmes de recommandation en temps réel.
- IoT (capteurs, télémétrie).

**Outils courants** : Apache Kafka, Apache Pulsar, Amazon Kinesis, Apache Flink, Logstash (en mode pipeline continu).

**Dans SciPulse** : nous n'avons pas de streaming pur, mais le concept est important à comprendre pour la culture d'ingénieur data. Kafka ou Pulsar seraient pertinents si nous devions traiter des millions d'événements HN par seconde en temps réel.

### 2.3. Micro-batch (compromis pragmatique)

**Principe** : les données sont collectées en petits lots très fréquents — toutes les secondes, toutes les minutes. C'est un compromis entre la simplicité du batch et la réactivité du streaming.

**Caractéristiques** :

- Latence modérée : secondes à minutes.
- Chaque micro-lot est petit (dizaines à milliers d'enregistrements).
- Infrastructure plus simple que le streaming pur.
- Modèle mental proche du batch (on traite un « petit lot ») mais fréquence élevée.

**Cas d'usage typiques** :

- Polling d'une API REST à intervalle régulier.
- Collecte de logs toutes les 30 secondes.
- Synchronisation incrémentale entre systèmes.
- Agrégation de métriques à la minute.

**Outils courants** : scripts Python + scheduler (Airflow, cron), Logstash (plugin `http_poller`), Filebeat, Apache Spark Structured Streaming (micro-batch natif).

**Dans SciPulse** : le poller Hacker News est un micro-batch. Toutes les 60 secondes, il interroge l'API Firebase, récupère les nouveaux items, et les indexe par lot dans Elasticsearch.

### 2.4. Synthèse comparative

| Critère | Batch | Micro-batch | Streaming |
|---------|-------|-------------|-----------|
| Latence | Minutes → heures | Secondes → minutes | Millisecondes |
| Complexité infra | Faible | Moyenne | Élevée |
| Volume par exécution | Très grand | Petit à moyen | Unitaire |
| Gestion des erreurs | Relancer le lot | Relancer le micro-lot | Dead letter, replay |
| Coût opérationnel | Faible | Moyen | Élevé |
| Exemple SciPulse | Dump ArXiv | Poller HN | (hors scope TP) |

**Le choix n'est pas binaire.** La plupart des architectures de production combinent les trois patterns. C'est ce que Martin Kleppmann appelle l'architecture « Lambda » (batch + streaming en parallèle) ou « Kappa » (tout en streaming, avec replay) dans *Designing Data-Intensive Applications* [1]. Dans SciPulse, nous combinons batch (ArXiv) et micro-batch (HN) — une architecture hybride réaliste.

### 2.5. Push vs. Pull

Orthogonalement aux trois patterns ci-dessus, il existe deux modèles de déclenchement :

**Pull (tirage)** : le système d'ingestion va chercher les données activement. C'est le modèle dominant en batch et micro-batch.

- Logstash surveille un répertoire et lit les nouveaux fichiers.
- Le poller HN interroge l'API toutes les 60 secondes.
- Un script extrait les données d'une base de production via SQL.

**Push (poussée)** : la source envoie les données au système d'ingestion. C'est le modèle dominant en streaming.

- Un webhook appelle une URL à chaque événement.
- Un producteur Kafka publie un message dans un topic.
- Un capteur IoT envoie une mesure via MQTT.

Dans SciPulse, nous sommes en mode pull pour les deux sources : Logstash tire les fichiers depuis MinIO, et le poller Python tire les items depuis l'API HN.

## 3. Architecture de Logstash

### 3.1. Qu'est-ce que Logstash ?

Logstash est un moteur de collecte et de transformation de données open-source, développé par Elastic. Il fait partie de la stack ELK (Elasticsearch, Logstash, Kibana) — renommée « Elastic Stack » depuis l'ajout de Beats.

Initialement conçu pour la collecte de **logs** (d'où son nom), Logstash a évolué pour devenir un outil d'ingestion généraliste capable de traiter tout type de données structurées ou non structurées.

**Ce que Logstash fait bien** :

- Parser et transformer des données hétérogènes sans écrire de code (configuration déclarative).
- Gérer nativement plus de 200 plugins d'entrée, de filtre et de sortie.
- Garantir la livraison des données (persistent queues).
- Gérer le backpressure (ralentir l'ingestion si la destination est saturée).

**Ce que Logstash fait moins bien** :

- Consommation mémoire élevée (JVM, minimum 512 Mo, recommandé 1 Go).
- Transformations complexes (au-delà du parsing, un script Python est souvent plus lisible).
- Scalabilité horizontale (pas de distribution native — il faut déployer plusieurs instances).

### 3.2. Le modèle IPO : Input → Filter → Output

L'architecture de Logstash est fondée sur un modèle simple en trois étapes que la documentation officielle [2] appelle le *pipeline* :

```
┌──────────┐     ┌──────────┐     ┌──────────┐
│  INPUT   │────▶│  FILTER  │────▶│  OUTPUT  │
│          │     │          │     │          │
│ Lecture  │     │ Parsing  │     │ Écriture │
│ des      │     │ Transfo  │     │ vers la  │
│ sources  │     │ Enrichi. │     │ destinat.│
└──────────┘     └──────────┘     └──────────┘
```

Chaque étape est composée de **plugins** configurables. Un pipeline Logstash est défini dans un fichier `.conf` qui déclare ces trois blocs.

### 3.3. Les plugins d'entrée (Input)

Le plugin d'entrée définit **d'où viennent les données**. Logstash en supporte plus de 50. Voici les plus courants :

| Plugin | Usage | Exemple |
|--------|-------|---------|
| `file` | Lire des fichiers locaux (avec suivi de position via sincedb) | Logs applicatifs, dumps CSV/JSON |
| `beats` | Recevoir les données de Filebeat/Metricbeat | Collecte distribuée de logs |
| `http` | Recevoir des données via HTTP POST | Webhooks, API custom |
| `http_poller` | Polling d'une API REST à intervalle régulier | API publiques, micro-batch |
| `jdbc` | Requêter une base de données relationnelle | Export incrémental depuis PostgreSQL |
| `kafka` | Consommer un topic Kafka | Streaming, event-driven |
| `stdin` | Lire depuis l'entrée standard | Debug et développement |
| `s3` | Lire des fichiers depuis un bucket S3 | Intégration cloud, landing zone |

**Dans SciPulse**, nous utilisons le plugin `file` pour lire les fichiers JSON du dump ArXiv :

```ruby
input {
  file {
    path => "/data/arxiv-raw/*.json"
    start_position => "beginning"     # Lire depuis le début (pas la fin)
    sincedb_path => "/dev/null"       # Pas de suivi de position (relecture complète)
    codec => "json"                   # Chaque ligne est un objet JSON
    mode => "read"                    # Lire puis s'arrêter (vs. "tail" = suivre)
  }
}
```

**Points d'attention** :

- `start_position` : `beginning` pour un traitement batch initial, `end` pour du suivi en continu (tail -f).
- `sincedb_path` : Logstash mémorise la position de lecture dans un fichier « sincedb ». En mettant `/dev/null`, on force la relecture complète à chaque redémarrage — utile en développement, pas en production.
- `codec` : détermine comment Logstash interprète chaque unité de données. `json` parse chaque ligne comme un objet JSON. `plain` traite chaque ligne comme du texte brut. `multiline` agrège plusieurs lignes en un seul événement (utile pour les stacktraces Java).

### 3.4. Les plugins de filtre (Filter)

Les filtres transforment les données **en vol**, entre l'entrée et la sortie. C'est le cœur du travail de Logstash. Les filtres s'exécutent dans l'ordre de déclaration, chaque filtre recevant en entrée la sortie du précédent.

Les filtres les plus utilisés :

#### `grok` — Parser du texte non structuré

Grok est le filtre le plus emblématique de Logstash. Il utilise des **expressions régulières nommées** pour extraire des champs structurés depuis du texte libre.

```ruby
# Ligne de log Apache :
# 192.168.1.1 - - [04/Jan/2024:10:15:32 +0000] "GET /index.html HTTP/1.1" 200 1234

filter {
  grok {
    match => {
      "message" => '%{IP:client_ip} - - \[%{HTTPDATE:timestamp}\] "%{WORD:method} %{URIPATH:path} HTTP/%{NUMBER:http_version}" %{NUMBER:status:int} %{NUMBER:bytes:int}'
    }
  }
}
```

Grok est extrêmement puissant pour les logs, mais il est fragile : un changement mineur dans le format du log casse le parsing. La documentation Elastic maintient un catalogue de patterns prédéfinis [3].

**Dans SciPulse** : nous n'avons pas besoin de grok parce que nos données ArXiv sont déjà en JSON. Le codec `json` fait le parsing à l'entrée. Grok serait utile si nous ingérions des logs textuels (logs Airflow, logs applicatifs).

#### `json` — Parser du JSON

Quand le codec d'entrée ne suffit pas (données JSON imbriquées dans un champ texte) :

```ruby
filter {
  json {
    source => "message"          # Champ contenant le JSON brut
    target => "parsed"           # Où placer l'objet parsé (optionnel)
  }
}
```

#### `mutate` — Le couteau suisse

Le filtre `mutate` regroupe les transformations les plus courantes :

```ruby
filter {
  mutate {
    # Renommer un champ
    rename => { "id" => "arxiv_id" }

    # Supprimer des champs inutiles
    remove_field => ["@version", "host", "path"]

    # Convertir un type
    convert => { "score" => "integer" }

    # Éclater une chaîne en tableau
    split => { "categories" => " " }

    # Remplacer du texte
    gsub => ["abstract", "\n", " "]

    # Supprimer les espaces en début/fin
    strip => ["title", "abstract"]

    # Ajouter un champ
    add_field => { "pipeline" => "arxiv-batch" }
  }
}
```

**Dans SciPulse**, `mutate` est notre filtre principal pour le pipeline ArXiv : renommage des champs, éclatement des catégories, nettoyage de l'abstract.

#### `date` — Parser les dates

Convertit une chaîne de texte en objet date Elasticsearch :

```ruby
filter {
  date {
    match => ["update_date", "yyyy-MM-dd"]
    target => "date_updated"
  }
}
```

Sans ce filtre, les dates resteraient des chaînes de texte et les requêtes temporelles dans Elasticsearch (range, histogrammes) ne fonctionneraient pas.

#### `ruby` — Code Ruby arbitraire

Pour les transformations impossibles avec les filtres standard, le filtre `ruby` permet d'exécuter du code Ruby directement :

```ruby
filter {
  ruby {
    code => '
      cats = event.get("categories")
      if cats.is_a?(Array) && cats.length > 0
        event.set("primary_category", cats[0])
      end
    '
  }
}
```

C'est puissant mais à utiliser avec modération : du code Ruby dans une config Logstash est difficile à tester et à maintenir. Si la logique devient complexe, mieux vaut la déplacer dans un script Python en amont.

#### Autres filtres notables

| Filtre | Usage |
|--------|-------|
| `csv` | Parser des lignes CSV |
| `kv` | Extraire des paires clé=valeur |
| `geoip` | Résoudre une IP en coordonnées géographiques |
| `useragent` | Parser un User-Agent en composants (navigateur, OS, device) |
| `translate` | Lookup dans un dictionnaire (enrichissement par table de correspondance) |
| `aggregate` | Agréger plusieurs événements en un seul (corrélation) |
| `fingerprint` | Générer un hash de déduplication |
| `drop` | Supprimer conditionnellement un événement |
| `clone` | Dupliquer un événement (envoi vers plusieurs destinations) |

### 3.5. Les plugins de sortie (Output)

Le plugin de sortie définit **où écrire les données transformées** :

| Plugin | Usage |
|--------|-------|
| `elasticsearch` | Indexation dans Elasticsearch (le plus courant) |
| `file` | Écriture dans un fichier |
| `stdout` | Affichage en console (debug) |
| `kafka` | Publication dans un topic Kafka |
| `s3` | Écriture dans un bucket S3 |
| `http` | Envoi HTTP POST vers une API |
| `jdbc` | Écriture dans une base relationnelle |

**Dans SciPulse** :

```ruby
output {
  elasticsearch {
    hosts => ["http://elasticsearch:9200"]
    index => "arxiv-papers-raw"
    document_id => "%{arxiv_id}"    # Clé de déduplication
    action => "index"
  }
}
```

Points clés :

- `document_id` : en utilisant l'ID ArXiv comme identifiant du document ES, on obtient une ingestion **idempotente** — relancer le pipeline n'insère pas de doublons. C'est une bonne pratique DataOps fondamentale.
- `action` : `index` crée ou remplace le document. `create` échoue si le document existe déjà. `update` met à jour partiellement.
- En production, on ajouterait un `stdout { codec => dots }` en parallèle pour suivre la progression (un point par événement traité).

### 3.6. Conditions et routage

Logstash supporte des **conditions** qui permettent d'appliquer des filtres ou des sorties différentes selon le contenu de l'événement :

```ruby
filter {
  if [type] == "story" {
    mutate { add_field => { "is_story" => true } }
  } else if [type] == "comment" {
    mutate { add_field => { "is_comment" => true } }
  }
}

output {
  if [primary_category] =~ /^cs\./ {
    elasticsearch {
      hosts => ["http://elasticsearch:9200"]
      index => "arxiv-cs"
    }
  } else {
    elasticsearch {
      hosts => ["http://elasticsearch:9200"]
      index => "arxiv-other"
    }
  }
}
```

Les opérateurs disponibles sont : `==`, `!=`, `<`, `>`, `in`, `not in`, `=~` (regex), `and`, `or`, `not`.

### 3.7. Pipelines multiples

Depuis Logstash 6.0, il est possible de définir **plusieurs pipelines** indépendants dans une même instance, via le fichier `pipelines.yml` :

```yaml
# config/pipelines.yml
- pipeline.id: arxiv
  path.config: "/usr/share/logstash/pipeline/arxiv.conf"
  pipeline.workers: 2

- pipeline.id: logs
  path.config: "/usr/share/logstash/pipeline/logs.conf"
  pipeline.workers: 1
```

Chaque pipeline a ses propres workers, sa propre queue, et ses propres métriques. C'est la bonne pratique en production : un pipeline par source ou par cas d'usage, plutôt qu'un pipeline monolithique avec des conditions partout.

### 3.8. Persistent queues et fiabilité

Par défaut, Logstash utilise une queue en mémoire (in-memory queue). Si Logstash plante, les événements en cours de traitement sont perdus.

Pour garantir la livraison, Logstash propose des **persistent queues** (PQ) sur disque :

```yaml
# logstash.yml
queue.type: persisted
queue.max_bytes: 1gb
```

Avec les PQ activées, les événements sont écrits sur disque avant d'être traités. En cas de crash, Logstash reprend là où il s'est arrêté au redémarrage. C'est une garantie de type « at-least-once delivery » — chaque événement est traité au moins une fois, mais potentiellement plus en cas de crash entre le traitement et l'acquittement.

Dans notre TP, nous n'activons pas les PQ (ce n'est pas critique pour un dump statique), mais en production c'est indispensable.

## 4. Filebeat : le collecteur léger

### 4.1. Pourquoi Filebeat ?

Logstash est puissant mais lourd : il consomme au minimum 500 Mo de RAM à cause de la JVM. Déployer une instance Logstash sur chaque serveur d'une flotte de 200 machines pour collecter des logs serait un gaspillage de ressources.

C'est le problème que résout **Filebeat** : un collecteur de fichiers léger, écrit en Go, qui consomme ~30 Mo de RAM. Son rôle est simple et ciblé : **surveiller des fichiers, lire les nouvelles lignes, et les envoyer à une destination** (Logstash, Elasticsearch, Kafka).

Filebeat fait partie de la famille des **Beats** — des agents légers spécialisés développés par Elastic :

| Beat | Spécialité |
|------|-----------|
| **Filebeat** | Fichiers et logs |
| Metricbeat | Métriques système et services (CPU, RAM, disque, MySQL, Redis…) |
| Packetbeat | Trafic réseau |
| Auditbeat | Audit système (Linux auditd) |
| Heartbeat | Monitoring de disponibilité (ping, HTTP, TCP) |

### 4.2. Architecture Filebeat

```
┌──────────────────────────────────────────┐
│              FILEBEAT                     │
│                                          │
│  ┌────────────┐     ┌──────────────┐     │
│  │ HARVESTER  │     │  HARVESTER   │     │
│  │ (fichier A)│     │  (fichier B) │     │
│  └─────┬──────┘     └──────┬───────┘     │
│        │                   │             │
│        ▼                   ▼             │
│  ┌────────────────────────────────┐      │
│  │          SPOOLER              │      │
│  │  (buffer en mémoire + disque) │      │
│  └──────────────┬────────────────┘      │
│                 │                        │
│                 ▼                        │
│  ┌────────────────────────────────┐      │
│  │          OUTPUT               │      │
│  │  (Logstash / ES / Kafka)      │      │
│  └────────────────────────────────┘      │
└──────────────────────────────────────────┘
```

Concepts clés :

- **Harvester** : un goroutine par fichier surveillé. Lit les nouvelles lignes et les envoie au spooler. Gère le suivi de position (offset) dans un fichier `registry`.
- **Spooler** : agrège les événements des harvesters et les transmet à l'output par batch.
- **Registry** : fichier persistant qui mémorise la position de lecture de chaque fichier. Survit aux redémarrages. L'équivalent du `sincedb` de Logstash, mais plus robuste.
- **Backpressure** : si la destination est saturée, Filebeat ralentit la lecture. Pas de perte de données.

### 4.3. Configuration de base

```yaml
# filebeat.yml
filebeat.inputs:
  - type: log
    enabled: true
    paths:
      - /var/log/airflow/*.log
    multiline:
      pattern: '^\d{4}-\d{2}-\d{2}'    # Les lignes de log commencent par une date
      negate: true                       # Si la ligne NE commence PAS par ce pattern...
      match: after                       # ...elle est rattachée à la ligne précédente

output.elasticsearch:
  hosts: ["http://elasticsearch:9200"]
  index: "pipeline-logs-%{+yyyy.MM.dd}"

# OU envoyer vers Logstash pour un traitement plus poussé :
# output.logstash:
#   hosts: ["logstash:5044"]
```

### 4.4. Filebeat vs. Logstash : quand utiliser quoi ?

C'est une question fréquente et la réponse dépend du contexte. Voici un arbre de décision pragmatique :

| Critère | Filebeat | Logstash | Python script |
|---------|----------|----------|---------------|
| Complexité du parsing | Faible (JSON, logs simples) | Moyenne (grok, mutate, ruby) | Élevée (NLP, API, logique métier) |
| Consommation mémoire | ~30 Mo | ~500 Mo – 1 Go | Variable |
| Nombre d'instances à déployer | Beaucoup (un par serveur) | Peu (centralisé) | Peu |
| Transformation des données | Basique (ajout de champs, décodage JSON) | Riche (grok, date, geoip, ruby) | Illimitée |
| Scalabilité | Horizontale native | Instances multiples manuelles | Libre |
| Cas d'usage SciPulse | Collecte des logs Airflow | Ingestion du dump ArXiv | Polling HN, enrichissement NLP |

**La combinaison Filebeat + Logstash est un pattern classique** : Filebeat collecte les données de manière légère et distribuée, puis les envoie à un Logstash centralisé qui fait le gros du parsing et de la transformation. C'est le pattern que nous utiliserons au Jour 4 pour la collecte des logs du pipeline.

**Le script Python est pertinent quand** :

- La source est une API REST (le polling HTTP est plus naturel en Python qu'en Logstash).
- La transformation nécessite de la logique métier complexe (NLP, croisement de données, appels à des services externes).
- Vous voulez des tests unitaires sur votre code d'ingestion (tester un script Python est trivial ; tester une config Logstash est laborieux).
- L'équipe maîtrise Python mais pas la syntaxe Logstash.

### 4.5. Le pattern Filebeat → Logstash → Elasticsearch

Ce pattern en trois étages est considéré comme la meilleure pratique pour les architectures de collecte de logs à l'échelle, tel que décrit dans la documentation Elastic et par les praticiens de la communauté [2][4] :

```
  Serveur App 1          Serveur App 2          Serveur App 3
  ┌───────────┐          ┌───────────┐          ┌───────────┐
  │ Filebeat  │          │ Filebeat  │          │ Filebeat  │
  └─────┬─────┘          └─────┬─────┘          └─────┬─────┘
        │                      │                      │
        └──────────────────────┼──────────────────────┘
                               │
                               ▼
                     ┌───────────────────┐
                     │    LOGSTASH       │
                     │  (centralisé)     │
                     │  parsing, enrichi.│
                     └────────┬──────────┘
                              │
                              ▼
                     ┌───────────────────┐
                     │  ELASTICSEARCH    │
                     └───────────────────┘
```

Avantages :

- **Séparation des préoccupations** : Filebeat s'occupe de la collecte fiable, Logstash du parsing intelligent.
- **Scalabilité** : on peut ajouter des serveurs (et donc des Filebeat) sans surcharger Logstash grâce au backpressure natif.
- **Centralisation du parsing** : les règles grok sont à un seul endroit, pas dupliquées sur 200 serveurs.
- **Résilience** : si Logstash est temporairement indisponible, Filebeat bufferise les événements localement.

## 5. Bonnes pratiques DataOps pour l'ingestion

Pour conclure ce bloc théorique, synthétisons les bonnes pratiques qui s'appliquent à toute couche d'ingestion, quel que soit l'outil choisi.

### 5.1. Immutabilité des données sources

Ne jamais modifier les données brutes en place. Toujours conserver une copie telle que reçue (dans MinIO, dans un bucket S3, sur un disque dédié). Les transformations produisent de **nouvelles** données dans un nouvel emplacement.

Pourquoi : si une transformation introduit un bug, on peut revenir aux données brutes et recommencer. Sans copie brute, les données corrompues sont perdues définitivement.

**Dans SciPulse** : le dump ArXiv reste dans MinIO (`arxiv-raw/`). Logstash lit le fichier mais ne le modifie pas. Les données transformées vont dans un index ES séparé (`arxiv-papers-raw`), puis dans un index enrichi (`arxiv-papers`).

### 5.2. Idempotence

Un pipeline d'ingestion est **idempotent** si l'exécuter une fois ou dix fois produit exactement le même résultat. C'est fondamental pour pouvoir relancer un pipeline en toute sécurité après un échec partiel.

Comment l'obtenir :

- Utiliser un identifiant naturel comme `document_id` dans Elasticsearch (notre `arxiv_id`). Réindexer un article existant l'écrase au lieu de le dupliquer.
- Utiliser des opérations `upsert` (insert or update) plutôt que des inserts purs.
- Horodater les exécutions et filtrer les données déjà traitées (watermarking).

**Dans SciPulse** : le `document_id => "%{arxiv_id}"` dans le pipeline Logstash garantit l'idempotence. Le poller HN vérifie l'existence de chaque item avant de l'indexer (`item_exists()`).

### 5.3. Schema-on-read vs. Schema-on-write

Deux philosophies s'affrontent :

- **Schema-on-write** : les données sont validées et structurées au moment de l'écriture. Si elles ne respectent pas le schéma, elles sont rejetées. C'est le modèle des bases relationnelles et d'Elasticsearch avec mapping explicite.
- **Schema-on-read** : les données sont stockées telles quelles, et la structure est appliquée au moment de la lecture. C'est le modèle des data lakes (S3, MinIO).

**En DataOps, on combine les deux** : schema-on-read pour la landing zone (MinIO stocke tout, sans filtre), schema-on-write pour la couche de serving (Elasticsearch impose un mapping strict). C'est exactement l'architecture de SciPulse : MinIO accepte tout, Elasticsearch valide.

### 5.4. Gestion des erreurs et dead letter queues

Les événements qui échouent au parsing ou à l'indexation ne doivent pas être silencieusement perdus. Les bonnes pratiques :

- **Dead letter queue (DLQ)** : les événements en erreur sont envoyés dans une file dédiée pour inspection manuelle. Logstash supporte les DLQ nativement [2].
- **Logging des erreurs** : chaque erreur est loggée avec suffisamment de contexte pour diagnostiquer le problème (l'événement original, le message d'erreur, le timestamp).
- **Métriques d'erreur** : le taux d'erreur est monitoré et alerte si un seuil est dépassé.
- **Circuit breaker** : si le taux d'erreur est trop élevé, le pipeline s'arrête plutôt que de continuer à produire des données corrompues.

### 5.5. Backpressure

Le backpressure est le mécanisme par lequel un système aval signale au système amont qu'il est saturé. Sans backpressure, un producteur rapide submerge un consommateur lent, ce qui mène à des pertes de données ou des crashs mémoire.

Logstash et Filebeat gèrent le backpressure nativement :

- Si Elasticsearch est lent à indexer, Logstash ralentit la lecture des inputs.
- Si Logstash est saturé, Filebeat ralentit la lecture des fichiers et bufferise localement.

Un script Python doit implémenter ce mécanisme manuellement (retry avec backoff exponentiel, limitation du débit d'appels API).

---

## 6. Ce qui vient ensuite

Le TP de cet après-midi met en pratique ces concepts :

1. **Configuration du pipeline Logstash** pour le dump ArXiv : input `file`, filtres `json`/`mutate`/`date`/`ruby`, output vers Elasticsearch.
2. **Pipeline Python alternatif** avec `elasticsearch-py` et les `bulk` helpers — pour comparer les approches et comprendre quand utiliser l'un ou l'autre.
3. **Validation** dans Kibana Discover : vérifier que les 100 000 articles sont correctement indexés et exploitables.

À la fin de la journée, l'index `arxiv-papers-raw` contiendra 100 000 documents prêts à être modélisés avec un mapping optimisé (Jour 2).

## Bibliographie

### Ouvrages de référence

[1] Kleppmann, M. (2017). *Designing Data-Intensive Applications: The Big Ideas Behind Reliable, Scalable, and Maintainable Systems*. O'Reilly Media.  
L'ouvrage de référence en ingénierie de données. Les chapitres 3 (Storage and Retrieval), 10 (Batch Processing) et 11 (Stream Processing) sont directement pertinents pour les patterns d'ingestion. La distinction entre les architectures Lambda et Kappa (chapitre 11) éclaire le choix entre batch, streaming et hybride.

[2] Elastic. (2024). *Logstash Reference*. https://www.elastic.co/guide/en/logstash/current/index.html  
Documentation officielle de Logstash. Les sections « How Logstash Works », « Input Plugins », « Filter Plugins » et « Output Plugins » sont les références canoniques. La section « Dead Letter Queues » couvre la gestion des événements en erreur. La section « Persistent Queues » détaille les garanties de livraison.

[3] Elastic. (2024). *Grok Pattern Reference*. https://www.elastic.co/guide/en/logstash/current/plugins-filters-grok.html  
Catalogue complet des patterns grok prédéfinis (IP, HTTPDATE, LOGLEVEL, etc.) avec exemples d'utilisation et référence à la bibliothèque Oniguruma sous-jacente.

[4] Elastic. (2024). *Filebeat Reference*. https://www.elastic.co/guide/en/beats/filebeat/current/index.html  
Documentation officielle de Filebeat. Les sections « How Filebeat Works » (harvesters, registry) et « Configure Filebeat » sont essentielles. La section « Filebeat vs. Logstash » fournit les critères de choix officiels.

[5] Reis, J., & Housley, M. (2022). *Fundamentals of Data Engineering: Plan and Build Robust Data Systems*. O'Reilly Media.  
Le chapitre 7 (Ingestion) est la référence la plus complète et récente sur les patterns d'ingestion dans le contexte du data engineering moderne. Il distingue clairement batch, micro-batch et streaming, et discute les trade-offs (latence vs. complexité vs. coût). Le chapitre 3 (Designing Good Data Architecture) pose le cadre des architectures hybrides.

[6] Narkhede, N., Shapira, G., & Palino, T. (2017). *Kafka: The Definitive Guide*. O'Reilly Media.  
Bien que nous n'utilisions pas Kafka dans le TP, les chapitres 1 et 2 offrent une excellente introduction au streaming et aux brokers de messages. Les concepts de topics, partitions, consumer groups et exactly-once semantics sont fondamentaux pour tout ingénieur data.

### Articles et ressources complémentaires

[7] Kreps, J. (2014). « Questioning the Lambda Architecture ». *O'Reilly Radar*. https://www.oreilly.com/radar/questioning-the-lambda-architecture/  
Article fondateur de l'architecture Kappa, qui propose de simplifier l'architecture Lambda en n'utilisant qu'un seul système de streaming (Kafka) pour le batch et le temps réel. Éclaire les compromis architecturaux entre les deux approches.

[8] Sato, D. et al. (2019). « Continuous Intelligence: Data Pipelines and the DataOps Revolution ». *ThoughtWorks Technology Radar*.  
Analyse de ThoughtWorks sur l'émergence du DataOps et des pipelines de données continus, avec des retours d'expérience terrain.

[9] DataOps Manifesto. https://dataopsmanifesto.org  
Les 18 principes fondateurs du DataOps, inspirés du Manifeste Agile. Référence rapide pour les principes évoqués dans le bloc théorique du matin.

[10] Elastic. (2024). *Elastic Stack Architecture Best Practices*. https://www.elastic.co/blog/elastic-stack-architecture-best-practices  
Guide d'architecture pour le déploiement de la stack Elastic en production, incluant les patterns Filebeat → Logstash → ES, le dimensionnement, et les stratégies de résilience.

[11] Dunning, T., & Friedman, E. (2016). *Streaming Architecture: New Designs Using Apache Kafka and MapR Streams*. O'Reilly Media.  
Ouvrage court et accessible sur les architectures de streaming, avec une bonne discussion des patterns micro-batch vs. streaming pur et des garanties de livraison (at-most-once, at-least-once, exactly-once).

### Datasets utilisés dans le TP

[12] Cornell University. (2024). *arXiv Dataset*. Kaggle. https://www.kaggle.com/datasets/Cornell-University/arxiv  
Dump complet des métadonnées ArXiv (2M+ articles). Format JSON lines. Mis à jour mensuellement.

[13] Hacker News. (2024). *Hacker News API (v0)*. https://github.com/HackerNews/API  
API Firebase publique de Hacker News. Documentation des endpoints (`/v0/newstories`, `/v0/item/{id}`, `/v0/topstories`). Pas de rate limiting strict, mais politesse recommandée (~30 req/s).
