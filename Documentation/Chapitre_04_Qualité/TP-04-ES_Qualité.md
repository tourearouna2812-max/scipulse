# Jour 2 — Après-midi
# TP : Tests de qualité et nettoyage des données

> **Durée** : 2h45  
> **Niveau** : débutant — chaque étape est détaillée  
> **Prérequis** : avoir complété le TP du matin (index `arxiv-papers` peuplé avec ~100 000 documents, premiers items HN indexés)

---

## Ce que vous allez construire

À la fin de ce TP, vous aurez :

1. Great Expectations installé et configuré.
2. Une suite de validations pour les données ArXiv (7+ expectations).
3. Une suite de validations pour les données Hacker News (5+ expectations).
4. Exécuté les checkpoints et consulté les Data Docs (rapports HTML).
5. Écrit un script de nettoyage guidé par les résultats des validations.
6. Revalidé les données nettoyées pour constater l'amélioration.

---

## Vérifications préalables (5 min)

```bash
# La stack tourne ?
docker compose ps

# L'index ArXiv optimisé est peuplé ?
curl -s http://localhost:9200/arxiv-papers/_count
# → Doit afficher ~100 000

# L'index HN a quelques documents ?
curl -s http://localhost:9200/hn-items/_count
# → Doit afficher > 0 (quelques dizaines minimum)

# L'environnement Python est activé ?
which python
# → Doit pointer vers .venv/bin/python (ou similaire)
```

Si l'index HN est vide, lancez un cycle de polling rapide :

```bash
python -m src.ingestion.hn_poller
```

---

# PARTIE A — Installation et configuration de Great Expectations (20 min)

## Étape 1 — Installer Great Expectations

### 1.1. Installation

```bash
pip install great-expectations
```

L'installation prend 1-2 minutes (GX a beaucoup de dépendances).

Vérifiez l'installation :

```bash
python -c "import great_expectations; print(f'GX version: {great_expectations.__version__}')"
```

Vous devez voir une version ≥ 0.18.

### 1.2. Comprendre l'approche que nous allons utiliser

Great Expectations propose deux modes d'utilisation :

- **Mode projet** : un dossier `great_expectations/` avec des fichiers YAML, des stores, et une arborescence complète. C'est le mode recommandé en production.
- **Mode programmatique** : tout en Python, sans fichiers YAML. Plus simple pour un TP, et plus facile à intégrer dans un pipeline.

Nous utiliserons le **mode programmatique** — tout se passe dans des scripts Python que vous pouvez exécuter, tester, et versionner dans Git. Pas de « magie » cachée dans des fichiers YAML.

---

## Étape 2 — Extraire les données depuis Elasticsearch (15 min)

Great Expectations ne se connecte pas nativement à Elasticsearch. Notre stratégie : extraire les données dans un DataFrame Pandas, puis valider le DataFrame.

### 2.1. Créer le script d'extraction

Créez le fichier `src/quality/extract_for_validation.py` :

```python
""".
SciPulse — Extraction de données depuis Elasticsearch vers Pandas
pour validation par Great Expectations.

Usage :
    python -m src.quality.extract_for_validation
"""

import pandas as pd
from elasticsearch import Elasticsearch, helpers


ES_HOST = "http://localhost:9200"


def extract_arxiv(es: Elasticsearch, index: str = "arxiv-papers", max_docs: int = 10_000) -> pd.DataFrame:
    """
    Extrait un échantillon d'articles ArXiv depuis Elasticsearch.

    On ne charge pas les 100 000 documents pour la validation —
    un échantillon de 10 000 est statistiquement suffisant et
    beaucoup plus rapide à traiter.
    """
    docs = []
    query = {"query": {"match_all": {}}, "size": max_docs}

    for hit in helpers.scan(es, index=index, query=query, size=1000, preserve_order=False):
        source = hit["_source"]
        docs.append({
            "arxiv_id":         source.get("arxiv_id"),
            "title":            source.get("title"),
            "abstract":         source.get("abstract"),
            "authors_flat":     source.get("authors_flat"),
            "categories":       source.get("categories"),
            "primary_category": source.get("primary_category"),
            "date_published":   source.get("date_published"),
            "date_updated":     source.get("date_updated"),
            "doi":              source.get("doi"),
        })

        if len(docs) >= max_docs:
            break

    df = pd.DataFrame(docs)
    print(f"✅ Extrait {len(df):,} articles ArXiv depuis '{index}'")
    print(f"   Colonnes : {list(df.columns)}")
    print(f"   Mémoire  : {df.memory_usage(deep=True).sum() / 1024 / 1024:.1f} Mo")
    return df


def extract_hn(es: Elasticsearch, index: str = "hn-items", max_docs: int = 5_000) -> pd.DataFrame:
    """Extrait les items Hacker News depuis Elasticsearch."""
    docs = []

    for hit in helpers.scan(es, index=index, query={"query": {"match_all": {}}}, size=500):
        source = hit["_source"]
        docs.append({
            "hn_id":            source.get("hn_id"),
            "type":             source.get("type"),
            "by":               source.get("by"),
            "time":             source.get("time"),
            "title":            source.get("title"),
            "url":              source.get("url"),
            "text":             source.get("text"),
            "score":            source.get("score"),
            "descendants":      source.get("descendants"),
            "arxiv_id_linked":  source.get("arxiv_id_linked"),
        })

        if len(docs) >= max_docs:
            break

    df = pd.DataFrame(docs)
    print(f"✅ Extrait {len(df):,} items HN depuis '{index}'")
    return df


if __name__ == "__main__":
    es = Elasticsearch(ES_HOST)

    print("\n📥 Extraction des données pour validation\n")

    df_arxiv = extract_arxiv(es)
    print()
    print(df_arxiv.info())
    print()
    print(df_arxiv.head(3).to_string())

    print("\n" + "─" * 60 + "\n")

    df_hn = extract_hn(es)
    print()
    print(df_hn.info())
```

### 2.2. Exécuter l'extraction

```bash
python -m src.quality.extract_for_validation
```

Sortie attendue :

```
📥 Extraction des données pour validation

✅ Extrait 10,000 articles ArXiv depuis 'arxiv-papers'
   Colonnes : ['arxiv_id', 'title', 'abstract', ...]
   Mémoire  : 42.3 Mo

<class 'pandas.core.frame.DataFrame'>
RangeIndex: 10000 entries, 0 to 9999
Data columns (total 9 columns):
 #   Column            Non-Null Count  Dtype
---  ------            --------------  -----
 0   arxiv_id          10000 non-null  object
 1   title             10000 non-null  object
 2   abstract          9987 non-null   object     ← 13 nulls !
 ...
```

**Observez déjà les premières anomalies** dans la sortie de `df.info()` : certaines colonnes ont moins de `non-null` que le nombre total de lignes. Notez-les — c'est exactement ce que les expectations vont formaliser.

---

# PARTIE B — Suite de validations ArXiv (45 min)

## Étape 3 — Écrire la suite ArXiv (30 min)

Créez le fichier `src/quality/validate_arxiv.py`. Nous allons l'écrire section par section.

### 3.1. Structure de base

```python
"""
SciPulse — Suite de validation Great Expectations pour les données ArXiv

Ce script :
  1. Extrait un échantillon depuis Elasticsearch
  2. Définit une suite d'expectations
  3. Exécute la validation
  4. Génère un rapport HTML (Data Docs)

Usage :
    python -m src.quality.validate_arxiv
"""

import sys

import great_expectations as gx
import pandas as pd
from elasticsearch import Elasticsearch

from src.quality.extract_for_validation import extract_arxiv

ES_HOST = "http://localhost:9200"
```

### 3.2. Fonction de création de la suite

Ajoutez cette fonction qui définit toutes les expectations :

```python
def validate_arxiv_data(df: pd.DataFrame) -> gx.checkpoint.types.checkpoint_result.CheckpointResult:
    """
    Valide un DataFrame d'articles ArXiv avec Great Expectations.

    Retourne le résultat du checkpoint (succès/échec par expectation).
    """

    # ── 1. Créer un contexte GX en mémoire (mode programmatique) ──────


    # ── 2. Connecter le DataFrame comme source de données ─────────────
    # a) Initialiser une source de données de type pandas, ayant pour nom 'arxiv_pa,das'
    # b) Définir pour la source un asset 'arxiv_data'
    # c) Connecter le dataframe df comme contenu de l'asset

    # ── 3. Créer la suite d'expectations ──────────────────────────────


    # ── 4. Obtenir un validateur (le pont entre les données et la suite)

    # ══════════════════════════════════════════════════════════════
    #  EXPECTATIONS — Complétude
    # ══════════════════════════════════════════════════════════════

    # E1 : L'identifiant ArXiv ne doit JAMAIS être null
    # C'est notre clé primaire — un seul null casserait l'indexation

    # E2 : Le titre ne doit jamais être null

    # E3 : L'abstract doit être présent dans au moins 99% des cas
    # Quelques articles très anciens peuvent ne pas avoir d'abstract

    # E4 : La catégorie primaire ne doit jamais être null

    # ══════════════════════════════════════════════════════════════
    #  EXPECTATIONS — Unicité
    # ══════════════════════════════════════════════════════════════

    # E5 : L'identifiant ArXiv doit être unique (pas de doublons)

    # ══════════════════════════════════════════════════════════════
    #  EXPECTATIONS — Validité
    # ══════════════════════════════════════════════════════════════

    # E6 : L'abstract doit avoir au moins 50 caractères
    # Un abstract de moins de 50 caractères est probablement corrompu

    # E7 : Le titre doit avoir au moins 10 caractères

    # E8 : La catégorie primaire doit être dans un ensemble connu
    # Les catégories ArXiv CS principales
    known_cs_categories = []

    # ══════════════════════════════════════════════════════════════
    #  EXPECTATIONS — Volume
    # ══════════════════════════════════════════════════════════════

    # E9 : Le nombre de lignes doit être dans un intervalle raisonnable

    # E10 : Toutes les colonnes attendues doivent être présentes

    # ══════════════════════════════════════════════════════════════
    #  SAUVEGARDER LA SUITE ET EXÉCUTER
    # ══════════════════════════════════════════════════════════════

    validator.save_expectation_suite(discard_failed_expectations=False)

    # Créer et exécuter le checkpoint
    checkpoint = context.add_or_update_checkpoint(
        name="arxiv_checkpoint",
        validations=[
            {
                "batch_request": batch_request,
                "expectation_suite_name": "arxiv_quality_suite",
            }
        ],
    )

    result = checkpoint.run()
    return context, result
```

### 3.3. Point d'entrée et affichage des résultats

Ajoutez à la fin du fichier :

```python
def print_results(result):
    """Affiche les résultats de manière lisible dans le terminal."""
    print("\n" + "═" * 60)
    print("  RÉSULTATS DE LA VALIDATION ARXIV")
    print("═" * 60)

    run_results = list(result.run_results.values())
    if not run_results:
        print("  ⚠️  Aucun résultat")
        return

    validation_result = run_results[0]["validation_result"]
    results_list = validation_result["results"]

    success_count = 0
    failure_count = 0

    for r in results_list:
        success = r["success"]
        expectation_type = r["expectation_config"]["expectation_type"]
        kwargs = r["expectation_config"]["kwargs"]

        # Extraire le nom de la colonne s'il existe
        column = kwargs.get("column", "—table—")

        if success:
            success_count += 1
            icon = "✅"
        else:
            failure_count += 1
            icon = "❌"

        # Simplifier le nom de l'expectation pour l'affichage
        short_name = expectation_type.replace("expect_column_values_to_", "").replace("expect_column_", "").replace("expect_table_", "table:")

        # Extraire les stats
        result_detail = r.get("result", {})
        element_count = result_detail.get("element_count", "")
        unexpected_count = result_detail.get("unexpected_count", "")
        unexpected_pct = result_detail.get("unexpected_percent", "")

        stats = ""
        if unexpected_count != "" and unexpected_count is not None:
            stats = f" ({unexpected_count} non-conformes"
            if unexpected_pct is not None:
                stats += f", {unexpected_pct:.2f}%"
            stats += ")"

        print(f"  {icon} {column:<22} {short_name:<35} {stats}")

    print(f"\n  {'─' * 56}")
    print(f"  ✅ Succès  : {success_count}")
    print(f"  ❌ Échecs  : {failure_count}")
    total = success_count + failure_count
    pct = success_count / total * 100 if total > 0 else 0
    print(f"  📊 Score   : {pct:.0f}% ({success_count}/{total})")
    print()


if __name__ == "__main__":
    print()
    print("╔══════════════════════════════════════════════════════════╗")
    print("║  SciPulse — Validation qualité ArXiv                    ║")
    print("╚══════════════════════════════════════════════════════════╝")

    # 1. Connexion à Elasticsearch
    pass
    
    # 2. Extraction des données
    print("\n📥 Extraction d'un échantillon depuis Elasticsearch...\n")
    pass
    
    # 3. Validation
    print("\n🔍 Exécution de la suite de validation...\n")
    pass
    
    # 3. Résultats
    print_results(result)

    # 4. Data Docs
    pass
    
    # 5. Code de sortie (pour intégration Airflow)
    if not result.success:
        print("⚠️  Certaines validations ont échoué — voir les détails ci-dessus.")
        sys.exit(1)
    else:
        print("🎉 Toutes les validations ont réussi !")
```

### 3.4. Exécuter la validation ArXiv

Sauvegardez le fichier et lancez :

```bash
python -m src.quality.validate_arxiv
```

Sortie attendue (les chiffres exacts varieront selon vos données) :

```
╔══════════════════════════════════════════════════════════╗
║  SciPulse — Validation qualité ArXiv                    ║
╚══════════════════════════════════════════════════════════╝

📥 Extraction d'un échantillon depuis Elasticsearch...

✅ Extrait 10,000 articles ArXiv depuis 'arxiv-papers'

🔍 Exécution de la suite de validation...

══════════════════════════════════════════════════════════
  RÉSULTATS DE LA VALIDATION ARXIV
══════════════════════════════════════════════════════════
  ✅ arxiv_id              not_be_null                          (0 non-conformes, 0.00%)
  ✅ title                 not_be_null                          (0 non-conformes, 0.00%)
  ✅ abstract              not_be_null                          (13 non-conformes, 0.13%)
  ✅ primary_category      not_be_null                          (0 non-conformes, 0.00%)
  ✅ arxiv_id              be_unique                            (0 non-conformes, 0.00%)
  ❌ abstract              value_lengths_to_be_between          (347 non-conformes, 3.47%)
  ✅ title                 value_lengths_to_be_between          (2 non-conformes, 0.02%)
  ✅ primary_category      be_in_set                            (156 non-conformes, 1.56%)
  ✅ —table—               table:row_count_to_be_between
  ✅ —table—               table:columns_to_include_column_set

  ────────────────────────────────────────────────────────
  ✅ Succès  : 9
  ❌ Échecs  : 1
  📊 Score   : 90% (9/10)
```

### 3.5. Analyser les résultats

Prenez quelques minutes pour analyser ce que la validation nous apprend :

**Les bonnes nouvelles** :

- `arxiv_id` est toujours présent et unique → pas de problème d'identifiant.
- `title` est toujours présent → la complétude est bonne.
- Le nombre de lignes est dans l'intervalle attendu.

**Les problèmes détectés** :

- **`abstract` longueur** : 347 abstracts (3.47%) font moins de 50 caractères. C'est au-dessus de notre seuil de tolérance (5%), donc l'expectation passe. Mais c'est un signal : ces abstracts courts méritent une inspection.
- **`primary_category` hors ensemble** : 156 articles (1.56%) ont une catégorie primaire qui n'est pas dans notre liste. Ce n'est probablement pas une erreur — notre liste n'est pas exhaustive. On pourrait la compléter.

**Exercice** : explorons les abstracts courts pour comprendre le problème.

```python
# Dans un notebook ou en console Python :
import pandas as pd
from elasticsearch import Elasticsearch, helpers
from src.quality.extract_for_validation import extract_arxiv

es = Elasticsearch("http://localhost:9200")
df = extract_arxiv(es)

# Abstracts de moins de 50 caractères
short = df[df["abstract"].str.len() < 50]
print(f"\n{len(short)} abstracts courts :")
print(short[["arxiv_id", "abstract"]].head(10).to_string())
```

Vous verrez probablement des abstracts vides, des placeholders (« No abstract available »), ou des abstracts tronqués.

---

# PARTIE C — Suite de validations Hacker News (20 min)

## Étape 4 — Écrire la suite HN

Créez le fichier `src/quality/validate_hn.py` :

```python
"""
SciPulse — Suite de validation Great Expectations pour les données Hacker News

Usage :
    python -m src.quality.validate_hn
"""

import sys

import great_expectations as gx
import pandas as pd
from elasticsearch import Elasticsearch

from src.quality.extract_for_validation import extract_hn

ES_HOST = "http://localhost:9200"


def validate_hn_data(df: pd.DataFrame):
    """Valide un DataFrame d'items Hacker News."""

    context = gx.get_context()
    datasource = context.sources.add_or_update_pandas(name="hn_pandas")
    data_asset = datasource.add_dataframe_asset(name="hn_data")
    batch_request = data_asset.build_batch_request(dataframe=df)

    suite = context.add_or_update_expectation_suite("hn_quality_suite")
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name="hn_quality_suite",
    )

    # ── Complétude ─────────────────────────────────────────────────

    # L'ID HN ne doit jamais être null

    # Le type doit toujours être présent

    # ── Validité ───────────────────────────────────────────────────

    # Le type doit être dans l'ensemble connu

    # Le score doit être >= 0 (quand il est présent)
    # Note : les commentaires n'ont pas de score → on filtre

    # ── Unicité ────────────────────────────────────────────────────

    # L'ID HN doit être unique

    # ── Volume ─────────────────────────────────────────────────────

    # On doit avoir au moins quelques documents

    # Sauvegarder et exécuter

    checkpoint = context.add_or_update_checkpoint(
        name="hn_checkpoint",
        validations=[
            {
                "batch_request": batch_request,
                "expectation_suite_name": "hn_quality_suite",
            }
        ],
    )

    result = checkpoint.run()
    return context, result


if __name__ == "__main__":
    print()
    print("╔══════════════════════════════════════════════════════════╗")
    print("║  SciPulse — Validation qualité Hacker News              ║")
    print("╚══════════════════════════════════════════════════════════╝")

    # 1. Connexion à Elasticsearch
    pass

    # 2. Extraction des données

    # 3. Jouer la suite de tests sur les données HN

    # 4. Réutiliser la fonction print_results du module ArXiv
    from src.quality.validate_arxiv import print_results
    print_results(result)

    # 5. Construire la documentation des données
```

### Exécuter la validation HN

```bash
python -m src.quality.validate_hn
```

Observez les résultats. Typiquement, les données HN sont assez propres (c'est une API bien structurée), mais le champ `score` peut avoir des valeurs nulles pour les commentaires.

---

# PARTIE D — Nettoyage des données (40 min)

## Étape 5 — Script de nettoyage guidé par les validations

Les validations ont identifié des problèmes. Écrivons un script de nettoyage qui les corrige.

Créez `src/quality/clean_arxiv.py` :

```python
"""
SciPulse — Nettoyage des données ArXiv
Guidé par les résultats des validations Great Expectations.

Ce script :
  1. Extrait les données depuis Elasticsearch
  2. Applique les corrections
  3. Affiche un rapport avant/après
  4. Réindexe les données corrigées

Usage :
    python -m src.quality.clean_arxiv
    python -m src.quality.clean_arxiv --dry-run     # Simuler sans réindexer
"""

import argparse
import re
import sys
import time

import pandas as pd
from elasticsearch import Elasticsearch, helpers

ES_HOST    = "http://localhost:9200"
INDEX      = "arxiv-papers"
BATCH_SIZE = 500


def load_all_articles(es: Elasticsearch) -> pd.DataFrame:
    """
    Charge tous les articles depuis Elasticsearch.
    Ne conserve que _id et _source
    """
    
    return df


def diagnose(df: pd.DataFrame) -> dict:
    """
    Analyse les problèmes de qualité et retourne un rapport.
    """
    report = {}

    # ── 1. Abstracts courts ou manquants ──────────────────────────────

    # ── 2. Titres vides ───────────────────────────────────────────────

    # ── 3. Doublons ───────────────────────────────────────────────────

    # ── 4. Catégories inconnues ───────────────────────────────────────

    # ── 5. Espaces superflus dans les titres et abstracts ─────────────

    return report


def print_diagnosis(report: dict, label: str):
    """Affiche le diagnostic de manière lisible."""
    print(f"\n  {'─' * 50}")
    print(f"  {label}")
    print(f"  {'─' * 50}")
    print(f"  Abstracts nulls          : {report['null_abstracts']:>6,}")
    print(f"  Abstracts < 50 car.      : {report['short_abstracts']:>6,}")
    print(f"  Titres nulls/vides       : {report['null_titles'] + report['empty_titles']:>6,}")
    print(f"  Doublons (arxiv_id)      : {report['duplicates']:>6,}")
    print(f"  Catégories inconnues     : {report['unknown_categories']:>6,}")
    print(f"  Titres avec multi-espaces: {report['multi_space_titles']:>6,}")


def clean(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applique les corrections de qualité.
    Retourne un nouveau DataFrame nettoyé.
    """
    original_len = len(df)
    df = df.copy()

    # ── Correction 1 : Supprimer les doublons ──────────────────────
    # Garder la première occurrence (souvent la plus récente)

    # ── Correction 2 : Nettoyer les abstracts ──────────────────────
    # Remplacer les retours à la ligne par des espaces
    # Marquer les abstracts trop courts (ne pas les supprimer, les signaler)

    # ── Correction 3 : Nettoyer les titres ─────────────────────────

    # ── Correction 4 : Normaliser les noms d'auteurs ──────────────
    # Supprimer les espaces superflus dans la chaîne d'auteurs

    # ── Correction 5 : Assurer la cohérence catégories ─────────────
    # Si primary_category est null mais categories est non-vide, extraire

    print(f"  ✅ Nettoyage terminé — {len(df):,} articles restants")
    return df


def reindex_cleaned(es: Elasticsearch, df: pd.DataFrame):
    """
    Réindexe les données nettoyées dans Elasticsearch.
    """
    print(f"\n🔄 Réindexation de {len(df):,} articles nettoyés...")

    def generate_actions():
        """
        Générateur 
        """

    start = time.time()
    success, errors = helpers.bulk(es, generate_actions(), chunk_size=BATCH_SIZE, raise_on_error=False)
    elapsed = time.time() - start

    # Réindexation et affichage des résultats
    pass

def main():
    parser = argparse.ArgumentParser(description="SciPulse — Nettoyage ArXiv")
    parser.add_argument("--dry-run", action="store_true", help="Simuler sans réindexer")
    args = parser.parse_args()

    print()
    print("╔══════════════════════════════════════════════════════════╗")
    print("║  SciPulse — Nettoyage des données ArXiv                 ║")
    print("╚══════════════════════════════════════════════════════════╝")

    #0. Connexion à Elasticsearch

    # 1. Charger
    print("\n📥 Chargement des données...")
    pass
    
    # 2. Diagnostic du dataframe AVANT
    pass

    # 3. Nettoyer
    print(f"\n🧹 Nettoyage en cours...")
    pass

    # 4. Diagnostic du dataframe APRÈS
    pass

    # 5. Comparer
    print(f"\n  {'─' * 50}")
    print(f"  AMÉLIORATION")
    print(f"  {'─' * 50}")
    for key in report_before:
        before = report_before[key]
        after = report_after[key]
        delta = before - after
        if delta > 0:
            print(f"  📉 {key:<28} {before:>6,} → {after:>6,} ({delta:>+6,})")
        elif delta == 0 and before > 0:
            print(f"  ── {key:<28} {before:>6,} → {after:>6,} (inchangé)")

    # 6. Réindexer (sauf dry-run)


if __name__ == "__main__":
    main()
```

### 5.1. Exécuter d'abord en mode dry-run

```bash
python -m src.quality.clean_arxiv --dry-run
```

Lisez le rapport avant/après. Vérifiez que les corrections sont raisonnables :

- Combien de doublons ont été supprimés ?
- Combien d'abstracts courts ont été identifiés ?
- Combien de catégories primaires ont été reconstruites ?

### 5.2. Exécuter le nettoyage réel

Si le rapport vous semble correct :

```bash
python -m src.quality.clean_arxiv
```

L'opération réindexe les documents corrigés dans Elasticsearch. Les documents existants sont écrasés (grâce à l'`_id`).

---

# PARTIE E — Re-validation (15 min)

## Étape 6 — Revalider après nettoyage

C'est le moment de vérité : les validations passent-elles mieux après le nettoyage ?

```bash
python -m src.quality.validate_arxiv
```

Comparez les résultats avec la première exécution (étape 3.4) :

| Expectation | Avant nettoyage | Après nettoyage |
|-------------|----------------|-----------------|
| abstract not_be_null | ___% | ___% |
| abstract longueur | ___% | ___% |
| arxiv_id unique | ___% | ___% |
| primary_category in_set | ___% | ___% |

Vous devriez observer une amélioration sur la plupart des expectations, en particulier sur les doublons (0 après dédupliation) et les catégories primaires (moins de nulls).

---

## Étape 7 — Consulter les Data Docs (10 min)

### 7.1. Ouvrir le rapport HTML

Les Data Docs ont été générés automatiquement. Ouvrez-les :

```bash
# Sur macOS :
open gx/uncommitted/data_docs/local_site/index.html

# Sur Linux :
xdg-open gx/uncommitted/data_docs/local_site/index.html

# Ou trouvez le fichier manuellement :
find . -name "index.html" -path "*/data_docs/*"
```

### 7.2. Explorer le rapport

Le rapport HTML contient :

1. **La liste des suites** : `arxiv_quality_suite` et `hn_quality_suite`.
2. **Pour chaque suite, les résultats de la dernière exécution** :
   - Vert = expectation réussie.
   - Rouge = expectation échouée.
   - Pour chaque expectation : le nombre de valeurs testées, le nombre de non-conformes, le pourcentage.
3. **Les valeurs problématiques** : un échantillon des valeurs qui ne respectent pas l'expectation (utile pour le diagnostic).

**Ce rapport est un livrable** : vous le présenterez lors de la soutenance du Jour 5 comme preuve que vos données ont été validées.

---

## Étape 8 — Commit (5 min)

```bash
git add src/quality/
git commit -m "feat: add Great Expectations validation suites + cleaning script"
```

---

## Checklist de fin de journée

- [ ] Great Expectations installé et fonctionnel
- [ ] Suite ArXiv écrite (10 expectations) et exécutée
- [ ] Suite HN écrite (6 expectations) et exécutée
- [ ] Problèmes de qualité identifiés et documentés
- [ ] Script de nettoyage exécuté (dry-run puis réel)
- [ ] Re-validation après nettoyage (amélioration constatée)
- [ ] Data Docs générées et consultées (rapport HTML)
- [ ] Commit Git propre

## Ce qui vient demain

Demain matin (Jour 3), nous passons à la **transformation et l'enrichissement** : dbt pour les transformations SQL, et un script Python pour l'enrichissement NLP (extraction de mots-clés) et le croisement ArXiv × Hacker News. L'après-midi sera consacré à la **fouille de texte avancée** dans Elasticsearch — `more_like_this`, `significant_terms`, `function_score`, et les requêtes parent-child.
