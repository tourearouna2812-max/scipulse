import json
import os
import sys
import time
from elasticsearch import Elasticsearch, helpers

# Configuration
ES_HOST    = os.getenv("ES_HOST", "http://localhost:9200")
INDEX_NAME = "arxiv-papers-raw-py"
BATCH_SIZE = 1000

def transform_article(raw: dict) -> dict:
    """Nettoyage équivalent aux filtres Logstash."""
    categories = raw.get("categories", "").split()
    
    return {
        "arxiv_id": raw.get("id"),
        "title": raw.get("title", "").strip().replace("\n", " "),
        "abstract": raw.get("abstract", "").strip().replace("\n", " "),
        "categories": categories,
        "primary_category": categories[0] if categories else None,
        "authors": raw.get("authors"),
        "date_published": raw.get("update_date"),
        "ingestion_source": "python-script"
    }


#5.3 & 5.4. Le Générateur Bulk et l'Ingestion
def generate_actions(filepath: str):
    """Générateur pour économiser la RAM (yield)."""
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            try:
                raw_article = json.loads(line)
                transformed = transform_article(raw_article)
                
                if transformed["arxiv_id"]:
                    yield {
                        "_index": INDEX_NAME,
                        "_id": transformed["arxiv_id"],
                        "_source": transformed
                    }
            except json.JSONDecodeError:
                continue

def ingest(filepath: str):
    es = Elasticsearch(ES_HOST)
    if es.indices.exists(index=INDEX_NAME):
        es.indices.delete(index=INDEX_NAME)
        print(f"Index {INDEX_NAME} supprime.")
        #print(f"🗑 Index {INDEX_NAME} supprimé.")

    start_time = time.time()
    success_count = 0
    
    print(f"📥 Ingestion de {filepath}...")
    
    # Utilisation du helpers.streaming_bulk pour l'efficacité
    for ok, result in helpers.streaming_bulk(es, generate_actions(filepath), chunk_size=BATCH_SIZE):
        if ok:
            success_count += 1
        if success_count % 10000 == 0:
            print(f"📊 {success_count} documents traités...")

    es.indices.refresh(index=INDEX_NAME)
    print(f"✅ Terminé ! {success_count} docs en {time.time()-start_time:.1f}s")

if __name__ == "__main__":
    # On cible le fichier téléchargé ce matin
    path = "data/arxiv-raw/arxiv-cs-subset-100k.json"
    ingest(path)