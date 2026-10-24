from elasticsearch import Elasticsearch

# Configuration
INDEX_NAME = "arxiv-papers"
ES_HOST = "http://localhost:9200"

es = Elasticsearch(ES_HOST)

SETTINGS = {
    "index": {
        "number_of_shards": 1,
        "number_of_replicas": 0 # Pour éviter le statut "yellow" sur un seul nœud
    },
    "analysis": {
        "filter": {
            "scientific_stop": {"type": "stop", "stopwords": "_english_"},
            "scientific_stemmer": {"type": "stemmer", "language": "english"},
            "scientific_synonyms": {
                "type": "synonym",
                "synonyms": [
                    "cnn, convolutional neural network",
                    "nlp, natural language processing",
                    "ml, machine learning",
                    "ai, artificial intelligence"
                ]
            }
        },
        "analyzer": {
            "scientific_english": {
                "type": "custom",
                "tokenizer": "standard",
                "filter": [
                    "lowercase",
                    "scientific_synonyms",
                    "scientific_stop",
                    "scientific_stemmer"
                ]
            }
        }
    }
}

MAPPINGS = {
    "properties": {
        "arxiv_id": {"type": "keyword"},
        "title": {
            "type": "text",
            "analyzer": "scientific_english",
            "fields": {
                "keyword": {"type": "keyword"},
                "suggest": {"type": "completion"} # Pour l'auto-complétion
            }
        },
        "abstract": {
            "type": "text",
            "analyzer": "scientific_english"
        },
        "authors": {
            "type": "nested", # Très important pour l'étape 4 du TP
            "properties": {
                "first_name": {"type": "keyword"},
                "last_name": {"type": "keyword"},
                "full_name": {"type": "text", "analyzer": "standard"}
            }
        },
        "primary_category": {"type": "keyword"},
        "categories": {"type": "keyword"},
        "date_published": {"type": "date"},
        "ingestion_source": {"type": "keyword"}
    }
}

def create_index():
    if es.indices.exists(index=INDEX_NAME):
        print(f"Index {INDEX_NAME} déjà existant. Suppression...")
        es.indices.delete(index=INDEX_NAME)
    
    print(f"Création de l'index {INDEX_NAME} avec mapping explicite...")
    es.indices.create(index=INDEX_NAME, settings=SETTINGS, mappings=MAPPINGS)
    print("✅ Index créé avec succès !")

if __name__ == "__main__":
    create_index()