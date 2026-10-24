from elasticsearch import Elasticsearch

es = Elasticsearch("http://localhost:9200")
INDEX_NAME = "hn-items"

MAPPINGS = {
    "properties": {
        "id": {"type": "keyword"},
        "by": {"type": "keyword"},
        "title": {"type": "text", "analyzer": "standard"},
        "text": {"type": "text", "analyzer": "standard"},
        "type": {"type": "keyword"}, # 'story' ou 'comment'
        "time": {"type": "date", "format": "epoch_second"},
        # LE CHAMP MAGIQUE : La relation
        "item_relation": {
            "type": "join",
            "relations": {
                "story": "comment" # La story est le parent du comment
            }
        }
    }
}

def create_hn_index():
    if es.indices.exists(index=INDEX_NAME):
        es.indices.delete(index=INDEX_NAME)
    es.indices.create(index=INDEX_NAME, mappings=MAPPINGS)
    print(f"✅ Index {INDEX_NAME} créé avec succès !")

if __name__ == "__main__":
    create_hn_index()