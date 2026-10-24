import requests
import time
import argparse
from elasticsearch import Elasticsearch

# Configuration
ES_HOST = "http://localhost:9200"
INDEX_NAME = "hn-items"
HN_API_BASE = "https://hacker-news.firebaseio.com/v0"

es = Elasticsearch(ES_HOST)

def get_item(item_id):
    """Récupère un item (story ou comment) depuis l'API HN"""
    response = requests.get(f"{HN_API_BASE}/item/{item_id}.json")
    return response.json()

def index_item(item):
    """Indexe l'item dans Elasticsearch avec la logique de JOIN"""
    if not item or item.get('deleted'):
        return

    doc = {
        "id": str(item['id']),
        "by": item.get('by'),
        "time": item.get('time'),
        "text": item.get('text'),
        "title": item.get('title'),
        "type": item.get('type')
    }

    # LOGIQUE DE RELATION (PARENT-ENFANT)
    if item['type'] == 'story':
        doc["item_relation"] = {"name": "story"}
        # Une story n'a pas de parent, pas besoin de routing spécifique
        es.index(index=INDEX_NAME, id=str(item['id']), document=doc)
        
    elif item['type'] == 'comment' and 'parent' in item:
        # Un commentaire est l'enfant d'une story
        doc["item_relation"] = {
            "name": "comment",
            "parent": str(item['parent'])
        }
        # CRUCIAL : Le routing doit être l'ID du parent pour être sur le même shard
        es.index(
            index=INDEX_NAME, 
            id=str(item['id']), 
            document=doc, 
            routing=str(item['parent']) 
        )

def run_poller(continuous=False, interval=60):
    print(f"🕵️ Démarrage du poller Hacker News sur {INDEX_NAME}...")
    
    while True:
        # On récupère les 10 derniers items publiés sur HN
        max_item_resp = requests.get(f"{HN_API_BASE}/maxitem.json")
        max_id = max_item_resp.json()
        
        print(f"Vérification des derniers items (Max ID: {max_id})...")
        
        # On indexe les 5 derniers pour l'exemple
        for i in range(max_id, max_id - 5, -1):
            if not es.exists(index=INDEX_NAME, id=str(i)):
                item = get_item(i)
                index_item(item)
                print(f"✅ Item {i} ({item.get('type')}) indexé.")
            else:
                print(f"⏭️ Item {i} déjà présent.")

        if not continuous:
            break
            
        print(f"Sleeping {interval}s...")
        time.sleep(interval)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--continuous", action="store_true")
    parser.add_argument("--interval", type=int, default=60)
    args = parser.parse_args()
    
    run_poller(continuous=args.continuous, interval=args.interval)