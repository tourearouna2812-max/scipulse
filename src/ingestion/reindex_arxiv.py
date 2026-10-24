from elasticsearch import Elasticsearch, helpers
import logging

# Configuration
SOURCE_INDEX = "arxiv-papers-raw-py"
DEST_INDEX = "arxiv-papers"
ES_HOST = "http://localhost:9200"

es = Elasticsearch(ES_HOST)
logging.basicConfig(level=logging.INFO)

def parse_authors(authors_string):
    """Transforme 'A. Toure, M. Cadennes' en [{'first_name': 'A.', 'last_name': 'Toure'}, ...]"""
    if not authors_string: return []
    author_list = []
    # Découpage simple par virgule
    names = authors_string.split(',')
    for name in names:
        parts = name.strip().split(' ')
        if len(parts) >= 2:
            author_list.append({
                "first_name": parts[0],
                "last_name": " ".join(parts[1:]),
                "full_name": name.strip()
            })
        else:
            author_list.append({"full_name": name.strip()})
    return author_list

def migrate_data():
    print(f"🔄 Début de la réindexation de {SOURCE_INDEX} vers {DEST_INDEX}...")
    
    # Utilisation de scan pour lire les 100k docs sans saturer la RAM
    success, failed = 0, 0
    actions = []
    
    for hit in helpers.scan(es, index=SOURCE_INDEX):
        source_data = hit['_source']
        
        # Transformation : on nettoie les auteurs
        if 'authors' in source_data and isinstance(source_data['authors'], str):
            source_data['authors'] = parse_authors(source_data['authors'])
            
        action = {
            "_index": DEST_INDEX,
            "_id": hit['_id'],
            "_source": source_data
        }
        actions.append(action)
        
        # Envoi par paquets de 1000
        if len(actions) >= 1000:
            helpers.bulk(es, actions)
            success += len(actions)
            print(f"Processed {success} documents...")
            actions = []
            
    # Dernier paquet
    if actions:
        helpers.bulk(es, actions)
        success += len(actions)
        
    print(f"✅ Réindexation terminée ! {success} docs migrés.")

if __name__ == "__main__":
    migrate_data()