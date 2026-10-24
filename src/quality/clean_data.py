import sys
from elasticsearch import Elasticsearch, helpers

# 1. Connexion à Elasticsearch
es = Elasticsearch("http://localhost:9200")

def clean_arxiv_data():
    print("🧹 Début du nettoyage de l'index 'arxiv-papers'...")
    
    # Requête pour trouver les articles retirés/retractés (withdrawn)
    query = {
        "query": {
            "match": {
                "abstract": "withdrawn"
            }
        }
    }
    
    try:
        # On récupère les articles correspondants
        res = helpers.scan(es, index="arxiv-papers", query=query, size=1000)
        docs_to_update = []
        
        for hit in res:
            doc_id = hit["_id"]
            source = hit["_source"]
            
            # Action de mise à jour : on ajoute un tag ou un champ 'status' pour marquer l'article
            docs_to_update.append({
                "_op_type": "update",
                "_index": "arxiv-papers",
                "_id": doc_id,
                "doc": {
                    "status": "withdrawn",
                    "is_valid": False
                }
            })
            
        if docs_to_update:
            # Envoi des modifications par lot (bulk)
            success, errors = helpers.bulk(es, docs_to_update)
            print(f"✅ Nettoyage ArXiv terminé : {success} articles marqués comme 'withdrawn'.")
        else:
            print("✨ Aucun article 'withdrawn' trouvé à nettoyer.")
            
    except Exception as e:
        print(f"❌ Erreur lors du nettoyage ArXiv : {e}")

def clean_hn_data():
    print("\n🧹 Début du nettoyage de l'index 'hn-items'...")
    
    # Requête pour trouver les commentaires ou posts sans auteur ('by' manquant ou vide)
    query = {
        "query": {
            "bool": {
                "must_not": {
                    "exists": {
                        "field": "by"
                    }
                }
            }
        }
    }
    
    try:
        res = helpers.scan(es, index="hn-items", query=query, size=1000)
        docs_to_update = []
        
        for hit in res:
            doc_id = hit["_id"]
            
            # On attribue un auteur par défaut '[deleted]' comme le fait souvent Hacker News
            docs_to_update.append({
                "_op_type": "update",
                "_index": "hn-items",
                "_id": doc_id,
                "doc": {
                    "by": "[deleted]"
                }
            })
            
        if docs_to_update:
            success, errors = helpers.bulk(es, docs_to_update)
            print(f"✅ Nettoyage Hacker News terminé : {success} items mis à jour avec l'auteur '[deleted]'.")
        else:
            print("✨ Aucun item Hacker News incomplet trouvé.")
            
    except Exception as e:
        print(f"❌ Erreur lors du nettoyage Hacker News : {e}")

if __name__ == "__main__":
    print("╔══════════════════════════════════════════════════════════╗")
    print("║          SciPulse — Nettoyage des données                ║")
    print("╚══════════════════════════════════════════════════════════╝")
    
    clean_arxiv_data()
    clean_hn_data()
    print("\n🎉 Fin du processus de nettoyage !")