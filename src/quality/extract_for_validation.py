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