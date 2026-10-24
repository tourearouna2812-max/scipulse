import json
import os
import sys
import boto3
from botocore.client import Config

# ── Configuration MinIO ─────────────────────────────────────────────
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS   = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET   = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BUCKET         = "arxiv-raw"

# ── Connexion ────────────────────────────────────────────────────────

# Initialisation du client avec la signature v4 (requise par MinIO)
s3 = boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS,
    aws_secret_access_key=MINIO_SECRET,
    config=Config(signature_version='s3v4')
)

def upload_file(local_path: str, object_name: str | None = None):
    """Upload un fichier complet vers MinIO."""
    if object_name is None:
        object_name = os.path.basename(local_path)
    
    print(f"📤 Uploading {local_path} to {BUCKET}/{object_name}...")
    s3.upload_file(local_path, BUCKET, object_name)
    print("✅ Upload réussi.")

def _upload_chunk(lines: list[str], object_name: str):
    """Upload un chunk en mémoire vers MinIO sans créer de fichier local."""
    # On transforme la liste de lignes en un seul bloc de texte
    body = "".join(lines) 
    s3.put_object(Bucket=BUCKET, Key=object_name, Body=body)

def split_and_upload(source_file: str, chunk_size: int = 10_000, prefix: str = "chunks/"):
    """Découpe le fichier JSON lines en chunks et les upload."""
    print(f"✂️ Découpage de {source_file} en morceaux de {chunk_size}...")
    
    current_chunk = []
    chunk_count = 0
    
    with open(source_file, 'r', encoding='utf-8') as f:
        for i, line in enumerate(f, 1):
            current_chunk.append(line)
            
            if i % chunk_size == 0:
                chunk_count += 1
                obj_name = f"{prefix}arxiv_chunk_{chunk_count:03d}.json"
                _upload_chunk(current_chunk, obj_name)
                print(f"  📦 Chunk {chunk_count} envoyé ({obj_name})")
                current_chunk = [] # Reset du buffer
        
        # Envoi du dernier chunk s'il reste des lignes
        if current_chunk:
            chunk_count += 1
            _upload_chunk(current_chunk, f"{prefix}arxiv_chunk_{chunk_count:03d}.json")
            
    print(f"✅ Total : {chunk_count} chunks uploadés.")

if __name__ == "__main__":
    source = sys.argv[1] if len(sys.argv) > 1 else "data/arxiv-raw/arxiv-cs-subset-100k.json"

    # 1) Traiter l'erreur si la source n'existe pas
    if not os.path.exists(source):
        print(f"❌ Erreur : Le fichier {source} est introuvable.")
        sys.exit(1)

    # 2) Vérifier/Créer le bucket si nécessaire
    try:
        s3.head_bucket(Bucket=BUCKET)
    except:
        print(f"🪣 Création du bucket {BUCKET}...")
        s3.create_bucket(Bucket=BUCKET)

    # 3) Upload du fichier complet (la source brute)
    upload_file(source, "arxiv-full-subset.json")

    # 4) Découpage en chunks pour simuler un flux continu
    split_and_upload(source)