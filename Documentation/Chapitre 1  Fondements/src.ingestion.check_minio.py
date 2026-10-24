import boto3
import os
from botocore.client import Config

# Configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS   = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET   = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BUCKET         = "arxiv-raw"

# Création du client S3
s3 = boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS,
    aws_secret_access_key=MINIO_SECRET,
    config=Config(signature_version='s3v4')
)

print(f"🧐 Connexion à MinIO ({MINIO_ENDPOINT}) pour vérification...")

# 1) Lire les objets du bucket arxiv-raw
try:
    response = s3.list_objects_v2(Bucket=BUCKET)
    
    if 'Contents' in response:
        print(f"✅ Objets trouvés dans le bucket '{BUCKET}' :\n")
        
        # 2) Pour tous les objets lus, afficher la clef et la taille
        for obj in response['Contents']:
            key = obj['Key']
            size_mb = obj['Size'] / (1024 * 1024)
            print(f" - Clé : {key:<40} | Taille : {size_mb:>6.2f} MB")
            
    else:
        print(f"⚠️ Le bucket '{BUCKET}' est vide.")

except Exception as e:
    print(f"❌ Erreur lors de la lecture : {e}")