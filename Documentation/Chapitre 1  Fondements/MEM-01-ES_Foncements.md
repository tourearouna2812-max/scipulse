# Mémento du TP 1

Les fonctions et méthodes utiles sont listées avec leur contexte d'usage principal.[^1]

## Stdlib Python

- `json.loads(line)` : Parser une ligne JSON (article ArXiv) en dict.[^1]
- `json.tool` : Pretty-print JSON (via `python -m json.tool`).[^1]
- `os.getenv(var, default)` : Récupérer variables d'environnement (ex. `MINIO_ENDPOINT`).[^1]
- `os.path.basename(path)` : Nom du fichier depuis chemin complet.[^1]
- `sys.argv[^1]` : Argument CLI (chemin fichier source).[^1]
- `sys.stdin` : Lecture stdin pour comptage catégories.[^1]
- `sys.exit(1)` : Sortie en erreur si fichier absent.[^1]
- `open(file, 'r/w')` : Lecture/écriture fichiers (dump ArXiv, subset).[^1]
- `enumerate(f, 1)` : Itération lignes avec compteur pour chunks.[^1]


## boto3 (MinIO/S3)

- `boto3.client('s3', ...)` : Client S3 avec endpoint MinIO, clés, config `signature_version='s3v4'`.[^1]
- `s3.upload_file(localpath, bucket, key)` : Upload fichier local vers bucket.[^1]
- `s3.put_object(Bucket, Key, Body)` : Upload objet mémoire (chunk JSON).[^1]
- `s3.list_objects_v2(Bucket)` : Lister objets dans bucket `arxiv-raw`.[^1]


## botocore

- `Config(signature_version='s3v4')` : Config client S3 pour MinIO.[^1]


## Autres utilitaires

Le fichier mentionne aussi `kaggle` (CLI pour dataset), `wc -l`/`head` (bash inspection), et Kibana Dev Tools (ES queries : `PUT/GET/DELETE`, `search`, `mapping`), mais pas de code Python supplémentaire. Pas d'`elasticsearch` dans cet énoncé (prévu Jour 2).[^1]

<div align="center">⁂</div>

[^1]: TP-E01_ES_Fondements.md
