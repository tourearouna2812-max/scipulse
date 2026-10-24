import json
import os

# Chemins avec extensions .json
INPUT = "data/arxiv-metadata-oai-snapshot.json"
OUTPUT = "data/arxiv-raw/arxiv-cs-subset-100k.json"

MAX = 100000
count = 0

print(f" Lecture depuis : {INPUT}")
print(f" Écriture vers : {OUTPUT}")

if not os.path.exists(INPUT):
    print(f"❌ ERREUR : Le fichier {INPUT} est introuvable !")
    exit(1)

# Création du dossier de destination si besoin
os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)

with open(INPUT, "r", encoding="utf-8") as fin, open(OUTPUT, "w", encoding="utf-8") as fout:
    for line in fin:
        try:
            data = json.loads(line)
            categories = data.get("categories", "")

            # Filtre : Informatique (cs.*) ou Machine Learning (stat.ML)
            if "cs." in categories or "stat.ML" in categories:
                fout.write(line)
                count += 1

            if count % 10000 == 0 and count > 0:
                print(f"⏳ Progression : {count} articles extraits...")

            if count >= MAX:
                break

        except json.JSONDecodeError:
            continue

print(f" Terminé : {count} articles écrits dans {OUTPUT}")
