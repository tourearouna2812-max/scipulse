# SciPulse Insights — image Spark du projet
#
# Pourquoi pas bitnami/spark (proposée dans le TP) : Bitnami a retiré ses
# images gratuites de Docker Hub en 2025, le dépôt bitnami/spark ne contient
# plus aucun tag. On part donc de l'image officielle Apache Spark (Docker
# Official Image "spark"), en version 3.5.0 pour rester aligné sur
# pyspark==3.5.0 utilisé dans tout le module.
FROM spark:3.5.0-python3

USER root

# L'image spark:3.5.0 est basée sur Ubuntu 20.04 → Python 3.8 :
# versions choisies parmi les dernières compatibles 3.8 (numpy 1.24, pandas 2.0).

# Dépendances Python des jobs :
#  - numpy        : requis par pyspark.ml (TF-IDF, similarité cosinus)
#  - pandas       : benchmark Pandas vs Spark (J1)
#  - requests     : appels API OpenAlex / Hacker News (Algolia)
#  - boto3        : écriture Bronze dans MinIO
#  - elasticsearch: création du mapping de l'index Gold
#  - pytest       : tests unitaires des transformations
RUN (command -v pip3 >/dev/null 2>&1 || \
        (apt-get update && apt-get install -y --no-install-recommends python3-pip \
         && rm -rf /var/lib/apt/lists/*)) \
    && pip3 install --no-cache-dir \
        "numpy==1.24.4" \
        "pandas==2.0.3" \
        "requests>=2.31,<3" \
        "boto3>=1.34,<2" \
        "python-dotenv>=1.0,<2" \
        "elasticsearch>=8.13,<9" \
        "pytest>=7.4,<8.4"

# Les jobs importent "src.spark_jobs..." : la racine du projet monté
# (/opt/spark-apps) et PySpark doivent être dans le PYTHONPATH.
ENV PYTHONPATH=/opt/spark-apps:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip \
    PYSPARK_PYTHON=python3 \
    SPARK_HOME=/opt/spark

WORKDIR /opt/spark-apps
