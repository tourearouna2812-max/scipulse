"""
SciPulse Insights — Fabrique de SparkSession partagée

Centralise la configuration Spark (connexion MinIO via S3A, connecteur
elasticsearch-hadoop, niveau de logs) pour que tous les jobs l'utilisent
de façon cohérente.

Remarque : quand un job est lancé par spark-submit (scripts/spark.ps1,
DAG Airflow), la JVM démarre AVANT ce code ; les jars sont donc passés
par --packages sur la ligne de commande. La ligne spark.jars.packages
ci-dessous ne sert que si l'on lance un job directement avec python3.
"""

from pyspark.sql import SparkSession

from src.spark_jobs import config


def get_spark_session(app_name: str = "SciPulse Insights") -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        # MinIO (compatible S3)
        .config("spark.hadoop.fs.s3a.endpoint", config.SPARK_S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", config.S3_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", config.S3_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # Connecteurs (hadoop-aws + elasticsearch-hadoop)
        .config("spark.jars.packages", config.SPARK_PACKAGES)
        .config("spark.jars.ivy", "/opt/ivy")
        # Volumes de TP : on évite 200 partitions de shuffle pour quelques Mo
        .config("spark.sql.shuffle.partitions", "32")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def path_exists(spark: SparkSession, path: str) -> bool:
    """Vrai si un chemin (s3a://…) existe — évite un AnalysisException sur une source absente."""
    jvm = spark._jvm
    hadoop_path = jvm.org.apache.hadoop.fs.Path(path)
    fs = hadoop_path.getFileSystem(spark._jsc.hadoopConfiguration())
    return fs.exists(hadoop_path)


if __name__ == "__main__":
    spark = get_spark_session()
    print(f"✅ SparkSession active — version {spark.version}")
    print(f"   Cœurs disponibles : {spark.sparkContext.defaultParallelism}")
    print(f"   Endpoint S3A      : {config.SPARK_S3_ENDPOINT}")
    spark.stop()
