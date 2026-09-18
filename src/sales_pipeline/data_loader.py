"""Chargement des données brutes de ventes depuis une source (S3, local, etc.)."""

from pyspark.sql import DataFrame, SparkSession


class SalesDataLoader:
    """Charge un fichier CSV de ventes dans un DataFrame Spark."""

    def __init__(self, spark: SparkSession):
        self._spark = spark

    def load(self, path: str) -> DataFrame:
        """Lit un CSV avec en-tête et retourne le DataFrame brut (colonnes en string)."""
        return (
            self._spark.read
            .option("header", True)
            .option("inferSchema", False)
            .csv(path)
        )
