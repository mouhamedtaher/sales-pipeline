"""Calcul des colonnes dérivées (chiffre d'affaires par commande)."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col


class SalesTransformer:
    """Ajoute les colonnes calculées nécessaires aux agrégations."""

    def add_revenue(self, df: DataFrame) -> DataFrame:
        """Ajoute la colonne 'ca' = quantity * price."""
        return df.withColumn("ca", col("quantity") * col("price"))
