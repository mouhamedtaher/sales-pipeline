"""Agrégations métier : CA par jour, top produits, meilleur client."""
"""Agrégations métier : CA par jour, top produits, meilleur client."""
"""Agrégations métier : CA par jour, top produits, meilleur client."""
from pyspark.sql import DataFrame
from pyspark.sql.functions import count, desc
from pyspark.sql.functions import sum as _sum


class SalesAggregator:
    """Calcule les agrégations demandées par l'exercice."""

    def revenue_per_day(self, df: DataFrame) -> DataFrame:
        """Chiffre d'affaires total par jour (order_date)."""
        return (
            df.groupBy("order_date")
            .agg(_sum("ca").alias("total_ca_per_date"))
            .orderBy("order_date")
        )

    def top_products(self, df: DataFrame, n: int = 5) -> DataFrame:
        """Top n produits les plus vendus par quantité totale."""
        return (
            df.groupBy("product_id")
            .agg(_sum("quantity").alias("total_quantity_per_product"))
            .orderBy(desc("total_quantity_per_product"))
            .limit(n)
        )

    def top_customer(self, df: DataFrame):
        """Retourne (customer_id, total_ca) du client ayant généré le plus de CA.

        Retourne None si le DataFrame est vide.
        """
        top_row = (
            df.groupBy("customer_id")
            .agg(_sum("ca").alias("total_ca_per_customer"))
            .orderBy(desc("total_ca_per_customer"))
            .limit(1)
            .collect()
        )
        if not top_row:
            return None
        return top_row[0]["customer_id"], top_row[0]["total_ca_per_customer"]

    def orders_per_customer(self, df: DataFrame, n: int = 10) -> DataFrame:
        """Top n clients par nombre de commandes."""
        return (
            df.groupBy("customer_id")
            .agg(count("order_id").alias("total_orders_per_customer"))
            .orderBy(desc("total_orders_per_customer"))
            .limit(n)
        )
