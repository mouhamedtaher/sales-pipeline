"""Nettoyage des données de ventes : typage, valeurs nulles et valeurs aberrantes."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_date


class SalesDataCleaner:
    """Cast les colonnes dans leur type attendu et retire les lignes invalides.

    Règles appliquées :
      - quantity -> int, price -> double, order_date -> date
        (une valeur non castable devient null, donc capturée par la
        suppression des nulls ci-dessous)
      - suppression des lignes avec une valeur nulle sur une colonne clé
      - suppression des valeurs aberrantes : quantity <= 0 ou price <= 0
    """

    REQUIRED_COLUMNS = (
        "order_id",
        "customer_id",
        "product_id",
        "quantity",
        "price",
        "order_date",
    )

    def clean(self, df: DataFrame) -> DataFrame:
        typed_df = (
            df.withColumn("quantity", col("quantity").cast("int"))
            .withColumn("price", col("price").cast("double"))
            .withColumn("order_date", to_date(col("order_date")))
        )

        no_nulls_df = typed_df.dropna(subset=list(self.REQUIRED_COLUMNS))

        return no_nulls_df.filter((col("quantity") > 0) & (col("price") > 0))
