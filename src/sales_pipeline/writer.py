"""Écriture des résultats au format Parquet, partitionnés par order_date."""

from pyspark.sql import DataFrame


class SalesWriter:
    """Écrit un DataFrame au format Parquet, partitionné par une colonne donnée."""

    def write_parquet(
        self,
        df: DataFrame,
        path: str,
        partition_by: str = "order_date",
        mode: str = "overwrite",
    ) -> None:
        df.write.partitionBy(partition_by).mode(mode).parquet(path)
