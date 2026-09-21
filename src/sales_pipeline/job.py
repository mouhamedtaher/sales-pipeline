"""Orchestrateur du pipeline de traitement des ventes."""

import logging
from typing import Optional

from pyspark.sql import DataFrame, SparkSession

from .aggregator import SalesAggregator
from .data_cleaner import SalesDataCleaner
from .data_loader import SalesDataLoader
from .transformer import SalesTransformer
from .writer import SalesWriter

logger = logging.getLogger(__name__)


class SalesPipelineJob:
    """Orchestre le pipeline complet de traitement des ventes."""

    def __init__(
        self,
        spark: SparkSession,
        input_path: str,
        output_path: str,
        loader: Optional[SalesDataLoader] = None,
        cleaner: Optional[SalesDataCleaner] = None,
        transformer: Optional[SalesTransformer] = None,
        aggregator: Optional[SalesAggregator] = None,
        writer: Optional[SalesWriter] = None,
    ):
        self._spark = spark
        self._input_path = input_path
        self._output_path = output_path
        self._loader = loader or SalesDataLoader(spark)
        self._cleaner = cleaner or SalesDataCleaner()
        self._transformer = transformer or SalesTransformer()
        self._aggregator = aggregator or SalesAggregator()
        self._writer = writer or SalesWriter()

    def run(self) -> DataFrame:
        """Exécute le pipeline complet et retourne le DataFrame final écrit."""
        raw_df = self._loader.load(self._input_path)
        clean_df = self._cleaner.clean(raw_df)
        enriched_df = self._transformer.add_revenue(clean_df)

        self._log_aggregations(enriched_df)

        self._writer.write_parquet(
            enriched_df, self._output_path, partition_by="order_date"
        )
        return enriched_df

    def _log_aggregations(self, df: DataFrame) -> None:
        revenue_per_day = self._aggregator.revenue_per_day(df)
        logger.info("Chiffre d'affaires total par jour:")
        revenue_per_day.show(50, truncate=False)

        top_products = self._aggregator.top_products(df, n=5)
        logger.info("Top 5 produits les plus vendus (quantité):")
        top_products.show(5, truncate=False)

        top_customer = self._aggregator.top_customer(df)
        logger.info("Client ayant généré le plus de CA: %s", top_customer)
