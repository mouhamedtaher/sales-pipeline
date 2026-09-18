from datetime import date

from sales_pipeline.aggregator import SalesAggregator

COLUMNS = ["order_id", "customer_id", "product_id", "quantity", "order_date", "ca"]

ROWS = [
    ("O0001", "C001", "P001", 5, date(2025, 1, 1), 50.0),
    ("O0002", "C001", "P002", 1, date(2025, 1, 1), 10.0),
    ("O0003", "C002", "P001", 2, date(2025, 1, 2), 20.0),
    ("O0004", "C002", "P003", 10, date(2025, 1, 2), 300.0),
    ("O0005", "C003", "P002", 1, date(2025, 1, 3), 5.0),
]


def _df(spark):
    return spark.createDataFrame(ROWS, COLUMNS)


def test_revenue_per_day(spark):
    result = {
        r["order_date"]: r["total_ca_per_date"]
        for r in SalesAggregator().revenue_per_day(_df(spark)).collect()
    }

    assert result[date(2025, 1, 1)] == 60.0
    assert result[date(2025, 1, 2)] == 320.0
    assert result[date(2025, 1, 3)] == 5.0


def test_top_products_orders_by_total_quantity_desc(spark):
    top = SalesAggregator().top_products(_df(spark), n=2).collect()

    assert [r["product_id"] for r in top] == ["P003", "P001"]
    assert top[0]["total_quantity_per_product"] == 10
    assert top[1]["total_quantity_per_product"] == 7


def test_top_customer_returns_highest_revenue_customer(spark):
    customer_id, total_ca = SalesAggregator().top_customer(_df(spark))

    assert customer_id == "C002"
    assert total_ca == 320.0


def test_top_customer_returns_none_on_empty_dataframe(spark):
    empty_df = spark.createDataFrame([], schema=_df(spark).schema)

    assert SalesAggregator().top_customer(empty_df) is None


def test_orders_per_customer(spark):
    top = SalesAggregator().orders_per_customer(_df(spark), n=1).collect()

    assert top[0]["total_orders_per_customer"] == 2
