from sales_pipeline.transformer import SalesTransformer


def test_add_revenue_multiplies_quantity_by_price(spark):
    df = spark.createDataFrame(
        [("O0001", 2, 10.0), ("O0002", 3, 5.5)],
        ["order_id", "quantity", "price"],
    )

    result = SalesTransformer().add_revenue(df)
    rows = {r["order_id"]: r["ca"] for r in result.collect()}

    assert rows["O0001"] == 20.0
    assert rows["O0002"] == 16.5
