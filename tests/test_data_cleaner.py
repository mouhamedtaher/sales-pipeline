from sales_pipeline.data_cleaner import SalesDataCleaner

RAW_COLUMNS = ["order_id", "customer_id", "product_id", "quantity", "price", "order_date"]

RAW_ROWS = [
    ("O0001", "C001", "P001", "2", "10.5", "2025-01-01"),
    ("O0002", None, "P002", "1", "20.0", "2025-01-02"),
    ("O0003", "C003", "P003", "0", "15.0", "2025-01-03"),
    ("O0004", "C004", "P004", "3", "-5.0", "2025-01-04"),
    ("O0005", "C005", "P005", "abc", "10.0", "2025-01-05"),
    ("O0006", "C006", "P006", "1", "10.0", "not-a-date"),
    ("O0007", "C007", "P007", "5", "9.99", "2025-01-07"),
]


def _raw_df(spark):
    return spark.createDataFrame(RAW_ROWS, RAW_COLUMNS)


def test_clean_removes_nulls_and_aberrant_values(spark):
    df = _raw_df(spark)

    cleaned = SalesDataCleaner().clean(df)
    rows = cleaned.orderBy("order_id").collect()

    assert [r["order_id"] for r in rows] == ["O0001", "O0007"]


def test_clean_casts_types(spark):
    df = _raw_df(spark)

    cleaned = SalesDataCleaner().clean(df)
    dtypes = dict(cleaned.dtypes)

    assert dtypes["quantity"] == "int"
    assert dtypes["price"] == "double"
    assert dtypes["order_date"] == "date"
