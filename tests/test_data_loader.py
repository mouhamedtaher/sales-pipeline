from sales_pipeline.data_loader import SalesDataLoader


def test_load_reads_header_and_returns_string_columns(spark, tmp_path):
    csv_path = tmp_path / "sales.csv"
    csv_path.write_text(
        "order_id,customer_id,product_id,quantity,price,order_date\n"
        "O0001,C001,P001,2,10.5,2025-01-01\n"
        "O0002,C002,P002,1,20.0,2025-01-02\n"
    )

    loader = SalesDataLoader(spark)
    df = loader.load(str(csv_path))

    assert df.count() == 2
    assert df.columns == [
        "order_id",
        "customer_id",
        "product_id",
        "quantity",
        "price",
        "order_date",
    ]
    assert dict(df.dtypes)["quantity"] == "string"
