from datetime import date

from sales_pipeline.writer import SalesWriter


def test_write_parquet_partitions_by_order_date(spark, tmp_path):
    df = spark.createDataFrame(
        [
            ("O0001", date(2025, 1, 1), 20.0),
            ("O0002", date(2025, 1, 2), 30.0),
        ],
        ["order_id", "order_date", "ca"],
    )
    output_path = str(tmp_path / "result")

    SalesWriter().write_parquet(df, output_path, partition_by="order_date")

    partitions = sorted(p.name for p in (tmp_path / "result").iterdir() if p.is_dir())
    assert partitions == ["order_date=2025-01-01", "order_date=2025-01-02"]

    read_back = df.sparkSession.read.parquet(output_path)
    assert read_back.count() == 2


def test_write_parquet_overwrite_mode_replaces_existing_data(spark, tmp_path):
    output_path = str(tmp_path / "result")
    writer = SalesWriter()

    df1 = df = spark.createDataFrame(
        [("O0001", date(2025, 1, 1), 20.0)], ["order_id", "order_date", "ca"]
    )
    writer.write_parquet(df1, output_path)

    df2 = spark.createDataFrame(
        [("O0002", date(2025, 1, 1), 99.0)], ["order_id", "order_date", "ca"]
    )
    writer.write_parquet(df2, output_path)

    read_back = df.sparkSession.read.parquet(output_path)
    rows = read_back.collect()
    assert len(rows) == 1
    assert rows[0]["order_id"] == "O0002"
