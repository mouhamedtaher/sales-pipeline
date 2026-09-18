"""Test d'intégration : vérifie l'orchestration complète SalesPipelineJob."""

from sales_pipeline.job import SalesPipelineJob


def test_pipeline_end_to_end(spark, tmp_path):
    csv_path = tmp_path / "sales.csv"
    csv_path.write_text(
        "order_id,customer_id,product_id,quantity,price,order_date\n"
        "O0001,C001,P001,2,10.0,2025-01-01\n"
        "O0002,C002,P002,1,50.0,2025-01-01\n"
        "O0003,C001,P001,0,10.0,2025-01-02\n"  # quantity aberrante -> exclue
        "O0004,,P003,3,20.0,2025-01-02\n"       # customer_id null -> exclue
        "O0005,C003,P003,4,25.0,2025-01-02\n"
    )
    output_path = str(tmp_path / "result")

    job = SalesPipelineJob(spark=spark, input_path=str(csv_path), output_path=output_path)
    result_df = job.run()

    assert result_df.count() == 3  # O0001, O0002, O0005 seulement

    read_back = spark.read.parquet(output_path)
    assert read_back.count() == 3

    partitions = sorted(p.name for p in (tmp_path / "result").iterdir() if p.is_dir())
    assert partitions == ["order_date=2025-01-01", "order_date=2025-01-02"]
