import pytest
pyspark = pytest.importorskip("pyspark")
from src.lending_pipeline.spark_pipeline import create_spark, read_source, CUSTOMER_SCHEMA, APPLICATION_SCHEMA, validate_and_transform
from src.lending_pipeline.generator import generate_data

def test_spark_transform_counts_and_categories(tmp_path):
    cp,ap=generate_data(tmp_path,8,30,5)
    spark=create_spark("LendingTest","local[2]",2)
    try:
        c=read_source(spark,cp,CUSTOMER_SCHEMA,"customers")
        a=read_source(spark,ap,APPLICATION_SCHEMA,"applications")
        valid_c, curated, rejects, metrics=validate_and_transform(c,a,5_000_000,"BASELINE","RUN_TEST")
        assert metrics["valid_customer_count"]==8
        assert metrics["valid_application_count"]==30
        assert metrics["output_count"]==30
        assert curated.select("application_id").distinct().count()==30
        assert set(r[0] for r in curated.select("loan_amount_category").distinct().collect()) <= {"SMALL","MEDIUM","LARGE"}
    finally: spark.stop()
