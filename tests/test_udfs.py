import pytest
import hashlib
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.getOrCreate()

@pytest.mark.parametrize("input_val, expected", [
    ("ELECT.", "Electronics"),
    ("app", "Apparel"),
    ("BEAUTY PRODUCTS", "Beauty"),
    (None, "Unknown")
])
def test_actual_normalize_category(spark, input_val, expected):
    val = f"'{input_val}'" if input_val is not None else "NULL"
    query = f"SELECT capstone_project.capstone_schema.udf_normalize_category({val}) as res"
    actual = spark.sql(query).collect()[0]['res']
    assert actual == expected

def test_actual_mask_customer_id(spark):
    cust_id = "USER_123"
    expected_hash = hashlib.sha256(cust_id.encode()).hexdigest()
    query = f"SELECT capstone_project.capstone_schema.udf_mask_customer_id('{cust_id}') as res"
    actual = spark.sql(query).collect()[0]['res']
    assert actual == expected_hash

@pytest.mark.parametrize("c_id, p_id, qty, price, disc, expected_score", [
    ("'C1'", "'P1'", 10, 20.0, 0.1, 1.0),
    ("NULL", "'P1'", 10, 20.0, 0.1, 0.8),
])
def test_actual_quality_score(spark, c_id, p_id, qty, price, disc, expected_score):
    query = f"""
        SELECT capstone_project.capstone_schema.udf_compute_quality_score(
            {c_id}, {p_id}, {qty}, {price}, {disc}
        ) as res
    """
    actual = spark.sql(query).collect()[0]['res']
    assert actual == pytest.approx(expected_score)
