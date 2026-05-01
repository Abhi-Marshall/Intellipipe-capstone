import pytest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import expr

@pytest.fixture(scope="session")
def spark():
    """Access the existing Databricks Spark session."""
    return SparkSession.builder.getOrCreate()

def test_silver_cleansing_logic(spark):
    """
    Tests the Silver layer logic: 
    1. Null handling (COALESCE)
    2. Quality Filter (score >= 0.7)
    """
    # 1. CREATE MOCK BRONZE DATA
    # We include one "Good" record and one "Dirty" record that should be dropped
    bronze_data = [
        # Good record: should pass
        Row(order_id="101", customer_id="C1", product_id="P1", quantity=10, unit_price=20.0, discount=0.1, category="ELECT."),
        # Dirty record: missing product_id and negative price (will get score < 0.7)
        Row(order_id="102", customer_id="C2", product_id=None, quantity=10, unit_price=-5.0, discount=None, category="app")
    ]
    bronze_df = spark.createDataFrame(bronze_data)

    # 2. APPLY THE SILVER TRANSFORMATIONS
    # This replicates the logic in your 02_silver_cleansing notebook
    silver_df = (
        bronze_df
        .withColumn("discount", expr("COALESCE(discount, 0.0)"))
        .withColumn("category", expr("capstone_project.capstone_schema.udf_normalize_category(category)"))
        .withColumn("customer_id", expr("capstone_project.capstone_schema.udf_mask_customer_id(customer_id)"))
        .withColumn("quality_score", expr("""
            capstone_project.capstone_schema.udf_compute_quality_score(
                customer_id, product_id, quantity, unit_price, discount
            )
        """))
    )

    # Apply the Medallion "Quality Gate"
    clean_orders = silver_df.filter("quality_score >= 0.7")

    # 3. ASSERTIONS
    results = clean_orders.collect()
    
    # Check 1: The dirty record (102) should have been filtered out
    assert len(results) == 1
    assert results[0]["order_id"] == "101"

    # Check 2: The category should be normalized (ELECT. -> Electronics)
    assert results[0]["category"] == "Electronics"

    # Check 3: The discount should be a float (0.1) and not Null
    assert isinstance(results[0]["discount"], float)

def test_gold_aggregation_logic(spark):
    """Verifies that the Gold layer correctly sums values."""
    # Mock Silver Data
    silver_data = [
        Row(order_id="101", quantity=2, unit_price=10.0),
        Row(order_id="102", quantity=1, unit_price=10.0)
    ]
    df = spark.createDataFrame(silver_data)
    
    # Apply simple aggregation logic (Total Revenue)
    gold_df = df.selectExpr("sum(quantity * unit_price) as total_revenue")
    
    total = gold_df.collect()[0]["total_revenue"]
    assert total == 30.0