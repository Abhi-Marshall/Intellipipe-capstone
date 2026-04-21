import pytest
import hashlib

# ==============================================================================
# INTREPID CAPSTONE: UNIT TESTS FOR SILVER LAYER UDFS
# ==============================================================================

# --- 1. The Core Logic (Imported from your src/udfs folder) ---
# Note: In a local IDE, you would use: 
# from src.udfs_setup import normalize_category, mask_customer_id, compute_quality_score

def normalize_category(cat: str) -> str:
    if not cat: return "Unknown"
    clean_cat = cat.strip().upper()
    category_mapping = {
        "ELECTRONICS": "Electronics", "ELECT.": "Electronics",
        "APPAREL": "Apparel", "APP": "Apparel",
        "BEAUTY": "Beauty", "BEAUTY PRODUCTS": "Beauty",
        "SPORTS": "Sports", "SPORT": "Sports"
    }
    return category_mapping.get(clean_cat, clean_cat.title())

def mask_customer_id(cust_id: str) -> str:
    if not cust_id: return None
    return hashlib.sha256(cust_id.encode()).hexdigest()

def compute_quality_score(customer_id, product_id, quantity, unit_price, discount):
    score = 1.0
    if not customer_id: score -= 0.2
    if not product_id: score -= 0.2
    if quantity is None or quantity < 1: score -= 0.3
    if unit_price is None or unit_price < 0.0: score -= 0.3
    if discount is None: score -= 0.1
    elif discount < 0.0 or discount > 1.0: score -= 0.2
    return max(0.0, round(score, 2))

# --- 2. Pytest Cases for Category Normalization ---
def test_normalize_category_exact_match():
    assert normalize_category("ELECTRONICS") == "Electronics"

def test_normalize_category_messy_input():
    assert normalize_category("  elect.  ") == "Electronics"
    assert normalize_category("app") == "Apparel"

def test_normalize_category_fallback():
    assert normalize_category("furniture") == "Furniture"

def test_normalize_category_null():
    assert normalize_category(None) == "Unknown"
    assert normalize_category("") == "Unknown"

# --- 3. Pytest Cases for PII Masking ---
def test_mask_customer_id():
    test_id = "CUST-12345"
    expected_hash = hashlib.sha256(test_id.encode()).hexdigest()
    assert mask_customer_id(test_id) == expected_hash

def test_mask_customer_id_null():
    assert mask_customer_id(None) is None

# --- 4. Pytest Cases for Quality Scoring ---
def test_compute_quality_score_perfect():
    # A perfect row should return 1.0
    assert compute_quality_score("CUST-1", "PROD-1", 5, 99.99, 0.1) == 1.0

def test_compute_quality_score_missing_ids():
    # Missing both IDs (-0.2 each)
    assert compute_quality_score(None, None, 5, 99.99, 0.1) == 0.6

def test_compute_quality_score_negative_values():
    # Negative quantity (-0.3) and negative price (-0.3)
    assert compute_quality_score("CUST-1", "PROD-1", -2, -50.0, 0.1) == 0.4

def test_compute_quality_score_bad_discount():
    # Discount > 1.0 (-0.2)
    assert compute_quality_score("CUST-1", "PROD-1", 5, 99.99, 1.5) == 0.8
    # Null discount (-0.1)
    assert compute_quality_score("CUST-1", "PROD-1", 5, 99.99, None) == 0.9

def test_compute_quality_score_worst_case():
    # Fails everything. Score should bottom out at 0.0, not go negative.
    assert compute_quality_score(None, None, -1, -1.0, 5.0) == 0.0

# --- 5. Databricks Execution Trigger ---
if __name__ == "__main__":
    import sys
    # Runs pytest programmatically inside the Databricks cluster
    retcode = pytest.main(["-v", "-p", "no:cacheprovider"])
    if retcode != 0:
        raise Exception("Pytest failed! Check the logs above.")
    else:
        print("All tests passed successfully! The UDFs are ready for production.")