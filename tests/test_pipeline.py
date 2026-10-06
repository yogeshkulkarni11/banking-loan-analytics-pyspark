from src.pipeline import build_gold, build_silver


def test_silver_derives_risk_and_ratio(spark):
    rows = [
        ("L1", "C1", "2025-01-01", "Home", 100000.0, 200000.0, 780, "Salaried", 120, 8.0, "Approved"),
        ("L2", "C2", "2025-01-02", "Personal", 50000.0, 60000.0, 620, "Salaried", 36, 14.0, "Rejected"),
    ]
    cols = ["loan_id", "customer_id", "application_date", "loan_type", "loan_amount", "annual_income", "credit_score", "employment_type", "tenure_months", "interest_rate", "application_status"]
    silver = build_silver(spark.createDataFrame(rows, cols))
    result = {r.loan_id: (r.risk_band, r.income_to_loan_ratio) for r in silver.collect()}
    assert result["L1"] == ("Low", 2.0)
    assert result["L2"] == ("High", 1.2)


def test_gold_approval_rate(spark):
    rows = [
        ("L1", "C1", "2025-01-01", "Home", 100000.0, 200000.0, 780, "Salaried", 120, 8.0, "Approved"),
        ("L2", "C2", "2025-01-10", "Home", 80000.0, 160000.0, 700, "Salaried", 60, 10.0, "Rejected"),
    ]
    cols = ["loan_id", "customer_id", "application_date", "loan_type", "loan_amount", "annual_income", "credit_score", "employment_type", "tenure_months", "interest_rate", "application_status"]
    gold = build_gold(build_silver(spark.createDataFrame(rows, cols)))
    row = gold["monthly"].collect()[0]
    assert row.approval_rate == 50.0
