from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


def create_spark():
    return SparkSession.builder.appName("BankingLoanAnalytics").master("local[*]").getOrCreate()


def read_bronze(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.option("header", True).option("inferSchema", True).csv(path)


def build_silver(df: DataFrame) -> DataFrame:
    cleaned = (
        df.withColumn("application_date", F.to_date("application_date"))
        .withColumn("loan_amount", F.col("loan_amount").cast("double"))
        .withColumn("annual_income", F.col("annual_income").cast("double"))
        .withColumn("credit_score", F.col("credit_score").cast("int"))
        .withColumn("interest_rate", F.col("interest_rate").cast("double"))
        .withColumn("income_to_loan_ratio", F.round(F.col("annual_income") / F.col("loan_amount"), 3))
        .withColumn("risk_band", F.when(F.col("credit_score") >= 750, "Low")
                    .when(F.col("credit_score") >= 650, "Medium").otherwise("High"))
        .withColumn("application_month", F.date_format("application_date", "yyyy-MM"))
        .dropDuplicates(["loan_id"])
    )
    return cleaned.filter(
        (F.col("loan_amount") > 0) &
        (F.col("annual_income") > 0) &
        F.col("customer_id").isNotNull()
    )


def build_gold(silver: DataFrame) -> dict[str, DataFrame]:
    approved = silver.filter(F.col("application_status") == "Approved")

    monthly = (
        silver.groupBy("application_month")
        .agg(
            F.count("loan_id").alias("applications"),
            F.sum(F.when(F.col("application_status") == "Approved", 1).otherwise(0)).alias("approved_applications"),
            F.sum(F.when(F.col("application_status") == "Rejected", 1).otherwise(0)).alias("rejected_applications"),
            F.sum("loan_amount").alias("requested_amount"),
            F.sum(F.when(F.col("application_status") == "Approved", F.col("loan_amount")).otherwise(0)).alias("approved_amount"),
            F.round(F.avg("credit_score"), 2).alias("avg_credit_score")
        )
        .withColumn("approval_rate", F.round(F.col("approved_applications") / F.col("applications") * 100, 2))
    )

    by_type = (
        silver.groupBy("loan_type")
        .agg(
            F.count("loan_id").alias("applications"),
            F.sum(F.when(F.col("application_status") == "Approved", 1).otherwise(0)).alias("approved_count"),
            F.sum("loan_amount").alias("requested_amount"),
            F.round(F.avg("interest_rate"), 2).alias("avg_interest_rate"),
            F.round(F.avg("credit_score"), 2).alias("avg_credit_score")
        )
        .withColumn("approval_rate", F.round(F.col("approved_count") / F.col("applications") * 100, 2))
    )

    customer = (
        approved.groupBy("customer_id")
        .agg(
            F.count("loan_id").alias("approved_loans"),
            F.sum("loan_amount").alias("total_approved_amount"),
            F.round(F.avg("interest_rate"), 2).alias("avg_interest_rate"),
            F.round(F.avg("credit_score"), 2).alias("avg_credit_score")
        )
    )
    rank_window = Window.orderBy(F.desc("total_approved_amount"))
    customer = customer.withColumn("customer_rank", F.dense_rank().over(rank_window))

    risk = (
        silver.groupBy("risk_band")
        .agg(
            F.count("loan_id").alias("applications"),
            F.sum(F.when(F.col("application_status") == "Approved", 1).otherwise(0)).alias("approved_count"),
            F.round(F.avg("loan_amount"), 2).alias("avg_loan_amount"),
            F.round(F.avg("credit_score"), 2).alias("avg_credit_score")
        )
    )

    trend_window = Window.orderBy("application_month")
    monthly = monthly.withColumn("previous_approved_amount", F.lag("approved_amount").over(trend_window))
    monthly = monthly.withColumn(
        "mom_approved_amount_pct",
        F.when(F.col("previous_approved_amount") > 0,
               F.round((F.col("approved_amount") - F.col("previous_approved_amount")) /
                       F.col("previous_approved_amount") * 100, 2))
    )
    return {"monthly": monthly, "loan_type": by_type, "customer": customer, "risk": risk}


def run(input_path: str, output_dir: str):
    spark = create_spark()
    try:
        bronze = read_bronze(spark, input_path)
        silver = build_silver(bronze)
        gold = build_gold(silver)
        silver.write.mode("overwrite").parquet(f"{output_dir}/silver")
        for name, frame in gold.items():
            frame.write.mode("overwrite").parquet(f"{output_dir}/gold/{name}")
    finally:
        spark.stop()


if __name__ == "__main__":
    run("data/raw/loan_applications.csv", "output")
