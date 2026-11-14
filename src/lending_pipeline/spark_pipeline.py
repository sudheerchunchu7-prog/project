from __future__ import annotations
from pathlib import Path
from time import perf_counter
import csv
from pyspark.sql import SparkSession, DataFrame, functions as F, types as T
from pyspark.sql.window import Window

CUSTOMER_SCHEMA = T.StructType([T.StructField(x, T.StringType(), True) for x in ["customer_id","customer_name","date_of_birth","region","customer_segment","created_at"]])
APPLICATION_SCHEMA = T.StructType([T.StructField(x, T.StringType(), True) for x in ["application_id","customer_id","loan_amount","application_status","application_timestamp","source_system","updated_at"]])

def create_spark(app_name: str = "LendingDataPipeline", master: str = "local[2]", shuffle_partitions: int = 4) -> SparkSession:
    return (SparkSession.builder.master(master).appName(app_name)
            .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
            .config("spark.sql.session.timeZone", "UTC").getOrCreate())

def read_source(spark: SparkSession, path: str | Path, schema: T.StructType, entity: str) -> DataFrame:
    p = Path(path)
    if not p.exists(): raise FileNotFoundError(f"{entity} input file not found: {p}")
    with p.open("r", newline="", encoding="utf-8-sig") as source:
        header = next(csv.reader(source), None)
    expected = schema.fieldNames()
    if header != expected:
        raise ValueError(f"Unexpected {entity} CSV header in {p}. Expected {expected}; received {header}")
    df = spark.read.option("header", True).option("mode", "PERMISSIVE").schema(schema).csv(str(p))
    return (df.withColumn("source_file", F.input_file_name())
              .withColumn("source_row_number", F.monotonically_increasing_id() + F.lit(1)))

def _reject(df: DataFrame, entity: str, key: str, condition, code: str, message: str) -> DataFrame:
    return df.filter(condition).select(
        F.lit(entity).alias("entity_type"), F.col(key).cast("string").alias("business_key"),
        F.to_json(F.struct(*[F.col(c) for c in df.columns])).alias("original_payload"),
        F.lit(code).alias("reason_code"), F.lit(message).alias("reason_message"),
        "source_file", "source_row_number")

def validate_and_transform(customers: DataFrame, applications: DataFrame, max_loan: float, mode: str, run_id: str, optimized: bool = False) -> tuple[DataFrame, DataFrame, DataFrame, dict]:
    """Validate both inputs, quarantine invalid rows, then create curated applications."""
    c = customers.withColumn("dob", F.to_date("date_of_birth")).withColumn("created_ts", F.to_timestamp("created_at"))
    c_dups = c.groupBy("customer_id").count().filter((F.col("count") > 1) & F.col("customer_id").isNotNull()).select("customer_id")
    c = c.join(c_dups.withColumn("duplicate_key", F.lit(True)), "customer_id", "left")
    c_bad = (F.col("customer_id").isNull() | (F.trim("customer_id") == "") | F.col("customer_name").isNull() | (F.trim("customer_name") == "") |
             F.col("dob").isNull() | F.col("region").isNull() | F.col("customer_segment").isNull() |
             F.col("created_ts").isNull() | F.col("duplicate_key").isNotNull())
    c_reject = _reject(c, "CUSTOMER", "customer_id", c_bad, "INVALID_CUSTOMER", "Missing/invalid required customer field, date, or duplicate customer_id")
    valid_c = c.filter(~c_bad).select("customer_id","customer_name","date_of_birth","region","customer_segment","created_at").withColumn("first_seen_run_id", F.lit(run_id))

    a = (applications.withColumn("amount", F.col("loan_amount").cast("double"))
         .withColumn("app_ts", F.to_timestamp("application_timestamp"))
         .withColumn("updated_ts", F.to_timestamp("updated_at")))
    a_dups = a.groupBy("application_id").count().filter((F.col("count") > 1) & F.col("application_id").isNotNull()).select("application_id")
    a = a.join(a_dups.withColumn("duplicate_key", F.lit(True)), "application_id", "left")
    #known_customers = valid_c.select("customer_id").distinct()
    known_customers = valid_c.select(
    "customer_id",
    F.to_date("date_of_birth").alias("dob")
).distinct()
    a = a.join(known_customers.withColumn("known_customer", F.lit(True)), "customer_id", "left")
    bad = (F.col("application_id").isNull() | (F.trim("application_id") == "") | F.col("customer_id").isNull() |
           F.col("known_customer").isNull() | F.col("amount").isNull() | (F.col("amount") <= 0) |
           (F.col("amount") > F.lit(float(max_loan))) | F.col("application_status").isNull() | (~F.upper(F.col("application_status")).isin("APPROVED","REJECTED","PENDING")) |
           F.col("app_ts").isNull() | F.col("updated_ts").isNull() | F.col("source_system").isNull() | (F.to_date("dob") > F.to_date("app_ts")) | F.col("duplicate_key").isNotNull())
    a_reject = _reject(a, "LOAN_APPLICATION", "application_id", bad, "INVALID_APPLICATION", "Missing/invalid application field, amount, status, customer reference, timestamp, or duplicate application_id")
    valid_a = a.filter(~bad).select("application_id","customer_id",F.col("amount").alias("loan_amount"),F.upper("application_status").alias("application_status"),F.col("app_ts").alias("application_timestamp"),"source_system",F.col("updated_ts").alias("updated_at"),"source_file","source_row_number")

    if optimized:
        # Customer table is the dimension side; broadcast only after this explicit optimized-mode choice.
        joined = valid_a.join(F.broadcast(valid_c), "customer_id", "inner")
    else:
        joined = valid_a.join(valid_c, "customer_id", "inner")
    curated = (joined.withColumn("age_at_application", F.floor(F.months_between(F.to_date("application_timestamp"), F.to_date("date_of_birth")) / F.lit(12)).cast("integer"))
        .withColumn("loan_amount_category", F.when(F.col("loan_amount") < 100000, "SMALL").when(F.col("loan_amount") <= 300000, "MEDIUM").otherwise("LARGE"))
        .withColumn("run_id", F.lit(run_id)).withColumn("processing_mode", F.lit(mode))
        .select("application_id","customer_id","loan_amount","application_status","age_at_application","loan_amount_category","application_timestamp","source_system","updated_at","run_id","processing_mode","source_file","source_row_number"))
    rejects = c_reject.unionByName(a_reject)
    metrics = {"input_customer_count": customers.count(), "input_application_count": applications.count(),
               "valid_customer_count": valid_c.count(), "valid_application_count": valid_a.count(),
               "rejected_count": rejects.count(), "output_count": curated.count()}
    return valid_c, curated, rejects, metrics

def run_spark_job(customer_csv: str, application_csv: str, max_loan: float, mode: str, run_id: str, shuffle_partitions: int = 4) -> dict:
    spark = create_spark(shuffle_partitions=shuffle_partitions)
    try:
        customers = read_source(spark, customer_csv, CUSTOMER_SCHEMA, "customers")
        applications = read_source(spark, application_csv, APPLICATION_SCHEMA, "loan applications")
        started = perf_counter()
        valid_c, curated, rejects, metrics = validate_and_transform(customers, applications, max_loan, mode, run_id, optimized=(mode == "OPTIMIZED"))
        duration = perf_counter() - started
        plan = curated._jdf.queryExecution().executedPlan().toString()
        return {"spark": spark, "customers": valid_c, "applications": curated, "rejects": rejects,
                "metrics": metrics, "spark_duration_seconds": duration, "physical_plan": plan,
                "partition_count": curated.rdd.getNumPartitions()}
    except Exception:
        spark.stop()
        raise
