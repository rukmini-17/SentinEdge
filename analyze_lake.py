from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("LakeAnalysis").getOrCreate()

# Read the Parquet files we just created
df = spark.read.parquet("data_lake/silver_transactions")

# Calculate Metrics
total = df.count()
frauds = df.filter(df.is_anomaly == "YES").count()
fraud_rate = (frauds / total) * 100

print(f"--- Data Lake Stats ---")
print(f"Total Transactions Processed: {total}")
print(f"Total Anomalies Detected: {frauds}")
print(f"Current Fraud Rate: {fraud_rate:.2f}%")

# See where the most fraud is happening
df.filter(df.is_anomaly == "YES").groupBy("location").count().show()