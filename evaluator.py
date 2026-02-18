from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Initialize Spark
spark = SparkSession.builder.appName("SentinEdgeEvaluation").getOrCreate()

# 1. Load the Silver Data Lake
try:
    df = spark.read.parquet("data_lake/silver_transactions")
    
    # 2. Define Ground Truth
    # Our producer 'simulates' fraud if amount > 5000 (roughly)
    df_eval = df.withColumn("ground_truth", when(col("amount") > 5000, "YES").otherwise("NO"))

    # 3. Calculate Confusion Matrix Elements
    tp = df_eval.filter((col("is_anomaly") == "YES") & (col("ground_truth") == "YES")).count()
    fp = df_eval.filter((col("is_anomaly") == "YES") & (col("ground_truth") == "NO")).count()
    fn = df_eval.filter((col("is_anomaly") == "NO") & (col("ground_truth") == "YES")).count()
    tn = df_eval.filter((col("is_anomaly") == "NO") & (col("ground_truth") == "NO")).count()

    # 4. Calculate Metrics
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0
    accuracy = (tp + tn) / (tp + fp + fn + tn) if (tp + fp + fn + tn) > 0 else 0

    print("\n" + "="*40)
    print("SENTINEDGE ACCURACY REPORT")
    print("="*40)
    print(f"Accuracy:  {accuracy:.2%}")
    print(f"Precision: {precision:.2%}")
    print(f"Recall:    {recall:.2%}")
    print("="*40 + "\n")

except Exception as e:
    print(f"Error: {e}. Make sure you've run the Processor and data exists in data_lake/!")