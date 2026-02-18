import os
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# 1. Define the Schema
schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("user_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("timestamp", StringType()),
    StructField("location", StringType())
])

# 2. Initialize Spark
spark = SparkSession.builder \
    .appName("SentinEdgeProcessor") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 3. Define the Fraud Detection logic INSIDE the worker
def detect_fraud(user_id, amount):
    # LAZY INITIALIZATION: Create the client only when the worker needs it
    # This prevents Spark from trying to pickle the connection object
    dynamodb = boto3.resource(
        'dynamodb', 
        endpoint_url='http://localhost:8000', 
        region_name='us-east-1',
        aws_access_key_id='fakeMyKeyId',
        aws_secret_access_key='fakeSecretAccessKey'
    )
    table = dynamodb.Table('UserFeatures')

    try:
        response = table.get_item(Key={'user_id': user_id})
        
        if 'Item' in response:
            avg_spend = float(response['Item']['avg_spend'])
            is_anomaly = "YES" if amount > (avg_spend * 5) else "NO"
            
            # Simple moving average update
            new_avg = (avg_spend + amount) / 2
            table.put_item(Item={'user_id': user_id, 'avg_spend': str(new_avg)})
            return is_anomaly
        else:
            table.put_item(Item={'user_id': user_id, 'avg_spend': str(amount)})
            return "INITIALIZING"
    except Exception as e:
        return f"ERR: {str(e)}"

fraud_check_udf = udf(detect_fraud, StringType())

# 4. Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "financial_transactions") \
    .option("startingOffsets", "latest") \
    .load()

# 5. Transform
transactions = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# 6. Enrich
enriched_df = transactions.withColumn(
    "is_anomaly", 
    fraud_check_udf(col("user_id"), col("amount"))
)

# 7. Sink to Console
print("--- SentinEdge Real-Time Processor Online ---")

# Console Sink 
console_query = enriched_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Data Lake Sink 
file_query = enriched_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "data_lake/silver_transactions") \
    .option("checkpointLocation", "data_lake/checkpoints") \
    .start()

# Wait for both to finish
spark.streams.awaitAnyTermination()