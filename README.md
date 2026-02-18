# SentinEdge: Real-Time Fraud Detection Engine

**SentinEdge** is a high-throughput, stateful streaming pipeline designed to identify financial anomalies in real-time. Built for the modern data stack, it leverages the **Medallion Architecture** to move data from raw ingestion to audited, high-performance storage.

## 🏗️ Architecture
The system is composed of four distinct layers:
1. **Ingestion (Bronze):** A Python-based producer simulating live financial traffic, streaming JSON transactions into **Apache Kafka** (KRaft mode).
2. **Processing (Silver):** **PySpark Structured Streaming** serves as the brain, performing real-time ETL, schema enforcement, and anomaly detection.
3. **Feature Store:** **Amazon DynamoDB** (Local) acts as a low-latency state provider, tracking historical user benchmarks (rolling averages) for millisecond lookups.
4. **Storage (Gold):** Enriched transactions are persisted as **Snappy-compressed Parquet** files, enabling post-hoc analytical queries and MLOps evaluation.

## 📊 Performance Benchmarks
As of February 2026, the engine achieves the following metrics on a standard transaction stream:
* **Overall Accuracy:** 99.21%
* **Precision:** 94.03% (Low false-positive rate for customer experience)
* **Recall:** 90.00% (High fraud capture rate)

## 📂 Project Structure
```text
SentinEdge/
├── data_lake/               # Local Parquet storage (Silver/Gold layers)
├── images/                  # Performance report screenshots
├── docker-compose.yml       # Infrastructure (Kafka & DynamoDB)
├── processor.py             # PySpark streaming logic & DynamoDB UDFs
├── producer.py              # Synthetic transaction generator
├── init_db.py               # Feature Store schema initialization
├── test_attack.py           # Manual fraud event simulator
├── check_metrics.py         # Real-time Feature Store monitor
├── evaluator.py             # Accuracy/Precision/Recall auditor
├── run_all.sh               # Automation orchestrator
└── requirements.txt         # Project dependencies

```

## 🚀 Getting Started
Prerequisites
* Docker & Docker Compose
* Python 3.9+
* Java 17 (for PySpark)

## Installation & Execution

1. Clone the repository:

```bash
git clone [https://github.com/yourusername/SentinEdge.git](https://github.com/yourusername/SentinEdge.git)
cd SentinEdge
```

2. Setup Environment:
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

3. Start Infrastructure:
```bash
docker-compose up -d
```

4. Launch the Pipeline:
The orchestrator handles database initialization and launches the Processor and Producer in separate terminal windows.
```bash
chmod +x run_all.sh
./run_all.sh
```

## 🛠️ Key Technical Challenges Solved

* **Distributed Serialization**: Resolved _thread.lock pickling errors by implementing Lazy Initialization of Boto3 resources within the Spark UDF executor context.

* **Cold Start Handling**: Implemented "Initializing" states for new users to prevent false positives before a reliable behavioral baseline is established.

* **Fault Tolerance**: Utilized Spark Checkpointing to ensure exactly-once processing and pipeline resilience across system restarts.