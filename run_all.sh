#!/bin/bash

# 1. Setup Environment Variables
export AWS_ACCESS_KEY_ID=fakeMyKeyId
export AWS_SECRET_ACCESS_KEY=fakeSecretAccessKey
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
PROJECT_DIR=$(pwd)

echo "Starting SentinEdge..."

# 2. Initialize the Infrastructure
source venv/bin/activate
echo "Initializing Feature Store (DynamoDB)..."
python init_db.py

echo "Ensuring Kafka Topic exists..."
docker exec sentinedge-kafka kafka-topics --create --topic financial_transactions --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 2>/dev/null || echo "Topic already exists."

# 3. Launch the Spark Processor in a NEW Terminal Window
echo "Launching Spark Processor..."
osascript -e "tell application \"Terminal\" to do script \"cd '$PROJECT_DIR' && source venv/bin/activate && export AWS_ACCESS_KEY_ID=fakeMyKeyId && export AWS_SECRET_ACCESS_KEY=fakeSecretAccessKey && python processor.py\""

# 4. Wait a few seconds for Spark to warm up
echo "Waiting for Spark to initialize..."
sleep 15

# 5. Launch the Traffic Producer in a NEW Terminal Window
echo "Launching Traffic Producer..."
osascript -e "tell application \"Terminal\" to do script \"cd '$PROJECT_DIR' && source venv/bin/activate && python producer.py\""

echo "All systems online! Check the new terminal windows."