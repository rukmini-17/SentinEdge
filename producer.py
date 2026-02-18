import json
import random
import time
from datetime import datetime
from confluent_kafka import Producer
from faker import Faker

fake = Faker()
conf = {'bootstrap.servers': "localhost:9092"}
producer = Producer(conf)

def delivery_report(err, msg):
    if err: print(f"Failed: {err}")
    else: print(f"Sent: {msg.value().decode('utf-8')[:50]}...")

def generate_transaction():
    user_id = f"user_{random.randint(100, 110)}" # Small pool for repeat activity
    chance = random.random()
    
    # 5% chance of a "Fraudulent" anomaly (high amount)
    amount = round(random.uniform(5000, 15000), 2) if chance > 0.95 else round(random.uniform(5, 500), 2)

    return {
        "transaction_id": fake.uuid4(),
        "user_id": user_id,
        "amount": amount,
        "timestamp": datetime.utcnow().isoformat(),
        "location": fake.city()
    }

print("--- SentinEdge: Generating Stream ---")
try:
    while True:
        data = generate_transaction()
        producer.produce('financial_transactions', key=data['user_id'], value=json.dumps(data), callback=delivery_report)
        producer.poll(0)
        time.sleep(0.5) # Fast enough to feel like a stream
except KeyboardInterrupt:
    producer.flush()