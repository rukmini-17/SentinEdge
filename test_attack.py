import json
from confluent_kafka import Producer

p = Producer({'bootstrap.servers': 'localhost:9092'})

# We'll use a specific user so we can track them in the DB
attack_data = {
    "transaction_id": "ATTACK_001",
    "user_id": "user_999", 
    "amount": 15000.00,  # Huge amount
    "timestamp": "2026-02-18T00:00:00Z",
    "location": "Hacker_City"
}

p.produce('financial_transactions', key="user_999", value=json.dumps(attack_data))
p.flush()
print("Attack transaction sent!")