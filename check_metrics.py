import boto3
import pandas as pd

# Point directly to your local Docker container
db = boto3.resource(
    'dynamodb',
    endpoint_url='http://localhost:8000',
    region_name='us-east-1',
    aws_access_key_id='fakeMyKeyId',
    aws_secret_access_key='fakeSecretAccessKey'
)

table = db.Table('UserFeatures')

try:
    # This will now look ONLY at your local Docker DB
    response = table.scan()
    items = response.get('Items', [])
    
    if not items:
        print("Connection Successful, but table is empty.")
        print("Action: Run your processor/producer for a minute to populate data!")
    else:
        df = pd.DataFrame(items)
        print("\n--- SentinEdge Real-Time Metrics ---")
        print(df.sort_values(by='avg_spend', ascending=False))
except Exception as e:
    print(f"Error: {e}")