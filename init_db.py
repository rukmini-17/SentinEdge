import boto3

# Explicitly passing dummy credentials for local dev
db = boto3.resource(
    'dynamodb', 
    endpoint_url='http://localhost:8000', 
    region_name='us-east-1',
    aws_access_key_id='fakeMyKeyId',
    aws_secret_access_key='fakeSecretAccessKey'
)

def create_table():
    try:
        table = db.create_table(
            TableName='UserFeatures',
            KeySchema=[{'AttributeName': 'user_id', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'user_id', 'AttributeType': 'S'}],
            ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
        )
        print("Creating table...")
        table.wait_until_exists()
        print("Table 'UserFeatures' is ready!")
    except db.meta.client.exceptions.ResourceInUseException:
        print("Table 'UserFeatures' already exists. Ready to go!")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    create_table()