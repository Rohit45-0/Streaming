import pandas as pd
import json
import time
from confluent_kafka import Producer

# Kafka producer configuration for AWS MSK
producer = Producer({
    'bootstrap.servers': 'b-1.kafkacluster.uy165v.c2.kafka.ap-south-1.amazonaws.com:9098',
    'security.protocol': 'SSL'  # If your MSK cluster requires SSL
})

# Read the CSV file
try:
    df = pd.read_csv("test_df.csv")
except FileNotFoundError:
    print("CSV file not found.")
    exit(1)
except pd.errors.EmptyDataError:
    print("CSV file is empty.")
    exit(1)

# Send each row as a JSON message to Kafka with a delay of 1 second
for _, row in df.iterrows():
    try:
        row_dict = row.to_dict()
        message = json.dumps(row_dict)

        producer.produce('transaction-topic', value=message)
        producer.flush()

        time.sleep(1)  # Sleep for 1 second before sending the next row

    except Exception as e:
        print(f"Error sending row: {e}")

print("✅ Data sent to Kafka successfully.")
