import pandas as pd
import json
import time
from confluent_kafka import Producer

# Kafka producer configuration (AWS MSK brokers)
producer = Producer({
    'bootstrap.servers': 'b-3.mymskcluster.vc2zm4.c21.kafka.us-east-1.amazonaws.com:9098,b-1.mymskcluster.vc2zm4.c21.kafka.us-east-1.amazonaws.com:9098,b-2.mymskcluster.vc2zm4.c21.kafka.us-east-1.amazonaws.com:9098',
    'security.protocol': 'SSL'  # Ensure MSK uses SSL
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

# Send each row as a JSON message to Kafka with a delay
for _, row in df.iterrows():
    try:
        # Convert row to a dictionary
        row_dict = row.to_dict()

        # Convert dictionary to a JSON string
        message = json.dumps(row_dict)

        # Send to Kafka topic 'transaction-topic'
        producer.produce('transaction-topic', value=message)
        producer.flush()

        # Delay to control message rate
        time.sleep(1)

    except Exception as e:
        print(f"Error sending row: {e}")

print("✅ Data sent to Kafka successfully.")
