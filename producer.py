import json
import time
import pandas as pd
from confluent_kafka import Producer

# Kafka producer configuration
producer = Producer({'bootstrap.servers': 'b-2.kafka01.6xrdlj.c2.kafka.ap-south-1.amazonaws.com:9092'})

# Read the CSV file
try:
    df = pd.read_csv("test_df.csv")  # Ensure the CSV file exists in the same directory
except FileNotFoundError:
    print("CSV file not found.")
    exit(1)
except pd.errors.EmptyDataError:
    print("CSV file is empty.")
    exit(1)

# Send each row as a JSON message to Kafka
for _, row in df.iterrows():
    try:
        message = json.dumps(row.to_dict())  # Convert row to JSON
        producer.produce('transaction-topic', value=message)
        producer.flush()
        print(f"Sent: {message}")
        time.sleep(1)  # Delay of 1 second
    except Exception as e:
        print(f"Error sending row: {e}")

print("Data sent to Kafka successfully.")
