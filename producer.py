import pandas as pd
import json
import time
from confluent_kafka import Producer

# Kafka producer configuration (AWS MSK brokers)
producer = Producer({
    'bootstrap.servers': 'b-3.kafka02.diqfpt.c2.kafka.ap-south-1.amazonaws.com:9094',
    'security.protocol': 'SSL',  # Ensure MSK uses SSL
    'ssl.ca.location': '/etc/ssl/certs/ca-cert.pem'  # Path to your CA cert
})

# Read the CSV file
try:
    df = pd.read_csv("test_df.csv")
except FileNotFoundError:
    print("❌ CSV file not found.")
    exit(1)
except pd.errors.EmptyDataError:
    print("❌ CSV file is empty.")
    exit(1)

# Send each row as a JSON message to Kafka
for _, row in df.iterrows():
    try:
        row_dict = row.to_dict()
        message = json.dumps(row_dict)

        # Send message to Kafka topic
        producer.produce('transaction-topic', value=message)
        producer.flush()

        print(f"📤 Sent message: {message}")

        time.sleep(1)  # Delay to control message rate

    except Exception as e:
        print(f"❌ Error sending row: {e}")

print("✅ Data sent to Kafka successfully.")
