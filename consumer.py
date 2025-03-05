import json
import requests
from confluent_kafka import Consumer, KafkaException, KafkaError

# Kafka consumer configuration for AWS MSK
consumer = Consumer({
    'bootstrap.servers': 'b-1.kafkacluster.uy165v.c2.kafka.ap-south-1.amazonaws.com:9098',
    'group.id': 'test-group',
    'auto.offset.reset': 'earliest',
    'security.protocol': 'SSL'  # If MSK requires SSL
})

# Subscribe to the Kafka topic
consumer.subscribe(['transaction-topic'])

print("🎧 Listening to Kafka topic 'transaction-topic'...")

try:
    while True:
        msg = consumer.poll(1.0)  # Poll for a message with a timeout of 1 sec

        if msg is None:
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f"End of partition reached: {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")
            else:
                raise KafkaException(msg.error())
        else:
            try:
                message_value = msg.value().decode('utf-8')
                json_data = json.loads(message_value)

                print(f"✅ Received message: {json_data}")

                # Send this data to the FastAPI endpoint
                response = requests.post('http://13.201.168.92:8000/predict', json=json_data)

                if response.status_code == 200:
                    print(f"🔹 Prediction result: {response.json()}")
                else:
                    print(f"❌ Error from FastAPI: {response.status_code}")

            except json.JSONDecodeError:
                print("❌ Received invalid JSON message")
            except Exception as e:
                print(f"❌ Error processing message: {e}")

finally:
    consumer.close()
