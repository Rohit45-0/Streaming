import json
import requests
from confluent_kafka import Consumer, KafkaException, KafkaError

# Kafka consumer configuration
consumer = Consumer({
    'bootstrap.servers': 'b-2.kafka01.6xrdlj.c2.kafka.ap-south-1.amazonaws.com:9092',
    'group.id': 'test-group',
    'auto.offset.reset': 'earliest'
})

# Subscribe to the Kafka topic
consumer.subscribe(['transaction-topic'])

print("🎧 Listening to Kafka topic 'transaction-topic'...")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f"End of partition: {msg.topic()} [{msg.partition}] @ {msg.offset()}")
            else:
                raise KafkaException(msg.error())
        else:
            try:
                message_value = msg.value().decode('utf-8')
                json_data = json.loads(message_value)
                print(f"Received: {json_data}")

                # Send this data to the FastAPI prediction endpoint
                response = requests.post('http://13.201.168.92:8000/predict', json=json_data)

                if response.status_code == 200:
                    print(f"Prediction result: {response.json()}")
                else:
                    print(f"FastAPI Error: {response.status_code}")

            except json.JSONDecodeError:
                print("Invalid JSON received")
            except Exception as e:
                print(f"Error processing message: {e}")

finally:
    consumer.close()
