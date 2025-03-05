import json
import requests
from confluent_kafka import Consumer, KafkaException, KafkaError

# Kafka consumer configuration (AWS MSK brokers)
consumer = Consumer({
    'bootstrap.servers': 'b-3.mymskcluster.vc2zm4.c21.kafka.us-east-1.amazonaws.com:9098,b-1.mymskcluster.vc2zm4.c21.kafka.us-east-1.amazonaws.com:9098,b-2.mymskcluster.vc2zm4.c21.kafka.us-east-1.amazonaws.com:9098',
    'security.protocol': 'SSL',  # Ensure MSK uses SSL
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
            continue  # No message received, continue polling

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f"End of partition reached: {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")
            else:
                raise KafkaException(msg.error())
        else:
            try:
                # Decode the message
                message_value = msg.value().decode('utf-8')
                json_data = json.loads(message_value)

                print(f"📩 Received message: {json_data}")

                # Send this data to FastAPI model for prediction
                response = requests.post('http://13.201.168.92:8000/predict', json=json_data)

                if response.status_code == 200:
                    print(f"✅ Prediction result: {response.json()}")
                else:
                    print(f"❌ Error from FastAPI: {response.status_code}")

            except json.JSONDecodeError:
                print("❌ Received invalid JSON message")
            except Exception as e:
                print(f"❌ Error processing message: {e}")

finally:
    consumer.close()
