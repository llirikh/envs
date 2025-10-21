from kafka import KafkaProducer
import json
import time
from datetime import datetime
import random
import uuid

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:29092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic = 'test_topic'

for i in range(1, 100):
    message = {
        'id': str(uuid.uuid4()),
        'name': f"test_record_{i}",
        'timestamp': datetime.now().isoformat()
    }
    producer.send(topic, value=message)
    
    print(f"Sent: {message}")
    time.sleep(1.5)

producer.close()