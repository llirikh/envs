from kafka import KafkaProducer
import json
import time
from datetime import datetime
import random
import uuid

producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],
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
    time.sleep(2)

producer.close()