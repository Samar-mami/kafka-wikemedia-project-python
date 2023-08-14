import json

import requests
from confluent_kafka import Producer
import urllib3
import logging

# Disable SSL certificate verification (not recommended for production)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Kafka configuration
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "wikimedia.recentchange"

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# Create a Kafka producer
producer_config = {
    "bootstrap.servers": KAFKA_BROKER,
    #"enable_idempotence": True,
    "acks": "all",
    "linger.ms": 20,
    "queue.buffering.max.ms": 20,
    "batch.size": str(32*1024),
    "compression.type": 'snappy'
     }
producer = Producer(producer_config)

# Fetch data from the Wikimedia stream without SSL verification
url = "https://stream.wikimedia.org/v2/stream/recentchange"
response = requests.get(url, stream=True, verify=False)

# Produce data to Kafka topic
for line in response.iter_lines():
    if line:
        try:
            line = line.decode('utf-8')
            producer.produce(KAFKA_TOPIC, value=line)
            producer.flush()
            print("Produced:", line)
        except Exception as e:
            print("Error:", e)

# Close the Kafka producer
producer.flush()
producer.close()
