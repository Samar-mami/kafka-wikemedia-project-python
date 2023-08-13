import requests
from confluent_kafka import Producer
import urllib3

# Disable SSL certificate verification (not recommended for production)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Kafka configuration
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "wikimedia.recentchange"

# Create a Kafka producer
producer_config = {
    "bootstrap.servers": KAFKA_BROKER
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
