import json
from urllib.parse import urlparse
from opensearchpy import OpenSearch
from confluent_kafka import Consumer, TopicPartition
import config


# Create an OpenSearch client and an index
def create_opensearch_client():
    conn_string = "http://localhost:9200"

    # Parse the connection string
    conn_uri = urlparse(conn_string)
    # Create a connection to the OpenSearch REST client
    # Create an index
    try:
        client = OpenSearch([{'host': conn_uri.hostname, 'port': conn_uri.port}],
                            use_ssl=False, verify_certs=False)
        index_name = 'wikimedia'
        if not client.indices.exists(index_name):
            client.indices.create(index_name)
            print('The wikimedia index has been created')
        else:
            print('Index already exists')
    except Exception as e:
        print('OpenSearchClient could not be created:', str(e))

    return client


# Create a Kafka consumer
def create_kafka_consumer():
    try:
        consumer = Consumer(
            {
                'bootstrap.servers': 'localhost:9092',
                'group.id': 'consumer-opensearch-demo',
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': False
            }
        )

        print('Kafka Consumer has been initiated...')
    except Exception as e:
        print('Consumer is not created:', str(e))
    return consumer


def get_partition_lag(consumer, topic, partition):
    topicPartition = TopicPartition(topic, partition)
    committed = consumer.committed([topicPartition])[0].offset
    last_offset = consumer.get_watermark_offsets(topicPartition)[1]
    partition_lag = last_offset - committed
    return partition_lag


opensearch_client = create_opensearch_client()
topic = 'wikimedia.recentchange'
consumer = create_kafka_consumer()
consumer.subscribe([topic])
print('Consumer subscribed to the topic wikimedia.recentchange')
topic_metadata = consumer.list_topics(topic=topic).topics.get(topic)
partition_count = len(topic_metadata.partitions)
print(f"Topic '{topic}' has {partition_count} partitions.")
batch_size = 10
while True:
    # Poll for new messages
    messages = consumer.consume(batch_size, timeout=2)
    messagesCount = len(messages)
    print('Received', messagesCount, 'messages')

    if not messages:
        continue
    for message in messages:
        if message.error():
            print(f"Error consuming message: {message.error()}")
        else:
            try:
                message_value = message.value()
                if message_value:
                    decoded_value = message_value.decode('utf-8')
                    # Split the key-value pair
                    key, value = decoded_value.split(": ", 1)

                    if key == "id":
                        try:
                            message_data_list = json.loads(value)
                            for message_data in message_data_list:
                                try:
                                    index_request = {
                                        'index': 'wikimedia',
                                        'body': message_data
                                    }
                                    response = opensearch_client.index(**index_request)
                                    print('Inserted one doc in OpenSearch:', response['result'], 'ID:', response['_id'])
                                except Exception as e:
                                    print("Error indexing document:", str(e))
                        except json.JSONDecodeError as json_error:
                            print("Error decoding JSON data:", str(json_error))
                    elif key == "data":
                        try:
                            message_data = json.loads(value)
                            index_request = {
                                'index': 'wikimedia',
                                'body': message_data
                            }
                            response = opensearch_client.index(**index_request)
                            print('Inserted one doc in OpenSearch:', response['result'], 'ID:', response['_id'])
                        except json.JSONDecodeError as json_error:
                            print("Error decoding JSON data:", str(json_error))
                        except Exception as e:
                            print("Error indexing document:", str(e))
                    else:
                        print("Unexpected key:", key)
                else:
                    print("Empty message value, skipping indexing.")
            except Exception as e:
                print("Error decoding message:", str(e))
    # Synchronous offset commit
    consumer.commit(asynchronous=False)
    print('Offsets have been committed')
    somme_lag = 0
    for partition in range(partition_count):
        print(get_partition_lag(consumer, topic, partition))
        somme_lag += (get_partition_lag(consumer, topic, partition))
    print(somme_lag)