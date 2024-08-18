from quixstreams import Application
from kafka import KafkaProducer
import json
from datetime import datetime
import time
from kafka.errors import NoBrokersAvailable
# Create an Application - the main configuration entry point
app = Application(
    broker_address="kafka:9092",
    consumer_group="binance-data-processor-v1",
    auto_offset_reset="earliest",
)

# Define a topic with Binance data in JSON format
binance_topic = app.topic(name="binance-top-of-book", value_deserializer="json")

# Create a StreamingDataFrame - the stream processing pipeline
sdf = app.dataframe(topic=binance_topic)

# Initialize Kafka producer for Pinot ingestion
def create_kafka_producer(max_retries=5, retry_interval=5):
    for attempt in range(max_retries):
        try:
            return KafkaProducer(bootstrap_servers=['kafka:9092'])
        except NoBrokersAvailable:
            if attempt < max_retries - 1:
                print(f"Failed to connect to Kafka. Retrying in {retry_interval} seconds...")
                time.sleep(retry_interval)
            else:
                raise
producer = create_kafka_producer()

# Define a transformation to process Binance data
def process_binance_data(data):
    processed = {
        'time': int(datetime.strptime(data['time'], '%Y-%m-%d %H:%M:%S.%f').timestamp() * 1000),
        'symbol': data['symbol'],
        'bid_price': float(data['bidPrice']),
        'ask_price': float(data['askPrice']),
        'spread': float(data['askPrice']) - float(data['bidPrice']),
        'spread_percentage': ((float(data['askPrice']) - float(data['bidPrice'])) / float(data['bidPrice'])) * 100
    }
    return processed


sdf = sdf.apply(process_binance_data)

# Function to send data to Kafka for Pinot ingestion
def send_to_pinot(data):
    producer.send('binance-data-pinot', json.dumps(data).encode())
    print(f"Sent to Pinot: {data}")

# Apply the send function to each processed message
sdf = sdf.update(send_to_pinot)

# Run the streaming application
if __name__ == "__main__":
    app.run(sdf)