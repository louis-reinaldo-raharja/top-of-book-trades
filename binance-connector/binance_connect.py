import asyncio
import json
from datetime import datetime
from binance import AsyncClient, BinanceSocketManager
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import time

async def create_kafka_producer(max_retries=5, retry_interval=5):
    for attempt in range(max_retries):
        try:
            return KafkaProducer(bootstrap_servers=['kafka:9092'])
        except NoBrokersAvailable:
            if attempt < max_retries - 1:
                print(f"Failed to connect to Kafka. Retrying in {retry_interval} seconds...")
                await asyncio.sleep(retry_interval)
            else:
                raise

async def main():
    # Initialize Binance client
    client = await AsyncClient.create()
    bm = BinanceSocketManager(client)

    # Initialize Kafka producer
    producer = await create_kafka_producer()

    async with bm.multiplex_socket(['btcusdt@bookTicker']) as stream:
        while True:
            res = await stream.recv()
            if res['stream'] == 'btcusdt@bookTicker':
                data = res['data']
                current_time = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                
                kafka_message = {
                    'time': current_time,
                    'symbol': data['s'],
                    'bidPrice': float(data['b']),
                    'bidQty': float(data['B']),
                    'askPrice': float(data['a']),
                    'askQty': float(data['A']),
                    'updateId': data['u']
                }
                
                producer.send('binance-top-of-book', json.dumps(kafka_message).encode())
                
                print(f"Sent to Kafka: {kafka_message}")

    await client.close_connection()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
