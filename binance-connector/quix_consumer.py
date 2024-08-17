from quixstreams import Application
import duckdb

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

# Connect to DuckDB
conn = duckdb.connect('binance_data.db')

# Create table if not exists
conn.execute("""
    CREATE TABLE IF NOT EXISTS binance_data (
        time TIMESTAMP,
        symbol VARCHAR,
        bid_price DOUBLE,
        ask_price DOUBLE,
        spread DOUBLE,
        spread_percentage DOUBLE
    )
""")

# Define a transformation to process Binance data
def process_binance_data(data):
    processed = {
        'time': data['time'],
        'symbol': data['symbol'],
        'bid_price': data['bidPrice'],
        'ask_price': data['askPrice'],
        'spread': data['askPrice'] - data['bidPrice']
    }
    processed['spread_percentage'] = (processed['spread'] / processed['bid_price']) * 100
    return processed

sdf = sdf.apply(process_binance_data)

# Function to insert data into DuckDB
def insert_to_duckdb(data):
    conn.execute("""
        INSERT INTO binance_data (time, symbol, bid_price, ask_price, spread, spread_percentage)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (data['time'], data['symbol'], data['bid_price'], data['ask_price'], data['spread'], data['spread_percentage']))
    print(f"Inserted into DuckDB: {data}")

# Apply the insert function to each processed message
sdf = sdf.update(insert_to_duckdb)

# Run the streaming application
if __name__ == "__main__":
    try:
        app.run(sdf)
    finally:
        conn.close()
