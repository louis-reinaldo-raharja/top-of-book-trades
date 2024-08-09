from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.expressions import col
from pyflink.table.udf import udf
from pyflink.table import DataTypes

def main():
    # Create a Table Environment
    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)

    # Add Kafka connector JAR
    t_env.get_config().set("pipeline.jars", "file:///opt/flink/lib/flink-sql-connector-kafka-1.16.0.jar")


    # Define the source table (Kafka)
    source_ddl = """
        CREATE TABLE kafka_source (
            symbol STRING,
            price DOUBLE,
            quantity DOUBLE,
            event_time TIMESTAMP(3),
            WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'binance-top-of-book',
            'properties.bootstrap.servers' = 'localhost:9092',
            'properties.group.id' = 'pyflink-test-consumer',
            'format' = 'json',
            'scan.startup.mode' = 'latest-offset'
        )
    """
    t_env.execute_sql(source_ddl)

    # Define a simple UDF to calculate total value
    @udf(result_type=DataTypes.DOUBLE())
    def calculate_total_value(price, quantity):
        return price * quantity

    # Register the UDF
    t_env.create_temporary_function("calculate_total_value", calculate_total_value)

    # Define the transformation
    result = t_env.from_path("kafka_source") \
        .select(
            col("symbol"),
            col("price"),
            col("quantity"),
            calculate_total_value(col("price"), col("quantity")).alias("total_value"),
            col("event_time")
        )

    # Print the results
    result.execute().print()

if __name__ == "__main__":
    main()
