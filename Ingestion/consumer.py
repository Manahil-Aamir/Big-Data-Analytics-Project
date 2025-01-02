from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, struct
from pyspark.sql.types import StringType
import fastavro
import json
import os
from io import BytesIO

# Kafka Broker configuration
KAFKA_BROKER = "sc6-kafka-1:9092"
OUTPUT_PATH = "/spark/output/csv_files"  # Spark's local directory for CSV files

# Create Spark session with Kafka and Avro support
spark = SparkSession.builder \
    .appName("KafkaSparkConsumer") \
    .config("spark.sql.streaming.schemaInference", "true") \
    .config("spark.sql.avro.compression.codec", "snappy") \
    .getOrCreate()

# Function to load Avro schema from a file
def load_avro_schema(schema_file):
    with open(schema_file, 'r') as file:
        schema_str = file.read()
        return fastavro.schema.parse_schema(json.loads(schema_str))

# Function to deserialize Avro message
def deserialize_avro(schema, message):
    try:
        buffer = BytesIO(message)
        reader = fastavro.schemaless_reader(buffer, schema)
        return reader  # Return the reader directly as a dictionary
    except Exception as e:
        print(f"Deserialization error: {e}")
        return None

# Read from Kafka as a streaming source
def consume_messages():
    # Load schema files dynamically for all topics
    schema_files = [f for f in os.listdir('/home/schemas') if f.endswith('_schema.avsc')]  # Adjusted path for schemas
    schemas = {}
    for schema_file in schema_files:
        schema_path = os.path.join('/home/schemas', schema_file)  # Adjusted path for schemas
        topic_name = schema_file.replace('_schema.avsc', '')  # Assuming topic name is the same as schema file name without '_schema'
        schema = load_avro_schema(schema_path)
        schemas[topic_name] = schema

    # Read from Kafka
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribePattern", ".*") \
        .load()

    # Get the value field from Kafka as a binary stream
    messages = raw_stream.selectExpr("CAST(value AS BINARY) AS message", "topic")

    # Deserialize the Avro message based on the topic schema
    def process_message(row):
        topic_name = row['topic']
        message = row['message']

        if topic_name in schemas:
            schema = schemas[topic_name]
            deserialized_message = deserialize_avro(schema, message)
            return deserialized_message
        else:
            print(f"No schema found for topic {topic_name}")
            return None

    # Register the UDF
    process_udf = udf(process_message, StringType())

    # Apply the UDF to process the Kafka stream
    processed_stream = messages.withColumn("processed", process_udf(struct(col("message"), col("topic"))))

    # Function to write data to a single CSV file for each topic
    def write_to_csv(batch_df, batch_id):
        if not batch_df.isEmpty():
            # Collect unique topics from the batch
            topics = [row["topic"] for row in batch_df.select("topic").distinct().collect()]
            
            for topic in topics:
                # Filter the batch DataFrame for the current topic
                topic_df = batch_df.filter(col("topic") == topic)
                
                # Write the filtered DataFrame to a CSV file
                topic_output_path = os.path.join(OUTPUT_PATH, f"topic_{topic}")
                topic_df.write \
                    .format("csv") \
                    .option("header", "true") \
                    .mode("append") \
                    .save(topic_output_path)


    # Apply the foreachBatch function to process and write the data in batches
    query = processed_stream.writeStream \
        .outputMode("append") \
        .foreachBatch(write_to_csv) \
        .start()

    # Await termination
    query.awaitTermination()

if __name__ == "_main_":
    consume_messages()