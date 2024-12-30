import os
import random
import time
import pandas as pd
from confluent_kafka import Producer
import fastavro
import json
import threading

# Kafka broker configuration
KAFKA_BROKER = "sc6-kafka-1:9092"
CONNECTION_TIMEOUT = 10  # Timeout for establishing a connection (in seconds)
REQUEST_TIMEOUT = 30  # Timeout for waiting for a response (in seconds)
MAX_UNACKNOWLEDGED_MESSAGES = 10  # Maximum unacknowledged messages before pausing

# Function to load Avro schema from a file
def load_avro_schema(schema_file):
    with open(schema_file, 'r') as file:
        schema_str = file.read()
        return fastavro.schema.parse_schema(json.loads(schema_str))

# Function to serialize a row using the Avro schema
def serialize_avro(schema, data):
    import io
    buffer = io.BytesIO()
    fastavro.schemaless_writer(buffer, schema, data)
    return buffer.getvalue()

# Function to handle message delivery callback
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# Function to stream random rows from chunk files
def stream_from_chunks(chunk_folder, producer, schema, row_interval=1):
    if not os.path.exists(chunk_folder):
        print(f"Chunk folder not found: {chunk_folder}")
        return

    # Use the name of the folder as the topic name
    topic = os.path.basename(chunk_folder)

    chunk_files = [os.path.join(chunk_folder, f) for f in os.listdir(chunk_folder) if f.startswith("part_")]
    if not chunk_files:
        print(f"No chunk files found in folder: {chunk_folder}")
        return

    print(f"Starting streaming for topic: {topic} from chunk files in: {chunk_folder}")

    try:
        # Round-robin: pick a chunk file to stream
        chunk_file = random.choice(chunk_files)

        try:
            df = pd.read_csv(chunk_file, on_bad_lines='skip')  # Skip problematic lines
        except Exception as e:
            print(f"Error reading CSV file {chunk_file}: {e}")
            return

        if df.empty:
            print(f"CSV file is empty: {chunk_file}")
            return

        # Pick a random row from the DataFrame
        random_row = df.sample(n=1).iloc[0].to_dict()

        # Ensure all fields in the row match the schema
        for field in schema['fields']:
            field_name = field['name']
            field_type = field['type']
        
        # Handle missing values for nullable fields
        if field_name not in random_row:
            if "null" in field_type:
                random_row[field_name] = None  # Set to None only if the field is nullable
            else:
                print(f"Missing non-nullable field: {field_name}, skipping row.")
                return  # Skip the row if a non-nullable field is missing


            # Ensure the field value matches the expected type (e.g., converting to string if necessary)
            if field_type == 'string' and field_name in random_row and random_row[field_name] is not None:
                random_row[field_name] = str(random_row[field_name])

        # Serialize the random record using the schema
        serialized_data = serialize_avro(schema, random_row)

        if not serialized_data:
            print("Serialized data is empty, skipping sending to Kafka.")
            return

        producer.produce(topic, value=serialized_data, callback=delivery_report)

        # Poll for delivery reports
        producer.poll(0)

        print(f"Sent random record from {chunk_file} to topic {topic}: {random_row}")

        time.sleep(row_interval)  # Delay between sending rows

    except Exception as e:
        print(f"Error in topic {topic}: {e}")
    finally:
        producer.flush()  # Ensure all messages are sent before exiting


# Function to stream random rows continuously from a CSV file
def random_csv_streamer(file_path, topic, producer, schema, row_interval=1):
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return

    try:
        df = pd.read_csv(file_path, on_bad_lines='warn')
    except Exception as e:
        print(f"Error reading CSV file {file_path}: {e}")
        return

    if df.empty:
        print(f"CSV file is empty: {file_path}")
        return

    print(f"Starting streaming for topic: {topic} from file: {file_path}")

    try:
        random_row = df.sample(n=1).iloc[0].to_dict()
        for field in schema['fields']:
            field_name = field['name']
            if field_name in random_row and field['type'] == 'string':
                random_row[field_name] = str(random_row[field_name])

        serialized_data = serialize_avro(schema, random_row)

        producer.produce(topic, value=serialized_data, callback=delivery_report)

        # Poll for delivery reports
        producer.poll(0)

        time.sleep(row_interval)  # Delay between sending rows

    except Exception as e:
        print(f"Error in topic {topic}: {e}")
    finally:
        producer.flush()  # Ensure all messages are sent before exiting

# Function to create the Kafka producer
def create_producer():
    producer_config = {
        'bootstrap.servers': KAFKA_BROKER,
        'socket.timeout.ms': CONNECTION_TIMEOUT * 1000,
        'request.timeout.ms': REQUEST_TIMEOUT * 1000,
        'retries': 5,
        'retry.backoff.ms': 500,
        'acks': '1',
        'batch.size': 16384,
        'linger.ms': 100,
    }
    return Producer(producer_config)


# Main function
def main():
    # Create producer
    PRODUCER = create_producer()

    # Get user input for the directory containing CSV and schema files
    user_dir = input("Enter the directory path for CSV files and schemas: ")
    if not os.path.isdir(user_dir):
        print(f"Invalid directory: {user_dir}")
        exit(1)

    csv_dir = os.path.join(user_dir)
    schema_dir = os.path.join(user_dir, 'schemas')
    chunk_dir = os.path.join(user_dir, 'click_stream')

    if not os.path.isdir(csv_dir) or not os.path.isdir(schema_dir) or not os.path.isdir(chunk_dir):
        print("Invalid directory structure.")
        exit(1)

    stop_event = threading.Event()  # Event to stop threads gracefully

    csv_topic_mapping = {}

    # Load CSV mapping and schemas for all topics
    for schema_file in os.listdir(schema_dir):
        if schema_file.endswith('_schema.avsc'):
            topic_name = schema_file.replace('_schema.avsc', '')
            schema_path = os.path.join(schema_dir, schema_file)
            csv_file = os.path.join(csv_dir, f"{topic_name}.csv")
            if os.path.exists(csv_file):
                schema = load_avro_schema(schema_path)
                csv_topic_mapping[csv_file] = (topic_name, schema)  # Map CSV file to topic and schema


    if not csv_topic_mapping:
        print("No valid CSV and schema files found. Exiting.")

    try:
        while True:
            # Fetch the schema for 'click_stream.csv' directly
            click_stream_schema = load_avro_schema(os.path.join(schema_dir, 'click_stream_schema.avsc'))
            # First stream from chunk files for click_stream topic
            print("Starting chunk file streaming for click_stream...")
            print("helo")
            stream_from_chunks(chunk_dir, PRODUCER, click_stream_schema, row_interval=1)  # Using click_stream schema
            
            # Then stream from CSV files for other topics
            for csv_file, (topic_name, schema) in csv_topic_mapping.items():
                if topic_name != 'click_stream':  # Skip click_stream since it's already processed
                    print(f"Starting CSV file streaming for {topic_name}...")
                    random_csv_streamer(csv_file, topic_name, PRODUCER, schema, row_interval=1)

            time.sleep(1)  # Delay between chunk and CSV streaming

    except KeyboardInterrupt:
        print("Stopping all streams...")
    finally:
        PRODUCER.flush()

if __name__ == "__main__":
    main()
