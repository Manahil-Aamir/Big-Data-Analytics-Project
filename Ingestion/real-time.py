import fastavro
from confluent_kafka import Producer
import pandas as pd
import json
import time
import threading
import os
import socket

# Now configure the Kafka producer to use the local IP address
KAFKA_BROKER = "sc6-kafka-1:9092"
CONNECTION_TIMEOUT = 10  # Timeout for establishing a connection (in seconds)
REQUEST_TIMEOUT = 30  # Timeout for waiting for a response (in seconds)

# Function to load Avro schema from a file
def load_avro_schema(schema_file):
    with open(schema_file, 'r') as file:
        schema_str = file.read()
        return fastavro.schema.parse_schema(json.loads(schema_str))

# Function to handle message delivery callback
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# Function to serialize a row using the Avro schema
def serialize_avro(schema, data):
    import io
    buffer = io.BytesIO()
    fastavro.schemaless_writer(buffer, schema, data)
    buffer.seek(0)
    return buffer.read()

# Function to stream random rows continuously from a specific CSV file to a specific topic
def random_csv_streamer(file_path, topic, producer, schema, delay=1, row_interval=0):
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return
    
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        print(f"Error reading CSV file {file_path}: {e}")
        return

    if df.empty:
        print(f"CSV file is empty: {file_path}")
        return

    print(f"Starting streaming for topic: {topic} from file: {file_path}")
    
    try:
        while True:  # Infinite loop for continuous streaming
            random_row = df.sample(n=1).iloc[0].to_dict()  # Pick a random row
            # Ensure all data fields match schema types, convert to string if necessary
            for field in schema['fields']:
                field_name = field['name']
                if field_name in random_row:
                    if field['type'] == 'string' and not isinstance(random_row[field_name], str):
                        random_row[field_name] = str(random_row[field_name])

            # Serialize the row using Avro
            serialized_data = serialize_avro(schema, random_row)
            # Send serialized data to Kafka asynchronously
            producer.produce(topic, value=serialized_data, callback=delivery_report)

            # Wait for the row interval (1 second) before sending the next row
            time.sleep(row_interval)  # Delay between sending each row

            # Add a 2-second break between producing rows
            time.sleep(1)  # Additional 2-second delay after producing each row

            # Periodically call poll to process Kafka's internal events
            producer.poll(1)  # Increased poll time to ensure proper event processing

    except KeyboardInterrupt:
        print(f"Stopped streaming for topic: {topic}")
    except Exception as e:
        print(f"Error in topic {topic}: {e}")
    finally:
        producer.flush()  # Ensure all messages are sent before closing

# Function to handle connection timeouts and retries
def create_producer():
    producer_config = {
        'bootstrap.servers': KAFKA_BROKER,
        'socket.timeout.ms': CONNECTION_TIMEOUT * 1000,  # Timeout for socket connections (in milliseconds)
        'request.timeout.ms': REQUEST_TIMEOUT * 1000,  # Timeout for requests to Kafka broker (in milliseconds)
        'retries': 5,  # Number of retries if connection fails
        'retry.backoff.ms': 5000,  # Time to wait before retrying
        'acks': 'all',  # Ensure message acknowledgment from all replicas before considering the message sent
    }

    # Create and return the Kafka producer with the defined configurations
    return Producer(producer_config)

def run_producer_thread(file_path, topic, producer, schema, delay=1, row_interval=0):
    try:
        random_csv_streamer(file_path, topic, producer, schema, delay, row_interval)
    except Exception as e:
        print(f"Error in producer thread for topic {topic}: {e}")

if __name__ == "__main__":
    # Initialize the Kafka producer with connection timeout handling
    PRODUCER = create_producer()

    # Ask the user to input the directory where their CSV files and schemas are located
    user_dir = input("Please enter the directory path where your CSV files and schemas are located: ")

    # Check if the specified directory exists
    if not os.path.isdir(user_dir):
        print(f"The directory {user_dir} does not exist. Please provide a valid directory path.")
        exit(1)

    # Define the paths for CSV and schemas directories
    csv_dir = os.path.join(user_dir)
    schema_dir = os.path.join(user_dir, 'schemas')

    # Check if the CSV and schemas subdirectories exist
    if not os.path.isdir(csv_dir):
        print(f"The CSV directory {csv_dir} does not exist.")
        exit(1)
    if not os.path.isdir(schema_dir):
        print(f"The schemas directory {schema_dir} does not exist.")
        exit(1)

    # Manually define CSV files and corresponding topics
    csv_topic_mapping = {}

    # Load schema files and map them with CSV files
    for schema_file in os.listdir(schema_dir):
        schema_path = os.path.join(schema_dir, schema_file)
        if schema_file.endswith('_schema.avsc'):
            topic_name = schema_file.replace('_schema.avsc', '')  # Assuming topic name is the same as schema file name without '_schema'
            csv_file = os.path.join(csv_dir, f"{topic_name}.csv")
            if os.path.exists(csv_file):
                schema = load_avro_schema(schema_path)
                csv_topic_mapping[csv_file] = (topic_name, schema)

    if not csv_topic_mapping:
        print("No valid CSV and schema files found. Exiting.")
        exit(1)

    DELAY = 1  # Time in seconds between sending rows to Kafka (used in the while loop)
    ROW_INTERVAL = 1  # Time in seconds between sending each row for each topic

    # Start streaming for each CSV file in a separate thread
    threads = []
    try:
        for file_path, (topic, schema) in csv_topic_mapping.items():
            thread = threading.Thread(
                target=run_producer_thread,
                args=(file_path, topic, PRODUCER, schema, DELAY, ROW_INTERVAL)
            )
            thread.start()
            threads.append(thread)

        # Wait for all threads to finish (indefinitely in this case)
        for thread in threads:
            thread.join()

    except KeyboardInterrupt:
        print("Stopping all streams...")
    except Exception as e:
        print(f"Unexpected error: {e}")
    finally:
        PRODUCER.flush()  # Ensure the producer is properly flushed
