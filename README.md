# E-commerce App Transactional Project

## Architecture Design

### 1. Old Architecture that was planned to be made:

![Old Architecture](old_arch.jpeg)

### 2. Bulk Loading by HBase & Streaming Data by Kafka Architecture:

![Bulk Loading by HBase & Streaming Data by Kafka Architecture](hbase_kafka_bulk_stream_arch.png)

### 3. Airflow Architecture:

![Airflow Architecture](airflow_arch.png)

## Data

- Run Data/`import.py` to retrieve the original data source.
- Then run Data/`inc_size.py` and Data/`500mb.py` to increase the size of the data.

## Compose files

All compose files are in the `ComposeFiles` folder.

## Real-time Streaming

The schemas for each CSV file for Avro serialization are in the `Data/schemas` folder.

### Kafka to Spark Streaming

#### Kafka

- Use this container: `docker-kafka-sc6-kafka-1-1`.
- Break the larger data size file into chunks using `Data/breaking.py`.
- The code for the Kafka producer is in `Ingestion/real-time.py`.
- In Kafka, store the `schemas` folder and all CSV files in one location.
- For running the code, Python needs to be installed in the Kafka container and a virtual environment created.
- In the virtual environment, install the required packages, run the code, and specify the location where schema CSV files are stored.

#### Spark

- For Spark, store the `schemas` folder in `spark-master:/home`.
- Store `consumer.py` in `spark-master:/`.
- Then, in the Spark container, install Python 3.
- Instructions:
  ```sh
  export SPARK_HOME=/spark
  export PATH=$SPARK_HOME/bin:$PATH
  export PYSPARK_PYTHON=/usr/bin/python3
  spark-submit \
   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 \
   consumer.py
  ```

### Spark to HBase Streaming

#### HBase

Create tables for all of them:

```sh
create 'customers', 'personal_info', 'device_info', 'location_info', 'join_info'
create 'clickstream', 'event_details', 'traffic_info', 'metadata'
create 'transactions', 'transaction_details', 'payment_info', 'shipment_info', 'transaction_summary'
create 'product', 'product_info', 'product_details', 'metadata'
```

#### Spark

- CSVs are in chunks after Kafka to Spark streaming. Copy all the files in the `sparkmerge` folder to `spark-master:/` and run them to merge these CSV files into one.
- Then, copy all codes in the `sparkhbase` folder and execute them.

#### HBase

The instructions for the execution of further steps from Spark to HBase in the HBase container are specified in the files of the `sparkhbase` folder as comments at the end of the code.

## Bulk Loading

- Use the HBase distributed compose file.
- Create tables:
  ```sh
  create 'customers', 'personal_info', 'device_info', 'location_info', 'join_info'
  create 'clickstream', 'event_details', 'traffic_info', 'metadata'
  create 'transactions', 'transaction_details', 'payment_info', 'shipment_info', 'transaction_summary'
  create 'product', 'product_info', 'product_details', 'metadata'
  ```
- Copy all the codes of `CSVToTable` in HBase and run them.
- Before executing these Java codes, do:
  ```sh
  export CLASSPATH=/path/to/hbase-1.2.6/lib/*
  ```

## Design

- Run `UI/design.py` locally.
- Ensure your Thrift server is started in HBase:
  ```sh
  hbase-daemon.sh start thrift
  ```
