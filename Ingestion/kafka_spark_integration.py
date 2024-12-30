from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess

# Default arguments for DAG
default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 12, 30),  # Start date for your DAG
    'catchup': False,
}

# Function to activate the virtual environment and run Kafka producer inside Docker container
def run_kafka_producer(**kwargs):
    virtualenv_path = 'myenv'
    producer_script = 'app/real-time.py'
    data_path = 'app'

    # Assuming the container is called "kafka_container"
    command = f"docker exec -u root -it docker-kafka-sc6-kafka-1-1 sh '. {virtualenv_path}/bin/activate && python {producer_script} --path {data_path}'"
    subprocess.run(command, shell=True, check=True)

# Function to run Spark consumer inside Docker container
def run_spark_consumer(**kwargs):
    spark_home = '/spark'
    consumer_script = 'consumer.py'

    # Assuming the container is called "spark_container"
    command = f"docker exec -it spark-master /bin/bash 'export SPARK_HOME={spark_home} && export PATH=$SPARK_HOME/bin:$PATH && export PYSPARK_PYTHON=/usr/bin/python3 && spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 {consumer_script}'"
    subprocess.run(command, shell=True, check=True)

# Define the DAG
with DAG(
    'kafka_spark_integration',
    default_args=default_args,
    description='A DAG to run Kafka producer and Spark consumer',
    schedule='@once',  # Replace schedule_interval with schedule
) as dag:

    # Task 1: Run Kafka Producer
    run_kafka_task = PythonOperator(
        task_id='run_kafka_producer',
        python_callable=run_kafka_producer
    )
    
    # Task 2: Run Spark Consumer
    run_spark_task = PythonOperator(
        task_id='run_spark_consumer',
        python_callable=run_spark_consumer
    )

    # Task dependencies (run producer first, then consumer)
    run_kafka_task >> run_spark_task
