from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Click Stream Data Processing") \
    .getOrCreate()

# Define the schema for your click_stream data
click_stream_schema = StructType([
    StructField("session_id", StringType(), True),
    StructField("event_name", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("event_id", StringType(), True),
    StructField("traffic_source", StringType(), True),
    StructField("event_metadata", StringType(), True)
])

# Step 1: Read all CSV files from the specified directory for click_stream data, ignoring the header
click_stream_df = spark.read.option("header", "false").csv("spark/output/csv_files/topic_click_stream/*")

# Step 2: Filter out rows where the value is 'processed'
click_stream_df_filtered = click_stream_df.filter(F.col("_c0") != "processed")

# Step 3: Clean the 'value' column by removing escape characters (backslashes)
# Assuming that the JSON-like data is in the first column
click_stream_cleaned_df = click_stream_df_filtered.withColumn("cleaned_json", F.regexp_replace(F.col("_c0"), r'\\', ''))

# Step 4: Parse the cleaned JSON string into structured columns based on the click_stream schema
click_stream_parsed_df = click_stream_cleaned_df.withColumn("json_data", F.from_json(F.col("cleaned_json"), click_stream_schema))

# Step 5: Extract the individual fields from the parsed JSON data
click_stream_final_df = click_stream_parsed_df.select("json_data.*")

# Step 6: Show the result
click_stream_final_df.show(truncate=False)

# Save the final DataFrame as a CSV file
click_stream_final_df.coalesce(1).write.option("header", "true").csv("spark/output/merged/click_stream_data")
