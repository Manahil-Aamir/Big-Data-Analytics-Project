from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Customer Data Processing") \
    .getOrCreate()

# Define the schema for your JSON data
schema = StructType([
    StructField("customer_id", DoubleType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("username", StringType(), True),
    StructField("email", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("birthdate", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("device_version", StringType(), True),
    StructField("home_location_lat", DoubleType(), True),
    StructField("home_location_long", DoubleType(), True),
    StructField("home_location", StringType(), True),
    StructField("home_country", StringType(), True),
    StructField("first_join_date", StringType(), True)
])

# Step 1: Read all CSV files from the specified directory, ignoring the header
df = spark.read.option("header", "false").csv("spark/output/csv_files/topic_customer/*")

# Step 2: Filter out rows where the value is 'processed'
df_filtered = df.filter(F.col("_c0") != "processed")

# Step 3: Clean the 'value' column by removing escape characters (backslashes)
# Assuming that the JSON-like data is in the first column
cleaned_df = df_filtered.withColumn("cleaned_json", F.regexp_replace(F.col("_c0"), r'\\', ''))

# Step 4: Parse the cleaned JSON string into structured columns based on the defined schema
parsed_df = cleaned_df.withColumn("json_data", F.from_json(F.col("cleaned_json"), schema))

# Step 5: Extract the individual fields from the parsed JSON data
final_df = parsed_df.select("json_data.*")

# Step 6: Show the result
final_df.show(truncate=False)

# Save the final DataFrame as a CSV file
final_df.coalesce(1).write.option("header", "true").csv("spark/output/merged/customer_data")

