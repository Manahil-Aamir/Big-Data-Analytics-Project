from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Transactions Data Processing") \
    .getOrCreate()

# Define the schema for your transactions data
transactions_schema = StructType([
    StructField("created_at", StringType(), True),
    StructField("customer_id", DoubleType(), True),
    StructField("booking_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("product_metadata", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("payment_status", StringType(), True),
    StructField("promo_amount", DoubleType(), True),
    StructField("promo_code", StringType(), True),
    StructField("shipment_fee", DoubleType(), True),
    StructField("shipment_date_limit", StringType(), True),
    StructField("shipment_location_lat", DoubleType(), True),
    StructField("shipment_location_long", DoubleType(), True),
    StructField("total_amount", DoubleType(), True)
])

# Step 1: Read all CSV files from the specified directory for transactions data, ignoring the header
transactions_df = spark.read.option("header", "false").csv("spark/output/csv_files/topic_transactions/*")

# Step 2: Filter out rows where the value is 'processed'
transactions_df_filtered = transactions_df.filter(F.col("_c0") != "processed")

# Step 3: Clean the 'value' column by removing escape characters (backslashes)
# Assuming that the JSON-like data is in the first column
transactions_cleaned_df = transactions_df_filtered.withColumn("cleaned_json", F.regexp_replace(F.col("_c0"), r'\\', ''))

# Step 4: Parse the cleaned JSON string into structured columns based on the transactions schema
transactions_parsed_df = transactions_cleaned_df.withColumn("json_data", F.from_json(F.col("cleaned_json"), transactions_schema))

# Step 5: Extract the individual fields from the parsed JSON data
transactions_final_df = transactions_parsed_df.select("json_data.*")

# Step 6: Show the result
transactions_final_df.show(truncate=False)

# Save the final DataFrame as a CSV file
transactions_final_df.coalesce(1).write.option("header", "true").csv("spark/output/merged/transactions_data")
