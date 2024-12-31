from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Initialize Spark session
spark = SparkSession.builder.appName("Replace product_metadata").getOrCreate()

# Load the input CSV file
input_path = "spark/output/merged/transactions_data/part-00000-804e5d91-e472-4ade-b71a-d151b9d33c89-c000.csv"
df = spark.read.option("header", "true").csv(input_path)

# Show the schema of the dataframe to confirm column types
df.printSchema()

# Replace the 'product_metadata' column with an empty string
df = df.withColumn("product_metadata", F.lit("metadata"))

# Handle missing or null values (if needed)
df = df.na.fill({"product_metadata": ""}).na.drop()

# Ensure the number of columns is correct (check if any rows are missing columns)
expected_columns = ["created_at", "customer_id", "booking_id", "session_id", "product_metadata", 
                    "payment_method", "payment_status", "promo_amount", "promo_code", "shipment_fee", 
                    "shipment_date_limit", "shipment_location_lat", "shipment_location_long", "total_amount"]

# Check if any row has excessive columns or missing ones
df = df.select(*expected_columns)

# Show the first few rows of the dataframe to verify the changes
df.show(5, truncate=False)

# Save the modified dataframe to a new CSV file
output_path = "spark/output/merged/transactions_data_modified"
df.write.option("header", "true").mode("overwrite").csv(output_path)

# Print completion message
print(f"Modified CSV saved to {output_path}")
