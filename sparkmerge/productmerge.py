from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Product Data Processing") \
    .getOrCreate()

# Define the schema for your product data
product_schema = StructType([
    StructField("id", DoubleType(), True),
    StructField("gender", StringType(), True),
    StructField("masterCategory", StringType(), True),
    StructField("subCategory", StringType(), True),
    StructField("articleType", StringType(), True),
    StructField("baseColour", StringType(), True),
    StructField("season", StringType(), True),
    StructField("year", DoubleType(), True),
    StructField("usage", StringType(), True),
    StructField("productDisplayName", StringType(), True)
])

# Step 1: Read all CSV files from the specified directory for product data, ignoring the header
product_df = spark.read.option("header", "false").csv("spark/output/csv_files/topic_product/*")

# Step 2: Filter out rows where the value is 'processed'
product_df_filtered = product_df.filter(F.col("_c0") != "processed")

# Step 3: Clean the 'value' column by removing escape characters (backslashes)
# Assuming that the JSON-like data is in the first column
product_cleaned_df = product_df_filtered.withColumn("cleaned_json", F.regexp_replace(F.col("_c0"), r'\\', ''))

# Step 4: Parse the cleaned JSON string into structured columns based on the product schema
product_parsed_df = product_cleaned_df.withColumn("json_data", F.from_json(F.col("cleaned_json"), product_schema))

# Step 5: Extract the individual fields from the parsed JSON data
product_final_df = product_parsed_df.select("json_data.*")

# Step 6: Show the result
product_final_df.show(truncate=False)

# Save the final DataFrame as a CSV file
product_final_df.coalesce(1).write.option("header", "true").csv("spark/output/merged/product_data")
