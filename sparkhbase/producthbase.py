from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("CSV to HDFS").getOrCreate()

# Load the original CSV file (or the cleaned one)
df = spark.read.option("header", "true").csv("spark/output/merged/product_data/*")

# Save it to HDFS (replace with your HDFS path)
df.write.option("header", "true").csv("hdfs://namenode:9000/product/")


# in hbase
#hbase org.apache.hadoop.hbase.mapreduce.ImportTsv   -Dimporttsv.separator=','   -Dimporttsv.columns=$COLUMN_MAPPINGS   -Dmapreduce.job.debug=true   product hdfs://namenode:9000/product/
# COLUMN_MAPPINGS="HBASE_ROW_KEY,product_info:productDisplayName,product_details:gender,product_details:masterCategory,product_details:subCategory,product_details:articleType,product_details:baseColour,metadata:season,metadata:year,metadata:usage"
