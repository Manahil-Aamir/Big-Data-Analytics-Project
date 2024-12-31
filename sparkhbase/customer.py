from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("CSV to HDFS").getOrCreate()

# Load the original CSV file (or the cleaned one)
df = spark.read.option("header", "true").csv("spark/output/merged/customer_data/*")

# Save it to HDFS (replace with your HDFS path)
df.write.option("header", "true").csv("hdfs://namenode:9000/customer/")


# in hbase
#hbase org.apache.hadoop.hbase.mapreduce.ImportTsv   -Dimporttsv.separator=','   -Dimporttsv.columns=$COLUMN_MAPPINGS   -Dmapreduce.job.debug=true   customer hdfs://namenode:9000/customer/
#COLUMN_MAPPINGS="HBASE_ROW_KEY,personal_info:first_name,personal_info:last_name,personal_info:username,personal_info:email,personal_info:gender,personal_info:birthdate,device_info:device_type,device_info:device_id,device_info:device_version,location_info:home_location_lat,location_info:home_location_long,location_info:home_location,location_info:home_country,join_info:first_join_date
