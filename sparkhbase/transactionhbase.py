from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("CSV to HDFS").getOrCreate()

# Load the original CSV file (or the cleaned one)
df = spark.read.option("header", "true").csv("spark/output/merged/transactions_data_modified*")

# Save it to HDFS (replace with your HDFS path)
df.write.option("header", "true").csv("hdfs://namenode:9000/transactionss/")

#COLUMN_MAPPINGS="HBASE_ROW_KEY,transaction_details:created_at,transaction_details:customer_id,transaction_details:session_id,transaction_details:product_metadata,payment_info:payment_method,payment_info:payment_status,payment_info:promo_amount,payment_info:promo_code,shipment_info:shipment_fee,shipment_info:shipment_date_limit,shipment_info:shipment_location_lat,shipment_info:shipment_location_long,transaction_summary:total_amount"
#hbase org.apache.hadoop.hbase.mapreduce.ImportTsv   -Dimporttsv.separator=','   -Dimporttsv.columns=$COLUMN_MAPPINGS   -Dmapreduce.job.debug=true   transactions hdfs://namenode:9000/transactionss
