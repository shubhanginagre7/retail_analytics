from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local") \
    .appName("append_retail_data") \
    .getOrCreate()

df_stores = spark.read.format("csv").option("header","True").load("C:\\Users\\shubhsat\\Desktop\\retail_domain\\source\\stores.csv")
df_stores.show()