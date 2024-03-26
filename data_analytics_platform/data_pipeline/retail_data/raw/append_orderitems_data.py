from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local") \
    .appName("append_retail_data") \
    .getOrCreate()

df_order_items = spark.read.format("csv").option("header", "True").load("C:\\Users\\shubhsat\\Desktop\\retail_domain\\source"
                                                                   "\\order_items.csv")
df_order_items.show()











