from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local") \
    .appName("append_retail_data") \
    .getOrCreate()


df_products = spark.read.format("csv").option("header","True").load("C:\\Users\\shubhsat\\Desktop\\retail_domain\\source\\products.csv")
df_products.show()