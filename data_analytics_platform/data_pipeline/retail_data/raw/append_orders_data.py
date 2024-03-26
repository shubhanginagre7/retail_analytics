from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date

spark = SparkSession.builder \
    .master("local") \
    .appName("append_retail_data") \
    .getOrCreate()

df_orders = spark.read.format("csv").option("header", "True").load(
    "C:\\Users\\shubhsat\\Desktop\\retail_domain\\source\\orders.csv")
df_orders.show()

df_orders.select(to_date("order_datetime", "yyyy-MMM-dd HH:mm:ss").alias("order_datetime")).show()

list = []
for i in range(1, 10):
    list.append(i)
    i += 1
print(list)
