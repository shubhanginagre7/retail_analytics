from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local") \
    .appName("append_retail_data") \
    .getOrCreate()

df_customers = spark.read.format("csv").option("header", "True").option("inferschema", "True").load(
    "C:\\Users\\shubhsat\\Desktop\\retail_domain\\source\\customers.csv")
df_customers.show(truncate=False)
df_customers.printSchema()

from pyspark.sql.functions import current_date

df_customer = df_customers.withColumn("batch_id", current_date())

df_customer.repartition(2).write.format("csv").mode("overwrite").option("header", "True").option("path","C:\\Users\\shubhsat\\Desktop\\retail_domain\\raw\\customers").save()
