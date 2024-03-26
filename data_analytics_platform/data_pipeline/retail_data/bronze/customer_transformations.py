from pyspark.sql import SparkSession
from pyspark.sql.functions import struct, explode, create_map, map_keys, map_values, from_json
from pyspark.sql.functions import concat, substring, col, split, lit, coalesce
from pyspark.sql.functions import bround, round, monotonically_increasing_id
from pyspark.sql.functions import current_date, current_timestamp, date_add, date_sub, to_date, months_between
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

spark = SparkSession.builder \
    .master("local") \
    .appName("customer_transformations") \
    .getOrCreate()

df_bronze_cust = spark.read.format("csv").option("inferschema", "True").option("header", "True").load(
    "C:\\Users\\shubhsat\\Desktop\\retail_domain\\raw\\customers")
df_bronze_cust.show()

customerdf = df_bronze_cust.withColumn("first_name", split("full_name", " ").__getitem__(0)). \
    withColumn("last_name", split("full_name", " ").__getitem__(1))

result = customerdf.select(concat(substring("first_name", 1, 1), substring("last_name", 1, 1)))

unique_id_df = (customerdf.withColumn("unique_customer_id", concat("customer_id", lit("_"),
                                                                   concat(substring("first_name", 1, 1),
                                                                          substring("last_name", 1, 1))))
                .withColumn("flag", col("CUSTOMER_ID") == 286))
# unique_id_df.show()

# result = concat(substring(col("FULL_NAME"), 1, 1), substring(col("full_name")," ", 1))
# unique_id_df.select(pow(col("CUSTOMER_ID") * col("CUSTOMER_ID"), 2).alias("multiplication")).show()
# unique_id_df.select(round(lit(2.8)), bround(lit(2.8))).show()

# df_bronze_customers=df_bronze_cust.withColumn("customer_unique_id",result)
# df_bronze_customers.show(truncate=False)
# unique_id_df.describe().show()
# unique_id_df.select(monotonically_increasing_id()).show()


dateDF = unique_id_df.withColumn("today", current_date()).withColumn("now", current_timestamp())
dateDF.select(date_add(col("today"), 5), date_sub(col("today"), 5)).limit(1).show()

dateDF.select(
    to_date(lit("2018-05-22")).alias("start"),
    to_date(lit("2017-01-01")).alias("end")) \
    .select(months_between(col("start"), col("end"))).limit(1).show()

dateFormat = "yyyy-dd-MM"
dateDF.select(
    to_date(lit("2017-20-12"), dateFormat).alias('date'),
    to_date(lit("2018-11-07"), dateFormat).alias("date2")
).limit(1).show()

unique_id_df.select(coalesce(col("full_name"), col("email_address"))).show()

complexDF = unique_id_df.select(struct("full_name", "customer_id").alias("complex"))
complexDF.createOrReplaceTempView("complexDF")

complexDF.select("complex.full_name").show()
complexDF.select(col("complex").getField("customer_id")).show()


unique_id_df.select(split(col("full_name")," ").__getitem__(0).alias("fn")).show()
unique_id_df.select(split(col("full_name")," ").getItem(1).alias("ln")).show()
unique_id_df.select(split(col("email_address"),"@").getItem(0).alias("splitted")).show()

unique_id_df.withColumn("splitted",split(col("email_address"),"@"))\
             .withColumn("exploded",explode(col("splitted")))\
             .select("full_name","email_address","exploded").show()


unique_id_df.select(create_map(col("full_name"),col("customer_id")).alias("complex_map")).show(truncate=False)

from pyspark.sql.functions import to_json

parseSchema = StructType((
 StructField("full_name",StringType(),True),
 StructField("customer_id",IntegerType(),True)))

unique_id_df.selectExpr("(full_name, customer_id) as myStruct") \
    .select(to_json(col("myStruct")).alias("newJSON")) \
    .select(from_json(col("newJSON"), parseSchema), col("newJSON")).show(truncate=False)



