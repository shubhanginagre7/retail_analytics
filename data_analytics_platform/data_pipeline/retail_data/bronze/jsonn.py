

from pyspark.sql.functions import get_json_object, json_tuple, col, concat, substring, split, lit

from pyspark.sql.functions import to_json


from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local") \
    .appName("customer_transformations") \
    .getOrCreate()

jsonDF = spark.range(1).selectExpr("""'{"myJSONKey" : {"myJSONValue" : [1, 2, 3]}}' as jsonString""")


jsonDF.select(
    get_json_object(col("jsonString"), "$.myJSONKey.myJSONValue[1]").alias("column"),
    json_tuple(col("jsonString"), "myJSONKey")).show(truncate=False)





