from pyspark.sql.functions import *
from pyspark.sql.types import *

from pyspark.sql import SparkSession

spark = SparkSession.builder \
        .appName("phong") \
        .master("local[*]") \
        .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                                      "org.mongodb.spark:mongo-spark-connector_2.12:10.5.0") \
        .getOrCreate()

df = spark.readStream \
        .format("kafka") \
        .option("startingOffsets","earliest") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "phong") \
        .load()

schemaKafka = StructType([
        StructField("user_id", IntegerType(), True),
        StructField("login", StringType(), True),
        StructField("gravatar_id", StringType(), True),
        StructField("avatar_url", StringType(), True),
        StructField("url", StringType(), True),
        StructField("state", StringType(), True),
        StructField("log_timestamp", StringType(), True)
    ])


data_decode = df.select(col("value").cast("string"))

#  phân tích (parse) nội dung chuỗi JSON theo một schema cụ thể
data = data_decode.select(from_json(col("value"),schemaKafka).alias("data")) \
                     .select("data.*")

# data_decode = df.selectExpr("CAST(value AS STRING)")

data.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", False) \
    .start() \
    .awaitTermination()
