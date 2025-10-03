from pyspark.sql.functions import *
from pyspark.sql.types import *
from config.database_config import get_spark_config
from config.spark_config import Spark_connect



jar = [
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
    "org.mongodb.spark:mongo-spark-connector_2.12:10.5.0",
    "mysql:mysql-connector-java:8.0.33"
]
spark_connect = Spark_connect(
    app_name="phong",
    master_url="local[*]",
    executor_memory="2g",
    executor_cores=1,
    driver_memory="2g",
    num_executors=1,
    jar_packages=jar,
    log_level="INFO"
)
spark = spark_connect.spark

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
spark_config = get_spark_config()
data.writeStream \
    .format("mongodb") \
    .option("checkpointLocation", "/tmp/spark_checkpoint/mongo") \
    .option("spark.mongodb.connection.uri", spark_config["mongoDB"]["uri"]) \
    .option("spark.mongodb.database", spark_config["mongoDB"]["database"]) \
    .option("spark.mongodb.collection", spark_config["mongoDB"]["collection"]) \
    .trigger(processingTime="1 seconds") \
    .outputMode("append") \
    .start() \
    .awaitTermination()
