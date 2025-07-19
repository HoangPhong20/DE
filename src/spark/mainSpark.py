from pyspark.sql.functions import col, lit
from pyspark.sql.types import *

from config.spark_config import Spark_connect

from config.database_config import get_spark_config

from spark_write_data import SparkWriteDatabases

def main():

    # JDBC la phuong thuc, khong phai 1 jar
    jar = [
        "mysql:mysql-connector-java:8.0.33",
        "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1"
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

    schema = StructType([
        StructField("actor", StructType([
            StructField("id", IntegerType(), True),
            StructField("login", StringType(), True),
            StructField("gravatar_id", StringType(), True),
            StructField("url", StringType(), True),
            StructField("avatar_url", StringType(), True)
        ]), True),
        StructField("repo", StructType([
            StructField("id", LongType(), False),
            StructField("name", StringType(), True),
            StructField("url", StringType(), True)
        ]), True)
    ])
    df = spark_connect.spark.read.schema(schema).json(r"C:\Users\Du\PycharmProjects\PythonProject\data\2015-03-01-17.json")

    df_write_table = df.withColumn("spark_temp",lit("sparkwrite")).select(
        col("actor.id").alias("user_id"),
        col("actor.login").alias("login"),
        col("actor.gravatar_id").alias("gravatar_id"),
        col("actor.url").alias("url"),
        col("actor.avatar_url").alias("avatar_url"),
        col("spark_temp").alias("spark_temp")
    )
    # lấy cấu hình spark
    spark_config = get_spark_config()
    # tạo df để ghi data bằng spark
    df = SparkWriteDatabases(spark_connect.spark,spark_config)
    df.write_all_databases(df_write_table)
    #validate
    df.validate_spark_all_databases(df_write_table)
if __name__ == "__main__":
    main()

