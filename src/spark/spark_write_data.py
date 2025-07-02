from pyspark.sql import SparkSession, DataFrame
from typing import Dict

from pyspark.sql.functions import lit

from databases.mongoDB_connect import MongoDBConnect
from databases.mysql_connect import MySQLConnect


class SparkWriteDatabases:
    def __init__(self,spark : SparkSession, db_config : Dict):
        self.spark = spark
        self.db_config = db_config

    def spark_write_mysql(self, df : DataFrame, table_name : str, jdbc_url : str, config : Dict, mode : str = "append"):
        #spark mạnh ở read,write và data transformation, nên dùng python cursor để tạo cột
        try:
            with MySQLConnect(config["host"], config["port"], config["user"],
                              config["password"]) as mysql_client:
                connection, cursor = mysql_client.connection, mysql_client.cursor
                database = "github_data"
                connection.database = database
                cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN spark_temp VARCHAR(255)")
                connection.commit()
                print(f"-------Add column spark_temp to mysql success------------")
                mysql_client.close()
        except Exception as e:
            raise Exception(f"---------fail to connect mysql: {e}-------------")

        df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", table_name) \
            .option("user", config["user"]) \
            .option("password", config["password"]) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .mode(mode) \
            .save()
        print(f"-----------Spark write data to mysql table : {table_name} success---------")

    # sql syntax do spark chi nhan 1 query chu ko nhan query phuc tap
    def validate_spark_mysql(self,df_write : DataFrame, table_name : str, jdbc_url : str, config : Dict, mode : str = "append"):
        df_read = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", f"(SELECT * FROM {table_name} WHERE spark_temp = 'sparkwrite') AS subq") \
            .option("user", config["user"]) \
            .option("password", config["password"]) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .load()
        # df_read.show()
        def subtract_df(df_spark_write : DataFrame, df_read_database : DataFrame):
            result = df_spark_write.exceptAll(df_read_database) # khac subtract o cho ko drop duplicate
            print(f"-----------records missing : {result.count()}------------")
            if not result.isEmpty():
                result.write \
                    .format("jdbc") \
                    .option("url", jdbc_url) \
                    .option("dbtable", table_name) \
                    .option("user", config["user"]) \
                    .option("password", config["password"]) \
                    .option("driver", "com.mysql.cj.jdbc.Driver") \
                    .mode(mode) \
                    .save()
                print(f"-----------spark write missing records to mysql table : {table_name} success------------")
        # so sanh 2 df
        if df_write.count() == df_read.count():
            print(f"-----------validate {df_read.count()} records success----------")
        else:
            print(f"-----------spark miss inserted records------------")
            subtract_df(df_write,df_read)
        # drop column spark_temp
        try:
            with MySQLConnect(config["host"], config["port"], config["user"],
                              config["password"]) as mysql_client:
                connection, cursor = mysql_client.connection, mysql_client.cursor
                database = "github_data"
                connection.database = database
                cursor.execute(f"ALTER TABLE {table_name} DROP COLUMN spark_temp ")
                connection.commit()
                print(f"-------Drop column spark_temp to mysql success------------")
                mysql_client.close()
        except Exception as e:
            raise Exception(f"---------fail to drop column mysql: {e}-------------")

        print("-----------Validate spark write data to MYSQL success--------------")

    def spark_write_mysql_primaryKey(self, df: DataFrame, jdbc_url: str, config: Dict, mode: str = "append"):
        df.write \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", "spark_table_temp") \
                .option("user", config["user"]) \
                .option("password", config["password"]) \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .mode(mode) \
                .save()
        print(f"-----spark write data to mysql table: spark_table_temp successfully-------")

    def validate_spark_write_primaryKey(self, df_write: DataFrame, jdbc_url: str, config: Dict,mode: str = "append"):
        try:
            df_read = self.spark.read \
                    .format("jdbc") \
                    .option("url", jdbc_url) \
                    .option("dbtable", "spark_table_temp") \
                    .option("user", config["user"]) \
                    .option("password", config["password"]) \
                    .option("driver", "com.mysql.cj.jdbc.Driver") \
                    .load()
            df_temp = df_write.exceptAll(df_read)
            if df_temp.count() != 0:
                df_temp.write \
                    .format("jdbc") \
                    .option("url", jdbc_url) \
                    .option("dbtable", "spark_table_temp") \
                    .option("user", config["user"]) \
                    .option("password", config["password"]) \
                    .option("driver", "com.mysql.cj.jdbc.Driver") \
                    .mode(mode) \
                    .save()
                print(f"-------validate spark write data to mysql table spark_table_temp successfully------")
        except Exception as e:
            raise Exception(f"----failed to write missing record to spark_table_temp in mysql----")

    def insert_data_mysql_primaryKey(self, config: Dict):
        try:
            with MySQLConnect(config["host"], config["port"], config["user"],
                              config["password"]) as mysql_client:
                connection, cursor = mysql_client.connection, mysql_client.cursor
                database = "github_data"
                connection.database = database
                cursor.execute(
                    "SELECT a. * FROM spark_table_temp a LEFT JOIN users b ON a.user_id = b.user_id WHERE b.user_id IS NULL;")
                for rec in cursor.fetchall():
                    try:
                        cursor.execute(
                            "INSERT INTO users (user_id, login, gravatar_id, url, avatar_url) VALUES (%s, %s, %s, %s, %s)",
                            rec
                        )
                        connection.commit()
                        print("-----insert data into mysql successfully-----")
                    except Exception as e:
                        print(f"Error inserting record {rec}: {str(e)}")
                        continue
        except Exception as e:
                raise Exception(f"-----failed to connect to mysql: {e}------")

    def write_all_databases(self, df: DataFrame, mode: str = "append"):
        self.spark_write_mysql(
            df,
            self.db_config["mysql"]["table"],
            self.db_config["mysql"]["jdbc_url"],
            self.db_config["mysql"]["config"],
            mode
        ),
        self.spark_write_mysql_primaryKey(
            df,
            self.db_config["mysql"]["jdbc_url"],
            self.db_config["mysql"]["config"],
            mode
        )
        # self.spark_write_mongodb(
        #     df,
        #     self.db_config["mongoDB"]["uri"],
        #     self.db_config["mongoDB"]["database"],
        #     self.db_config["mongoDB"]["collection"],
        #     mode
        # )
        print(f"-----------Spark write to all database success---------")



