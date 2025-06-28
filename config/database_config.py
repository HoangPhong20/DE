from dotenv import load_dotenv
import os
from dataclasses import dataclass

@dataclass
class MySQLConfig():
    host : str
    port : int
    user : str
    password : str
    database : str
    table : str = "users"

@dataclass
class MongoDBConfig():
    uri : str
    db_name : str
    collections : str = "users"

def get_database_config():
    load_dotenv()
    config = {
        "mysql" : MySQLConfig(
                    host = os.getenv("MYSQL_HOST"),
                    port = os.getenv("MYSQL_PORT"),
                    user = os.getenv("MYSQL_USER"),
                    password = os.getenv("MYSQL_PASSWORD"),
                    database = os.getenv("MYSQL_DATABASE"),
                    table = "users"
                    ),
        "mongoDB" : MongoDBConfig(
                    uri = os.getenv("MONGO_URI"),
                    db_name = os.getenv("MONGO_DB_NAME")
                    )
    }
    return config

def get_spark_config():
    db_config = get_database_config()

    return {
        "mysql" : {
            "table" : db_config["mysql"].table,
            "jdbc_url" : "jdbc:mysql://{}:{}/{}".format(db_config["mysql"].host,db_config["mysql"].port,db_config["mysql"].database),
            # cach 1 de viet config
            "config" : {
                "host" : db_config["mysql"].host,
                "port" : db_config["mysql"].port,
                "user" : db_config["mysql"].user,
                "password" : db_config["mysql"].password,
                "database" : db_config["mysql"].database
            }
        },

        "mongoDB" : {
            "uri" : db_config["mongoDB"].uri,
            # cach 2
            "database" : db_config["mongoDB"].db_name,
            "collection" : db_config["mongoDB"].collections

        },
        "redis" : {}
    }

# if __name__ == "__main__" :
#     config = get_spark_config()
#     print(config)

