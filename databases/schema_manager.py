from pathlib import Path
from mysql.connector import Error
SQL_FILE_PATH = Path("../sql/schema.sql")

def create_mysql_schema(connection, cursor):

    database = "github_data"
    cursor.execute(f"DROP DATABASE IF EXISTS {database}")
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database}")

    connection.commit()
    print(f"-------Create {database} success------------")
    connection.database = database
    try:
        with open(SQL_FILE_PATH,"r") as f:
            sql_script = f.read()
            sql_commands = [cmd.strip() for cmd in sql_script.split(";") if cmd.strip()]

            for cmd in sql_commands:
                cursor.execute(cmd)
                print(f"---------Executed mysql command-------------")

        connection.commit()
    except Error as e:
        connection.rollback()
        raise Exception(f"----------Failed to create MySQL schema: {e} ------------") from e

def validate_mysql_schema(cursor):
    cursor.execute("SHOW TABLES")
    tables = [row[0] for row in cursor.fetchall()] #fetchall sẽ fetch dữ liệu ra rồi xóa
    if "users" not in tables or "repositories" not in tables:
        raise ValueError("----------Table doesn't exist-----------")

    cursor.execute("SELECT * FROM users where user_id = 1")
    user = cursor.fetchone()
    if not user:
        raise ValueError("----------user not found-------------")
    print("-----------Validate schema in Mysql success-------------")

def create_mongo_schema(db):
    db.drop_collection("users")
    db.create_collection(
        "users", validator = {
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["user_id", "login"],
                "properties": {
                    "user_id": {
                        "bsonType": "int"
                    },
                    "login": {
                        "bsonType": "string"
                    },
                    "gravatar_id": {
                        "bsonType": ["string", "null"]
                    },
                    "url": {
                        "bsonType": ["string", "null"]
                    },
                    "avatar_url": {
                        "bsonType": ["string", "null"]
                    },
                }
            }

    })
    print("----------created collection users in MongoDB------------")

def validate_mongodb_schema(db):
    collections = db.list_collection_names()
    # print(f"-----------collection: {collections}-------------")
    if "users" not in collections:
         raise ValueError("----------collection in Mongo doesn't exist-----------")

    user = db.users.find_one({"user_id" : 1})
    # print(user)
    if not user:
        raise ValueError("------------user not found in Mongodb----------")

    print("-----------validate success in Mongodb------------")
