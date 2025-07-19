from kafka import KafkaProducer
from databases.mysql_connect import MySQLConnect
from config.database_config import get_database_config
import json


def get_data_trigger(mysql_client,last_timestamp):

        connection, cursor = mysql_client.connection, mysql_client.cursor
        database = "github_data"
        connection.database = database

        query = ("SELECT user_id, login, gravatar_id, url, avatar_url, state, "
                       " DATE_FORMAT(log_timestamp, '%Y-%m-%d %H:%i:%s.%f') AS log_timestamp1"
                       " FROM users_log_after")

        if last_timestamp:
            query += f" WHERE DATE_FORMAT(log_timestamp, '%Y-%m-%d %H:%i:%s.%f') > '{last_timestamp}'"
            cursor.execute(query)
        else:
            cursor.execute(query)

        rows = cursor.fetchall()
        connection.commit()

        schema = ["user_id", "login", "gravatar_id", "url", "avatar_url", "state", "log_timestamp"]
        data = [dict(zip(schema, row)) for row in rows]

        max_timestamp = max((row["log_timestamp"] for row in data), default = last_timestamp) if data else last_timestamp

        return data, max_timestamp
def main():
    last_timestamp = None
    config = get_database_config()
    with MySQLConnect(config["mysql"].host, config["mysql"].port, config["mysql"].user,
                          config["mysql"].password) as mysql_client:

        producer = KafkaProducer(bootstrap_servers="localhost:9092"
                                     , value_serializer=lambda x: json.dumps(x).encode('utf-8'))

        while True:
            data , max_timestamp = get_data_trigger(mysql_client, last_timestamp)
            last_timestamp = max_timestamp
            print(f"----------last_timestamp = {last_timestamp}------------")
            for record in data:
                producer.send("phong", record)
                    # print(record)
if __name__ == "__main__":
    main()
