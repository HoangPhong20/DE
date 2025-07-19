

from kafka import KafkaConsumer

consumer = KafkaConsumer("phong", bootstrap_servers = "localhost:9092")

running = True
while running:
    msg_pack = consumer.poll(timeout_ms=500)
    for tp,msgs in msg_pack.items():
        for msg in msgs:
            print(msg.value.decode('utf-8')) #decode