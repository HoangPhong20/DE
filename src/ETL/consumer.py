from kafka import KafkaConsumer

consumer = KafkaConsumer("phong", bootstrap_servers = "localhost:9092")
running = True
count_msg = 0
print("Waiting for messages...")
while running:
    msg_pack = consumer.poll(timeout_ms=500)
    # consumer.poll() là hàm của KafkaConsumer, dùng để lấy message từ Kafka.
    for tp,msgs in msg_pack.items():
        # .items Lấy từng cặp (TopicPartition, [messages])
        for msg in msgs:
            print(msg.value.decode('utf-8')) #decode
            count_msg += 1
            print(f"------------{count_msg}----------------")
