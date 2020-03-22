from confluent_kafka import Consumer


def kafka_consumer():
    
    consumer = Consumer({
        "bootstrap.servers": 'PLAINTEXT://localhost:9092',
        "group.id": '0',
        "auto.offset.reset": 'earliest'
    })
    
    consumer.subscribe(topics=["PoliceCalls"])
    
    try:
        while True:
            msg = consumer.poll(timeout=2.0)
            
            if msg is None:
                print("No message received")
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue
            else:
                print(f"Received message: {msg.value().decode('utf-8')}")

    except KeyboardInterrupt:
        logging.info("Stopping Kafka consumer")
        consumer.close()


if __name__ == "__main__":
    kafka_consumer()