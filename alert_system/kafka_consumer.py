from confluent_kafka import Consumer
import os
import json

class AlertEventConsumer:
    def __init__(self):
        self.consumer = Consumer({
            "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
            "group.id": "alert-system",
            "auto.offset.reset": "latest"
        })

        self.topic = os.getenv("KAFKA_ALERT_TOPIC")

        if not self.topic:
            raise RuntimeError("KAFKA_ALERT_TOPIC not set")

        self.consumer.subscribe([self.topic])

    def poll(self):
        msg = self.consumer.poll(1.0)

        if msg is None:
            return None

        if msg.error():
            print(f"[Kafka][ERROR] {msg.error()}")
            return None

        return json.loads(msg.value().decode("utf-8"))
