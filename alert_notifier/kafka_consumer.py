from confluent_kafka import Consumer
import json
import os

class AlertNotifierConsumer:
    def __init__(self):
        self.consumer = Consumer({
            "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
            "group.id": "alert-notifier",
            "auto.offset.reset": "latest"
        })

        topic = os.getenv("KAFKA_NOTIFIER_TOPIC")
        if not topic:
            raise RuntimeError("KAFKA_NOTIFIER_TOPIC not set")

        self.consumer.subscribe([topic])

    def poll(self):
        msg = self.consumer.poll(1.0)
        if msg is None:
            return None
        if msg.error():
            print("[Notifier] Kafka error:", msg.error(), flush=True)
            return None

        return json.loads(msg.value().decode("utf-8"))
