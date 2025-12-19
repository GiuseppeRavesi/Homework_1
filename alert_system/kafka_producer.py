from confluent_kafka import Producer
import os
import json

class AlertProducer:
    def __init__(self):
        self.producer = Producer({
            "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS")
        })
        self.topic = os.getenv("KAFKA_NOTIFIER_TOPIC")

    def send_alert(self, alert: dict):
        self.producer.produce(
            self.topic,
            json.dumps(alert).encode("utf-8")
        )
        self.producer.flush()
        print("[ALERT] Inviato evento a notifier", flush=True)
