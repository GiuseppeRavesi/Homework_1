from kafka_consumer import AlertNotifierConsumer
from smtp_notifier import send_email

def main():
    consumer = AlertNotifierConsumer()
    print("[AlertNotifier] Avviato", flush=True)

    while True:
        event = consumer.poll()
        if not event:
            continue

        email = event["email"]
        airport = event["airport"]
        condition = event["condition"]

        subject = f"Allerta voli aeroporto {airport}"
        body = condition

        send_email(
            to_email=email,
            subject=subject,
            body=body
        )

if __name__ == "__main__":
    main()
