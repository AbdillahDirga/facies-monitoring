import argparse
import json

import paho.mqtt.client as mqtt
from kafka import KafkaProducer

MQTT_TOPIC_FILTER = "facies/welllog/#"
KAFKA_TOPIC = "facies-welllog"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bridge MQTT well log messages into Kafka")
    parser.add_argument("--mqtt-broker", type=str, default="localhost")
    parser.add_argument("--mqtt-port", type=int, default=1883)
    parser.add_argument("--kafka-broker", type=str, default="localhost:9094")
    return parser.parse_args()


def extract_well_name(mqtt_topic: str) -> str:
    return mqtt_topic.split("/", 2)[2]


def build_kafka_producer(kafka_broker: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=kafka_broker,
        api_version=(2, 8, 1),
        value_serializer=lambda v: v.encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )


def main():
    args = parse_args()
    producer = build_kafka_producer(args.kafka_broker)

    def on_connect(client, userdata, flags, rc, *extra):
        if rc == 0:
            print(f"Connected to MQTT broker at {args.mqtt_broker}:{args.mqtt_port}")
            client.subscribe(MQTT_TOPIC_FILTER)
            print(f"Subscribed to '{MQTT_TOPIC_FILTER}'\n")
        else:
            print(f"FAILED to connect to MQTT broker (rc={rc})")

    def on_message(client, userdata, msg):
        well_name = extract_well_name(msg.topic)
        payload = msg.payload.decode("utf-8")

        future = producer.send(KAFKA_TOPIC, key=well_name, value=payload)
        try:
            record_metadata = future.get(timeout=10)
            print(
                f"[{well_name}] MQTT '{msg.topic}' -> Kafka '{KAFKA_TOPIC}' "
                f"(partition={record_metadata.partition}, offset={record_metadata.offset})"
            )
        except Exception as e:
            print(f"[{well_name}] ERROR sending to Kafka: {e}")

    if hasattr(mqtt, "CallbackAPIVersion"):
        client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    else:
        client = mqtt.Client()

    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(args.mqtt_broker, args.mqtt_port, keepalive=60)

    print("Bridge running. Press Ctrl+C to stop.\n")
    try:
        client.loop_forever()
    except KeyboardInterrupt:
        print("\nStopping bridge...")
    finally:
        client.disconnect()
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()