"""
MQTT-to-Kafka bridge.

Subscribes to every per-well MQTT topic (facies/welllog/<WELL_NAME>) and
republishes each message, unchanged, to a single Kafka topic
(facies-welllog). The well name is used as the Kafka message key, which
Kafka uses to consistently route messages from the same well to the same
partition.

This bridge sits between two different messaging philosophies:
- MQTT: lightweight, many small topics, built for simple pub/sub (IoT-style)
- Kafka: fewer topics, partitioned internally, built for durable,
  high-throughput stream processing

Usage:
    python bridge.py
    python bridge.py --mqtt-broker localhost --kafka-broker localhost:9092
"""

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
    # "facies/welllog/CROSS H CATTLE" -> "CROSS H CATTLE"
    # split with maxsplit=2 so well names containing "/" (none in this
    # dataset, but defensive anyway) are not accidentally truncated.
    return mqtt_topic.split("/", 2)[2]


def build_kafka_producer(kafka_broker: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=kafka_broker,
        # Explicit api_version skips kafka-python's automatic broker
        # version probing, which is known to crash against newer Kafka
        # brokers (see github.com/dpkp/kafka-python/issues/2450).
        api_version=(2, 8, 1),
        # Messages are already JSON strings coming from the replayer;
        # encode them to bytes for Kafka, and encode the key separately.
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