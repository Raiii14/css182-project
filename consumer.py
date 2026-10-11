import argparse
import json
import sys
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaException

from model import MagnitudeBand


SLOTS = {
    "low-1": MagnitudeBand.LOW,
    "low-2": MagnitudeBand.LOW,
    "high-1": MagnitudeBand.HIGH,
    "high-2": MagnitudeBand.HIGH,
}


def main() -> None:
    parser = argparse.ArgumentParser(description="Read one final-project Kafka stream")
    parser.add_argument("slot", choices=SLOTS)
    args = parser.parse_args()
    band = SLOTS[args.slot]
    group = f"finalproject-{args.slot}"
    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([band.value])
    print(f"Consumer {args.slot} listening to {band.value} in group {group}", flush=True)
    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                raise KafkaException(message.error())
            try:
                event = json.loads(message.value())
                occurred = datetime.fromtimestamp(
                    event["occurred_at_ms"] / 1000, tz=timezone.utc
                ).strftime("%Y-%m-%d %H:%M:%S UTC")
                print(
                    f"[{args.slot}] topic={message.topic()} offset={message.offset()} "
                    f"id={event['event_id']} magnitude={event['magnitude']} "
                    f"place={event['place']}",
                    flush=True,
                )
                print(
                    f"  occurred={occurred} depth_km={event['depth_km']} "
                    f"coordinates=({event['longitude']}, {event['latitude']}) "
                    f"details={event['details_url']}",
                    flush=True,
                )
            except (ValueError, KeyError, TypeError) as error:
                print(f"[{args.slot}] malformed Kafka message: {error}", file=sys.stderr, flush=True)
    except KeyboardInterrupt:
        print(f"Consumer {args.slot} stopped.", flush=True)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
