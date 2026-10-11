import sys
import time

import requests
from confluent_kafka import KafkaException, Producer

from model import MagnitudeBand, QuakeMessage, band_for, parse_usgs_feature


USGS_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"
POLL_SECONDS = 60
EVENTS_PER_POLL = 5


def select_events(
    features: list[object], captured_at_ms: int, band: MagnitudeBand
) -> tuple[list[QuakeMessage], int]:
    parsed = [parse_usgs_feature(feature, captured_at_ms) for feature in features]
    rejected = sum(event is None for event in parsed)
    selected = [
        event for event in parsed if event is not None and band_for(event.magnitude) is band
    ]
    selected.sort(key=lambda event: event.occurred_at_ms, reverse=True)
    return selected[:EVENTS_PER_POLL], rejected


def fetch_events(band: MagnitudeBand) -> tuple[list[QuakeMessage], int]:
    response = requests.get(USGS_URL, timeout=15)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError("USGS response is not an object")
    features = payload.get("features")
    if not isinstance(features, list):
        raise ValueError("USGS response has no features list")
    return select_events(features, int(time.time() * 1000), band)


def run_producer(band: MagnitudeBand) -> None:
    producer = Producer(
        {"bootstrap.servers": "localhost:9092", "client.id": f"finalproject-{band.name.lower()}"}
    )
    while True:
        try:
            events, rejected = fetch_events(band)
            delivery_errors: list[str] = []

            def on_delivery(error: object, message: object) -> None:
                if error is not None:
                    delivery_errors.append(str(error))

            for event in events:
                producer.produce(
                    band.value,
                    key=event.event_id.encode("utf-8"),
                    value=event.to_json().encode("utf-8"),
                    on_delivery=on_delivery,
                )
            pending = producer.flush(20)
            if pending or delivery_errors:
                raise RuntimeError(
                    f"{pending} messages still queued; delivery errors: {delivery_errors}"
                )
            for event in events:
                print(
                    f"Delivered topic={band.value} id={event.event_id} "
                    f"magnitude={event.magnitude:.2f} place={event.place}",
                    flush=True,
                )
            print(
                f"Poll complete: delivered={len(events)} rejected={rejected} topic={band.value}",
                flush=True,
            )
        except (requests.RequestException, ValueError, KafkaException, BufferError, RuntimeError) as error:
            print(f"Producer {band.value} failed: {error}", file=sys.stderr, flush=True)
        time.sleep(POLL_SECONDS)
