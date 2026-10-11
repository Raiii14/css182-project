import json
import math
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Mapping


MAGNITUDE_SPLIT = 2.5


class MagnitudeBand(Enum):
    LOW = "usgs-earthquakes-below-2-5"
    HIGH = "usgs-earthquakes-2-5-plus"


@dataclass(frozen=True)
class QuakeMessage:
    event_id: str
    magnitude: float
    place: str
    occurred_at_ms: int
    source_updated_at_ms: int
    captured_at_ms: int
    details_url: str
    longitude: float | None
    latitude: float | None
    depth_km: float | None

    def to_json(self) -> str:
        return json.dumps(asdict(self), allow_nan=False, separators=(",", ":"))


def band_for(magnitude: float) -> MagnitudeBand:
    return MagnitudeBand.LOW if magnitude < MAGNITUDE_SPLIT else MagnitudeBand.HIGH


def _number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    number = float(value)
    return number if math.isfinite(number) else None


def _timestamp(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        return None
    return value


def parse_usgs_feature(feature: object, captured_at_ms: int) -> QuakeMessage | None:
    if not isinstance(feature, Mapping):
        return None
    properties = feature.get("properties")
    geometry = feature.get("geometry")
    if not isinstance(properties, Mapping):
        return None

    event_id = feature.get("id")
    magnitude = _number(properties.get("mag"))
    place = properties.get("place")
    occurred_at_ms = _timestamp(properties.get("time"))
    source_updated_at_ms = _timestamp(properties.get("updated"))
    details_url = properties.get("url")
    if (
        not isinstance(event_id, str)
        or not event_id
        or magnitude is None
        or not isinstance(place, str)
        or not place
        or occurred_at_ms is None
        or source_updated_at_ms is None
        or not isinstance(details_url, str)
        or not details_url.startswith("https://earthquake.usgs.gov/earthquakes/eventpage/")
    ):
        return None

    coordinates = geometry.get("coordinates") if isinstance(geometry, Mapping) else None
    longitude = latitude = depth_km = None
    if isinstance(coordinates, list) and len(coordinates) >= 3:
        longitude = _number(coordinates[0])
        latitude = _number(coordinates[1])
        depth_km = _number(coordinates[2])

    return QuakeMessage(
        event_id=event_id,
        magnitude=magnitude,
        place=place,
        occurred_at_ms=occurred_at_ms,
        source_updated_at_ms=source_updated_at_ms,
        captured_at_ms=captured_at_ms,
        details_url=details_url,
        longitude=longitude,
        latitude=latitude,
        depth_km=depth_km,
    )
