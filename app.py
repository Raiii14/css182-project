from datetime import datetime, timedelta, timezone
from functools import lru_cache

from cassandra.cluster import Cluster, NoHostAvailable
from flask import Flask, make_response, render_template

from model import MagnitudeBand


app = Flask(__name__)

SELECT_DAY = """
SELECT event_day, event_id, magnitude, place, occurred_at, source_updated_at,
       captured_at, details_url, longitude, latitude, depth_km, source_topic
FROM finalproject.earthquakes_by_day WHERE event_day = %s
"""


@lru_cache(maxsize=1)
def cassandra_session():
    cluster = Cluster(["127.0.0.1"], port=9042)
    return cluster, cluster.connect("finalproject")


def recent_events(session, today, limit=50):
    rows = []
    for day in (today, today - timedelta(days=1)):
        rows.extend(session.execute(SELECT_DAY, (day,)))
    rows.sort(key=lambda row: row.occurred_at, reverse=True)
    return rows[:limit]


def display_time(value):
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


@app.get("/")
def index():
    now = datetime.now(timezone.utc)
    error = None
    try:
        _, session = cassandra_session()
        rows = recent_events(session, now.date())
    except NoHostAvailable:
        rows = []
        error = "Cassandra is unavailable. Start the database and refresh this page."

    events = [
        {
            "event_id": row.event_id,
            "magnitude": f"{row.magnitude:.2f}",
            "place": row.place,
            "occurred_at": display_time(row.occurred_at),
            "captured_at": display_time(row.captured_at),
            "details_url": row.details_url,
            "depth_km": f"{row.depth_km:.1f}" if row.depth_km is not None else "Unknown",
            "source_topic": row.source_topic,
            "band": "Below 2.5" if row.source_topic == MagnitudeBand.LOW.value else "2.5 and above",
        }
        for row in rows
    ]
    latest_capture = max((row.captured_at for row in rows), default=None)
    response = make_response(
        render_template(
            "index.html",
            events=events,
            count=len(events),
            low_count=sum(row.source_topic == MagnitudeBand.LOW.value for row in rows),
            high_count=sum(row.source_topic == MagnitudeBand.HIGH.value for row in rows),
            latest_capture=display_time(latest_capture) if latest_capture else "Waiting for data",
            rendered_at=display_time(now),
            error=error,
        ),
        503 if error else 200,
    )
    response.headers["Cache-Control"] = "no-store"
    return response


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=False, use_reloader=False)
