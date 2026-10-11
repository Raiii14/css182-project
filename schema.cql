CREATE KEYSPACE IF NOT EXISTS finalproject
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS finalproject.earthquakes_by_day (
    event_day date,
    event_id text,
    magnitude double,
    place text,
    occurred_at timestamp,
    source_updated_at timestamp,
    captured_at timestamp,
    details_url text,
    longitude double,
    latitude double,
    depth_km double,
    source_topic text,
    PRIMARY KEY ((event_day), event_id)
);
