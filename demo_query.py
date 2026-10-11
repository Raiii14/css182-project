import argparse
from datetime import datetime, timedelta, timezone

from cassandra import OperationTimedOut
from cassandra.cluster import Cluster, NoHostAvailable


FIND_EVENT = """
SELECT event_day, event_id FROM finalproject.earthquakes_by_day
WHERE event_day = %s AND event_id = %s
"""


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate a CQL query for the event chosen in the Flask demo."
    )
    parser.add_argument("event_id", nargs="?", help="Event ID copied from Step 1")
    args = parser.parse_args()
    event_id = args.event_id if args.event_id is not None else input("Event ID from Step 1: ")
    event_id = event_id.strip()
    if not event_id:
        parser.error("Copy a nonempty Event ID from the Flask table.")

    today = datetime.now(timezone.utc).date()
    cluster = Cluster(["127.0.0.1"], port=9042)
    row = None
    try:
        session = cluster.connect("finalproject")
        for day in (today, today - timedelta(days=1)):
            row = session.execute(FIND_EVENT, (day, event_id)).one()
            if row is not None:
                break
    except (NoHostAvailable, OperationTimedOut) as error:
        parser.exit(1, f"Cassandra query failed: {error}\n")
    finally:
        cluster.shutdown()

    if row is None:
        parser.exit(
            1,
            "That ID was not found in today's or yesterday's UTC partitions. "
            "Copy the Event ID from a current Flask row.\n",
        )

    quoted_id = row.event_id.replace("'", "''")
    print(
        "SELECT event_id, magnitude, place, occurred_at, source_topic\n"
        "FROM finalproject.earthquakes_by_day\n"
        f"WHERE event_day = '{row.event_day}' AND event_id = '{quoted_id}';"
    )


if __name__ == "__main__":
    main()
