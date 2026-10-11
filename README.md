# USGS earthquake pipeline

This project reads the [USGS all-earthquakes past-day GeoJSON feed](https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson) and displays recent records at <http://127.0.0.1:5000/>. The feed is updated every minute. Two producers poll it every 60 seconds: one publishes magnitudes below 2.5 to `usgs-earthquakes-below-2-5`, and the other publishes magnitudes 2.5 and above to `usgs-earthquakes-2-5-plus`. Each poll sends up to five recent events for its band. Repeated sightings update the same Cassandra row rather than creating duplicate rows.

The message carries the USGS event ID, magnitude, place, occurrence and source-update times, capture time, details URL, longitude, latitude, and depth. Spark Structured Streaming parses both Kafka topics and writes valid rows to `finalproject.earthquakes_by_day`. Four independent demo consumers, two per topic, show Kafka delivery. Flask queries Cassandra and refreshes its page every 60 seconds.

## Open the website

The Flask server is [app.py](app.py). The page markup is [templates/index.html](templates/index.html), and its styles are [static/tokens.css](static/tokens.css) and [static/style.css](static/style.css). Open <http://127.0.0.1:5000/> in your Windows or WSL browser. This is the earthquake page with the summary and table; Spark's separate dashboard is at <http://localhost:4040/>.

Flask is currently running in the prepared tmux session's `website` window. Attach with `tmux attach -t ds2-finalproject-demo`, then press `Ctrl-b` followed by `6` to see its server output. If the server has stopped, run this command from the workspace root in that window or another terminal:

```bash
cd /mnt/d/Documents/ds2
.venv/bin/python -u project/app.py
```

Leave that terminal open while using the page. Start the existing Cassandra container first if the page reports that the database is unavailable: `docker start ds2-finalproject-cassandra`. Do not start a second copy of Flask when port 5000 is already in use.

## Open the Spark dashboard

Open <http://localhost:4040/> and select **Structured Streaming**. This dashboard belongs to `project/stream.py` and is available while the Spark application runs. The prepared tmux session keeps the job in window `8`, `spark`; press `Ctrl-b` followed by `8` to inspect its output. Kafka runs in window `7`.

Use `localhost` for Spark in your Windows browser. On this setup, the Windows check reached `localhost:4040` but could not reach `127.0.0.1:4040`.

If port `4040` is unavailable, check whether the streaming job stopped. After Kafka and Cassandra are ready, use the Spark command in [Start or restart on this WSL machine](#start-or-restart-on-this-wsl-machine), or the standalone command in [the recording script](docs/final_script.md#keep-the-spark-dashboard-running). Leave it running while using the dashboard. Flask can still display previously stored rows when Spark is stopped, so an available Flask page alone does not establish that streaming is active.

## File map

```text
ds2/
├── .venv/                         shared Python environment
└── project/
    ├── app.py                     Flask server and Cassandra queries
    ├── templates/
    │   └── index.html             website HTML
    ├── static/
    │   ├── tokens.css             colors and typography variables
    │   └── style.css              website layout
    ├── producer_low.py            below-2.5 producer entry point
    ├── producer_high.py           2.5-and-above producer entry point
    ├── producer.py                shared USGS polling and Kafka delivery
    ├── consumer.py                four consumer slots
    ├── demo_query.py              generate CQL for the event chosen in the video
    ├── model.py                   earthquake message fields and validation
    ├── stream.py                  Spark stream and Cassandra writes
    ├── schema.cql                 Cassandra keyspace and table
    ├── requirements.txt           Python dependencies for the shared .venv
    └── docs/
        ├── final_script.md        recording commands and narration
        ├── evidence_handoff.md    verified facts and links for Luigi's report
        └── screenshots/          captured workflow evidence
```

## What the course requires and what we chose

The [project instructions](instructions.md) require a changing source with at least five fields, two repeating producers and separate Kafka topics, Spark Structured Streaming, Cassandra, a Flask page, workflow evidence, a video, and member contributions. The [rubric](Project_Rubric.md) also grades four independent consumers and at least five working references.

This machine's setup choices are WSL Kafka 4.3.1 and Spark 4.2.0, the shared workspace `.venv` managed by `uv`, Cassandra 5.0.9 in the WSL Docker Engine with a named volume, the 2.5 magnitude split, and a `foreachBatch` Cassandra sink. The course instructions do not prescribe those versions, Docker, a named volume, or installation screenshots. The Spark Kafka package matches the installed Spark version; the Python Cassandra driver writes each microbatch because the available Spark Cassandra connector did not cover Spark 4.2.0.

## Start or restart on this WSL machine

Run commands from `/mnt/d/Documents/ds2`. Use separate tmux windows or panes for long-running commands. Install the project dependencies into the existing shared environment if they are missing:

```bash
cd /mnt/d/Documents/ds2
uv pip install --python .venv/bin/python -r project/requirements.txt
```

The Cassandra container and named volume already exist. Start the existing container and check readiness. On a fresh machine only, create the volume and container instead of `docker start`:

```bash
docker start ds2-finalproject-cassandra
docker exec ds2-finalproject-cassandra nodetool status
```

If `nodetool` cannot connect immediately after startup, wait and retry until the node reports `UN` (up and normal) before applying the schema or starting Spark.

```bash
docker volume create ds2-finalproject-cassandra-data
docker run -d --name ds2-finalproject-cassandra \
  -e MAX_HEAP_SIZE=1G \
  -p 127.0.0.1:9042:9042 \
  -v ds2-finalproject-cassandra-data:/var/lib/cassandra \
  cassandra:5.0.9
```

Start the already formatted Kafka broker in another pane. Its KRaft data is at `~/kafka-data/ds2-kraft`; **do not run `kafka-storage.sh format` again**. Create the two topics if this is a new broker, then apply the idempotent CQL schema:

```bash
KAFKA_HEAP_OPTS='-Xms512m -Xmx512m' "$KAFKA_HOME/bin/kafka-server-start.sh" "$KAFKA_HOME/config/server.properties"
```

```bash
"$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server localhost:9092 --create --if-not-exists --topic usgs-earthquakes-below-2-5 --partitions 1 --replication-factor 1
"$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server localhost:9092 --create --if-not-exists --topic usgs-earthquakes-2-5-plus --partitions 1 --replication-factor 1
docker exec -i ds2-finalproject-cassandra cqlsh < project/schema.cql
```

Start Spark in its own pane after Kafka and Cassandra are ready. The first run with `--packages` needs network access to download the Spark Kafka connector; it is cached locally on this machine now. The Python environment variables point Spark at the shared `.venv`:

```bash
export PYSPARK_PYTHON="$PWD/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="$PWD/.venv/bin/python"
"$SPARK_HOME/bin/spark-submit" --master 'local[1]' --conf spark.driver.memory=512m \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 project/stream.py
```

Start the two producers and Flask in three more panes:

```bash
.venv/bin/python -u project/producer_low.py
.venv/bin/python -u project/producer_high.py
.venv/bin/python -u project/app.py
```

For the four independently reading consumers, run one command in each of four panes:

```bash
.venv/bin/python -u project/consumer.py low-1
.venv/bin/python -u project/consumer.py low-2
.venv/bin/python -u project/consumer.py high-1
.venv/bin/python -u project/consumer.py high-2
```

Each consumer has its own Kafka group, so both consumers for a topic can show the same messages. The prepared layout in `tmux attach -t ds2-finalproject-demo` has eight windows: two producer panes, two low-consumer panes, two high-consumer panes, cqlsh, Kafka topic descriptions, the Flask website server, Kafka, and Spark. Cassandra runs in Docker. If a service has stopped by recording time, use the startup commands above.

## Verify the flow

Open the [Spark dashboard](http://localhost:4040/) and its **Structured Streaming** tab, then the [Flask page](http://127.0.0.1:5000/). In a terminal, verify topics and stored rows:

```bash
"$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server localhost:9092 --describe --topic usgs-earthquakes-below-2-5
"$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server localhost:9092 --describe --topic usgs-earthquakes-2-5-plus
docker exec -it ds2-finalproject-cassandra cqlsh
```

At the `cqlsh>` prompt:

```sql
SELECT event_day, event_id, magnitude, source_topic FROM finalproject.earthquakes_by_day LIMIT 20;
```

To follow the newest displayed event, reload Flask and scroll the table to the top at Step 1. Copy the first row's **Event ID** and date from **Occurred (UTC)**, then click that row's **Details** link. Keep this same ID through the recording even if newer events arrive. Flask sorts by occurrence time; the overview query above does not.

To generate the matching single-event CQL query, run this in a WSL shell outside the recorded window:

```bash
cd /mnt/d/documents/ds2
.venv/bin/python project/demo_query.py
```

Paste the ID from Step 1 when prompted, then copy the generated SQL into cqlsh at Step 7. The helper finds the stored date in the same two UTC-day partitions Flask reads. It only reads Cassandra and prints the query; it does not change stored rows or choose a different event. You can also pass the copied ID as a command-line argument. The helper is preparation and does not need to appear in the video. No fixed event ID or date is part of the recording procedure.

The producer logs count **messages per poll**, while Cassandra and Flask show **distinct event IDs**. USGS may repeat the same event in successive polls, so these counts need not match. See [the recording script](docs/final_script.md) for the exact screen sequence and [the technical evidence handoff](docs/evidence_handoff.md) for captioned screenshots and source links that Luigi can use in his report.
