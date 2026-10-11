# Technical evidence handoff for Luigi's report

This file is a set of verified facts, screenshots, and source links for Jon Luigi L. Ariola, called Luigi in these project notes. It is not the final submission report. Luigi will write that report and its discussion in his own words. Zel will handle the technical project work and record the video.

## Requirements and implementation facts

The [project instructions](../instructions.md) require a live source with at least five fields, two repeated producers and separate Kafka topics, Spark Structured Streaming, Cassandra storage, a Flask page, screenshots or terminal proof, a full-flow video, and student contributions. The [rubric](../Project_Rubric.md) also grades four independent consumers and five working references.

The implementation uses the [USGS past-day GeoJSON feed][usgs]. It contains earthquake features with `id`, `properties.mag`, `properties.place`, `properties.time`, `properties.updated`, `properties.url`, and `geometry.coordinates` (longitude, latitude, and depth). `project/model.py` validates these fields and creates structured JSON messages. `project/producer_low.py` sends magnitudes below 2.5 to `usgs-earthquakes-below-2-5`; `project/producer_high.py` sends magnitudes 2.5 and above to `usgs-earthquakes-2-5-plus`. Each process polls the feed every 60 seconds and sends up to five recent events in its band. The 2.5 split and poll size are implementation choices, not course requirements.

Four independent consumers each use their own Kafka group. `low-1` and `low-2` read the lower-magnitude topic; `high-1` and `high-2` read the other topic. [Spark Structured Streaming][spark-kafka] subscribes to both topics, parses JSON into a streaming DataFrame, checks required fields and topic/band agreement, and uses [`foreachBatch`][foreachbatch] to write each microbatch with the Cassandra Python driver. The CQL schema has a partition for the UTC event date and one row per event ID. Later captures of the same event update the row, and an older replay cannot overwrite a newer capture. Flask queries today's and yesterday's partitions, shows the latest 50 events, and refreshes every 15 seconds.

This machine uses a preinstalled WSL Kafka broker and Spark installation, the shared workspace `.venv` managed with `uv`, and a Cassandra Docker container backed by the named volume `ds2-finalproject-cassandra-data`. These are local setup choices. The course instructions do not require Docker or installation screenshots. See the [startup and verification guide](../README.md) for exact commands.

## Evidence captured on this WSL machine

The screenshots below were captured from the running pipeline on 2026-10-11. IDs, counts, and batches will change as the live feed changes. The producer count is messages sent per poll; the Cassandra and Flask counts are distinct event rows.

| Evidence | What it shows |
| :--- | :--- |
| [Both producers](screenshots/producers.png) | Each producer repeatedly delivers to its distinct topic; both panes show recent IDs and magnitudes. |
| [Kafka topics](screenshots/kafka-topics.png) | Kafka describes both separate topics and their active partitions. |
| [Low consumers](screenshots/low-consumers.png) | `low-1` and `low-2` independently read the lower-magnitude topic with matching IDs and offsets. |
| [High consumers](screenshots/high-consumers.png) | `high-1` and `high-2` independently read the 2.5-and-above topic with matching IDs and offsets. |
| [Spark streaming dashboard](screenshots/spark-streaming.png) | The Structured Streaming query is running and displays batch/input metrics. |
| [Cassandra table](screenshots/database.png) | `cqlsh` returns stored IDs, magnitudes, and both source topics. |
| [Flask page](screenshots/flask-page.png) | The page shows counts, latest capture time, and earthquake rows read from Cassandra. |
| [Flask lower rows](screenshots/flask-high-rows.png) | Scrolling reveals records from both magnitude bands. |

One verified event, `aka2026udlptb`, appeared in the lower-magnitude producer/consumer output, Cassandra, and Flask with occurrence time `2026-10-11 09:42:17 UTC`. This is an example from the capture session, not a fixed ID for future recordings.

## Problems encountered and resolutions

Kafka's old KRaft data path was under `/tmp`, and that directory was gone when this project started. After checking that no `meta.properties` backup was available in WSL and receiving approval, we configured a new store at `~/kafka-data/ds2-kraft` and formatted that empty store once. It is now the active local cluster; messages from the missing old store cannot be recovered. The new store must be reused, not reformatted.

An early Spark-to-Cassandra check exposed an eight-hour shift in the stored occurrence time. The sink now constructs UTC-aware timestamps directly from the source's millisecond values. We checked the same event ID in the Kafka consumer, Cassandra, and Flask after the correction; all three showed the matching UTC time.

## Facts and items for Luigi to complete

**Accessible YouTube link:** Pending recording and upload. Use the [recording script](final_script.md) to show source, Kafka, all four consumers, Spark, Cassandra, Flask, and contributions. Luigi must add the final link to his report before submission.

**Member names and assigned components:** The current role split below comes from the team. Luigi should obtain each person's own learning statement for this project.

| Member | Agreed role |
| :--- | :--- |
| Marl Zeldrix M. Caliliw | Technical implementation, integration, and verification on WSL; video recording. |
| Jon Luigi L. Ariola | Write the final project report and discussion. |

## Candidate references for Luigi to cite

1. [USGS GeoJSON summary format and past-day feed][usgs]. Source format, fields, and feed update frequency.
2. [Apache Kafka quickstart][kafka]. Local broker startup, topics, and console client commands.
3. [Apache Spark Structured Streaming Kafka integration][spark-kafka]. Reading Kafka topics as a stream.
4. [PySpark `foreachBatch` API][foreachbatch]. Processing and writing each streaming microbatch.
5. [Apache Cassandra CQL documentation][cql]. Keyspace, table, and query syntax.
6. [Flask quickstart][flask]. Routes, templates, and local web application behavior.

[usgs]: https://earthquake.usgs.gov/earthquakes/feed/v1.0/geojson.php
[kafka]: https://kafka.apache.org/quickstart/
[spark-kafka]: https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
[foreachbatch]: https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.DataStreamWriter.foreachBatch.html
[cql]: https://cassandra.apache.org/doc/latest/cassandra/developing/cql/index.html
[flask]: https://flask.palletsprojects.com/en/stable/quickstart/
