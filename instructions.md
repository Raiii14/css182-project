Create a complete real-time data pipeline project that begins with a live or frequently changing website and ends with a Flask webpage displaying the stored data. Use Kafka as the message broker, Spark as the streaming processor, Cassandra as the database, and Flask as the web application. Your project must use two producers and two Kafka topics, and must show the full flow from data capture to final display.

 

Part 1: Choose a live data source. Select one public website or data source with changing or real-time information, such as traffic, weather, trading, crypto, or news. Extract at least five fields from the source.



Part 2: Create two producers. Build two separate producers that each scrape or capture data from the chosen source and send structured messages, preferably in JSON format, to Kafka.

Build two separate producers.
Each producer must scrape or capture data from the chosen source.
Each producer must send structured messages to Kafka.
Use JSON format if possible.
Each producer must continuously or repeatedly send data.


Part 3: Create two Kafka topics. Use one unique topic for each producer so that the data streams are separated properly.

Create one unique topic for each producer.
Use clear and meaningful topic names.
Keep the topics separate so the streams do not mix.
Verify that both topics are working properly.


Part 4: Stream the data with Spark. Use Spark Structured Streaming to read the Kafka data, convert it into a streaming DataFrame, process the selected fields, and write the output into Cassandra.

Use Spark Structured Streaming to read the Kafka data.
Convert the incoming messages into a streaming DataFrame.
Process the selected fields from the data stream.
Write the output into Cassandra.
Show that Spark is actively processing the stream.


Part 5: Store the data in Cassandra. Create the required keyspace and table, then verify that the streamed records are saved correctly and can be queried in cqlsh.

Create the required keyspace.
Create the required table.
Match the table columns with the extracted data fields.
Verify that the streamed records are saved correctly.
Query the stored data in cqlsh.


Part 6: Display the data in Flask. Build a Flask webpage that connects to Cassandra, reads the stored rows, and displays the data in a clean, scrollable layout.

Build a Flask webpage that connects to Cassandra.
Read the stored rows from the Cassandra table.
Display the data in a clean layout.
Ensure the webpage updates based on the stored data.


Part 7: Submit proof of the workflow. Include screenshots or terminal proof of both producers, the Spark dashboard, the Cassandra table with stored data, and the Flask webpage.

Include screenshots or terminal proof of Producer 1.
Include screenshots or terminal proof of Producer 2.
Include proof of the Spark dashboard.
Include proof of the Cassandra table with stored data.
Include proof of the Flask webpage.


Part 8: Submit a video demonstration / discussion. Record a video showing the full workflow from the live source to Kafka, Spark, Cassandra, and Flask.

Record a video showing the full workflow.
Show the live source being captured.
Show Kafka receiving the data.
Show Spark processing the stream.
Show Cassandra storing the data.
Show Flask displaying the final output.


Part 9: Student Contribution to the Project.

Part of the project per student
Learning for each student


Document requirements: Full Workflow

Indicate names of members
Indicate the accessible YouTube link in the document
Discussion / Screenshots
Follow other requirements prescribed in the course.
