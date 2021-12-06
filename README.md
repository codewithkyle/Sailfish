# Event Stream Prototype

Exploring and prototyping a custom event streaming architecture.

## Goals

-   [x] Create a system where n+1 number of producers can submit data
-   [x] Log data
-   [x] Create a system where n+1 number of approved consumers can sequentially read the data
-   [x] Create a system where a new consumer could process all previous data
-   [x] Create a system where a new consumer could join the live data stream after processing the entire log
-   [x] Create a system where consumers can fail & recover during the reading process
-   [x] Add persistence to consumer read statuses
-   [x] Add consumer status endpoint
-   [x] Setup spawnable consumer nodes
-   [x] Add consumer acknowledgement endpoint
-   [x] Custom event format
    -   [x] UUIDs
    -   [x] Event recieved timestamp
    -   [x] Custom message lengths
    -   [x] Data
-   [ ] Allow consumers to request batched events
-   [ ] Switch all `fs` write OPs to `BufWriter`
-   [ ] Allow consumers to rewind
-   [ ] Add a process to compact old logs
-   [ ] Distributed brokers/logs
-   [ ] Switch message UUIDs to time-ordered UUIDs
-   [ ] Index min & max timestamps for each events file
-   [ ] Allow observers to query events like a time series database
    -   [ ] ranges `SELECT * IN group FROM timestamp TO timestamp`
    -   [ ] time slices `SELECT * IN group FROM timestamp SAMPLE BY 5m`
-   [ ] Add the ability to create groups
-   [ ] Allow producers to push data into groups
-   [ ] Allow consumers to "subscribe" (assign) to a group
-   [ ] Come up with a cleaver catchy name for this project

## Technology

-   Producers: web browsers (JavaScript)
-   Broker(s): Rust
-   Consumners: NodeJS

## References

-   ["Apache Kafka and the Next 700 Stream Processing Systems" by Jay Kreps (video)](https://www.youtube.com/watch?v=9RMOc0SwRro)
-   [Martin Kleppmann — Event Sourcing and Stream Processing at Scale (video)](https://www.youtube.com/watch?v=avi-TZI9t2I)
-   [I Heart Logs (book)](https://www.oreilly.com/library/view/i-heart-logs/9781491909379/)
-   [Designing Data-Intensive Applications (book)](https://www.oreilly.com/library/view/designing-data-intensive-applications/9781491903063/)
