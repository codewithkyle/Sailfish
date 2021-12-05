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
-   [ ] Setup consumers as Web Sockets
-   [x] Add consumer status endpoint
-   [ ] Add new event push logic to sleepy consumers

## Technology

-   Producers: web browsers (JavaScript)
-   Broker(s): Rust
-   Consumners: NodeJS

## References

-   ["Apache Kafka and the Next 700 Stream Processing Systems" by Jay Kreps (video)](https://www.youtube.com/watch?v=9RMOc0SwRro)
-   [Martin Kleppmann — Event Sourcing and Stream Processing at Scale (video)](https://www.youtube.com/watch?v=avi-TZI9t2I)
-   [I Heart Logs (book)](https://www.oreilly.com/library/view/i-heart-logs/9781491909379/)
-   [Designing Data-Intensive Applications (book)](https://www.oreilly.com/library/view/designing-data-intensive-applications/9781491903063/)
