Marathon Event Producer for Apache Kafka
=======================================

This tool setups a HTTP endpoint for [Marathon Event Bus](https://mesosphere.github.io/marathon/docs/event-bus.html) and produces all incoming HTTP requests to a Kafka topic.

**Usage**:

`go run marathon_event_producer.go --broker.list localhost:9092 --topic marathon`

**Configuration parameters**:

`--broker.list` - Kafka broker list host:port to discover cluster. *This parameter is required*.

`--topic` - destination topic for all incoming messages. *This parameter is required*.

`--producer.config` - property file to configure embedded producer. *This parameter is optional*.

`--port` - HTTP endpoint binding port. *Defaults to `9000`*.

`--pattern` - HTTP endpoint url pattern to listen, e.g. `/marathon`. *Defaults to `/`*.

`--schema.registry` - URL to Confluent Schema Registry. Setting this triggers all events to be sent in Avro format. *Turned off by default*.

`--avsc` - Avro schema to use when producing Avro messages. *Defaults to `http_request.avsc`*

`--required.acks` - required acks for producer. `0` - no server response. `1` - the server will wait the data is written to the local log. `-1` - the server will block until the message is committed by all in sync replicas. More on this [here](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-ProduceRequest). *Defaults to `1`*.

`--acks.timeout` - provides a maximum time in milliseconds the server can await the receipt of the number of acknowledgements in `--required.acks`. *Defaults to 1000*.

`--log.level` - log level for built-in logger. Possible values are: `trace`, `debug`, `info`, `warn`, `error`, `critical`. *Defaults to `info`*.

`--max.procs` - maximum number of CPUs that can be executing simultaneously. *Defaults to `runtime.NumCPU()`*.