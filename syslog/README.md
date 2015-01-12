Syslog Producer for Go Kafka Client
==================================

This tool setups a Syslog server listening for syslog messages via TCP, parses them and produces into a Kafka topic. Messages can be in RFC 5424 or RFC 3164 format.

**Usage**:

`go run syslog.go --producer.config producer.properties --topic syslog`

**Configuration parameters**:

`--producer.config` - property file to configure embedded producers. *This parameter is required*.

`--topic` - destination topic for all incoming syslog messages. *This parameter is required*.

`--format` - format of incoming syslog messages. Possible values are `rfc5424` and `rfc3164`. *Defaults to rfc5424*.

`--tcp.port` - TCP port to listen for incoming syslog messages. *Defaults to 5140*.

`--tcp.host` - TCP host to listen for incoming syslog messages. *Defaults to 0.0.0.0*.

`--num.producers` - number of producer instances. This can be used to increase throughput. *Defaults to 1*.

`--queue.size` - number of messages that are buffered for producing. *Defaults to 10000*.

`--log.level` - log level for built-in logger. Possible values are: `trace`, `debug`, `info`, `warn`, `error`, `critical`. *Defaults to info*.

`--max.procs` - maximum number of CPUs that can be executing simultaneously. *Defaults to runtime.NumCPU()*.