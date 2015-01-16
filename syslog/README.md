Syslog Producer for Go Kafka Client
==================================

This tool setups a Syslog server listening for syslog messages via TCP, parses them and produces into a Kafka topic.

**Usage**:

`go run syslog.go --producer.config producer.properties --topic syslog`

**Configuration parameters**:

`--producer.config` - property file to configure embedded producers. *This parameter is required*.

`--topic` - destination topic for all incoming syslog messages. *This parameter is required*.

`--tcp.port` - TCP port to listen for incoming syslog messages. *Defaults to 5140*.

`--tcp.host` - TCP host to listen for incoming syslog messages. *Defaults to 0.0.0.0*.

`--udp.port` - UDP port to listen for incoming syslog messages. *Defaults to 5141*.

`--udp.host` - UDP host to listen for incoming syslog messages. *Defaults to 0.0.0.0*.

`--num.producers` - number of producer instances. This can be used to increase throughput. *Defaults to 1*.

`--queue.size` - number of messages that are buffered for producing. *Defaults to 10000*.

`--log.level` - log level for built-in logger. Possible values are: `trace`, `debug`, `info`, `warn`, `error`, `critical`. *Defaults to info*.

`--max.procs` - maximum number of CPUs that can be executing simultaneously. *Defaults to runtime.NumCPU()*.

Running with Docker
==================

We provided a Docker image for quick deployment without cloning the repo, building the binary etc. Running it is as easy as following:

`docker run --net=host -v $(pwd):/syslog stealthly/syslog --topic syslog --producer.config=/syslog/producer.properties`

You may pass all configurations described above as arguments to the container.

Try it out using Vagrant
=======================

1. `cd $GOPATH/src/github.com/stealthly/go_kafka_client/syslog`
2. `vagrant up`

After this is done you will have a VM available at `192.168.66.66` with Zookeeper on port 2181, Kafka on port 9092, and Syslog Server listening on TCP port 5140 and UDP port 5141. To verify this is working you may do the following:

1. Start the console consumer: `$KAFKA_HOME/bin/kafka-console-consumer.sh --zookeeper 192.168.66.66:2181 --topic syslog --from-beginning`
2. Send some syslog messages via console: `echo "<34>1 2003-10-11T22:14:15.003Z localhost.stealth.ly su - ID23 - a simple message" > /dev/tcp/192.168.66.66/5140` to send via TCP or `echo "<34>1 2003-10-11T22:14:15.003Z localhost.stealth.ly su - ID23 - a simple message" > /dev/udp/192.168.66.66/5141` to send via UDP.

**Customizing server**:

You may want to produce to different topic or listen to other ports than 5140 and 5141 etc. All settings are kept in `syslog/vagrant/syslog.sh` file and can be modified before `vagrant up` to customize configurations.