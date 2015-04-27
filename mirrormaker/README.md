Mirror Maker for Go Kafka Client
================================

Kafka's mirroring feature makes it possible to maintain a replica of an existing Kafka cluster.
The tool uses a Kafka consumer to consume messages from the source cluster, and re-publishes those messages to the target cluster.

**Usage**:

`go run mirror_maker.go --consumer.config sourceCluster1Consumer.config --consumer.config sourceCluster2Consumer.config --num.streams 2 --producer.config targetClusterProducer.config --whitelist=".*"`

**Configuration parameters**:

`--whitelist`, `--blacklist` - whitelist or blacklist of topics to mirror. Exactly one whitelist or blacklist is allowed, e.g. passing both whitelist and blacklist will cause panic. *This parameter is required*.

`--consumer.config` - consumer property files to consume from a source cluster. You can pass multiple of those like this: `--consumer.config sourceCluster1Consumer.config --consumer.config sourceCluster2Consumer.config`. *At least one consumer config is required*.

`--producer.config` - property file to configure embedded producers. *This parameter is required*.

`--num.producers` - number of producer instances. This can be used to increase throughput. This helps because each producer's requests are effectively handled by a single thread on the receiving Kafka broker. i.e., even if you have multiple consumption streams (see next section), the throughput can be bottle-necked at handling stage of the mirror maker's producer request. *Defaults to 1*.

`--num.streams` - used to specify the number of mirror consumer goroutines to create. If the number of consumption streams is higher than number of available partitions then some of the mirroring routines will be idle by virtue of the consumer rebalancing algorithm (if they do not end up owning any partitions for consumption). *Defaults to 1*.

`--preserve.partitions` - flag to preserve partition number. E.g. if message was read from partition 5 it'll be written to partition 5. Note that this can affect performance. *Defaults to false*.

`--preserve.order` - flag to preserve message order. E.g. message sequence 1, 2, 3, 4, 5 will remain 1, 2, 3, 4, 5 in destination topic. Note that this can affect performance. *Defaults to false*.

`--prefix` - destination topic prefix. E.g. if message was read from topic "test" and prefix is "dc1_" it'll be written to topic "dc1_test". *Defaults to empty string*.

`--queue.size` - number of messages that are buffered between the consumer and producer. *Defaults to 10000*.

`--max.procs` - maximum number of CPUs that can be executing simultaneously. *Defaults to runtime.NumCPU()*.  

`--timings.producer.config` - property file to configure embedded timings producer.  

`--schema.registry.url` - Avro schema registry URL for requesting avro schemas.
 
 **Docker usage:**
 
 1. Install Docker [https://docs.docker.com/installation/#installation](https://docs.docker.com/installation/#installation)
 2. `cd $GOPATH/src/github.com/stealthly/go_kafka_client`
 3. Build docker image: `docker build -t stealthly/go_kafka_mirrormaker --file Dockerfile.mirrormaker .`
 4. `docker run -v $(pwd)/mirrormaker:/mirrormaker stealthly/go_kafka_mirrormaker --consumer.config /mirrormaker/sourceCluster1Consumer.config --consumer.config /mirrormaker/sourceCluster2Consumer.config --num.streams 2 --producer.config /mirrormaker/targetClusterProducer.config --whitelist=".*"`
