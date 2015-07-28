End-to-end latency
=================

**Usage:**

**LogLine protobuf**

1. From `producer` folder:
```
$ go run producer.go --broker.list localhost:9092 --topic1 step1 --topic2 step2 --msg.per.sec 2000
```

2. From `mirror` folder:
```
$ go run mirror.go --zookeeper localhost:2181 --broker.list localhost:9092 --consume.topic step2 --produce.topic step3
```

3. From `consumer` folder:
```
$ go run consumer.go --zookeeper localhost:2181 --topic step3
```

**Avro**

1. From `producer` folder:
```
$ go run producer.go --broker.list localhost:9092 --topic1 step1 --topic2 step2 --msg.per.sec 2000 --schema.registry http://localhost:8081 
```

2. From `mirror` folder:
```
$ go run mirror.go --zookeeper localhost:2181 --broker.list localhost:9092 --consume.topic step2 --produce.topic step3 --schema.registry http://localhost:8081
```

3. From `consumer` folder:
```
$ go run consumer.go --zookeeper localhost:2181 --topic step3 --schema.registry http://localhost:8081
```

**Full flag list**:

**producer**:

`--broker.list` - Broker List to produce messages too.

`--topic1` - Step 1 topic to produce to.

`--topic2` - Step 2 topic to produce to after getting an ack from Step 1 topic.

`--msg.per.sec` - Messages per second to send.

`--schema.registry` - Confluent schema registry url. This triggers all messages to be sent as Avro.

`--avsc` - Avro schema to use when in Avro mode.

**mirror**:

`--broker.list` - Broker List to produce messages too.

`--zookeeper` - Zookeeper urls for consumer to use.

`--consume.topic` - Step 2 topic to consume timings from.

`--produce.topic` - Step 3 topic to produce timings to.

`--siesta` - Use Siesta client for consumer. Defaults to false.

`--schema.registry` - Confluent schema registry url. This must match the same setting in `producer`.

**consumer**:

`--zookeeper` - Zookeeper urls for consumer to use.

`--topic` - Step 3 topic to consume timings from.

`--siesta` - Use Siesta client for consumer. Defaults to false.

`--schema.registry` - Confluent schema registry url. This must match the same setting in `producer`.