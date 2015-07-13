Offset storage
=============

go_kafka_client now exposes the OffsetStorage interface to be flexible about managing your offset.

go_kafka_client now also supports [Kafka offset storage](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetAPI) with [Siesta](https://github.com/stealthly/siesta) library. To take advantage of this feature you should include the following to your ConsumerConfig:

```
config := DefaultConsumerConfig()
// your configurations go here
config.OffsetStorage = NewSiestaClient(config)
```

The default offset storage for now is still Zookeeper though and needs no additional configuration to get it working.