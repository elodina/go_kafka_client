/* Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. */

package main

import (
    "flag"
    "os"
    "fmt"
    kafka "github.com/stealthly/go_kafka_client"
    "github.com/stealthly/go-avro"
    "strings"
    "os/signal"
    "time"
    "github.com/golang/protobuf/proto"
    sp "github.com/stealthly/go_kafka_client/syslog/syslog_proto"
)

var brokerList = flag.String("broker.list", "", "Broker List to produce messages too.")
var zookeeper = flag.String("zookeeper", "", "Zookeeper urls for consumer to use.")
var schemaRegistry = flag.String("schema.registry", "", "Confluent schema registry url")
var consumeTopic = flag.String("consume.topic", "", "Topic to consume timings from.")
var produceTopic = flag.String("produce.topic", "", "Topic to produce timings to.")
var siesta = flag.Bool("siesta", false, "Use siesta client.")

var protobuf = true

var producer kafka.Producer

func main() {
    parseAndValidateArgs()
    ctrlc := make(chan os.Signal, 1)
    signal.Notify(ctrlc, os.Interrupt)

    producerConfig := kafka.DefaultProducerConfig()
    producerConfig.BrokerList = strings.Split(*brokerList, ",")

    zkConfig := kafka.NewZookeeperConfig()
    zkConfig.ZookeeperConnect = strings.Split(*zookeeper, ",")
    coordinator := kafka.NewZookeeperCoordinator(zkConfig)

    config := kafka.DefaultConsumerConfig()
    config.Debug = true
    config.Groupid = "perf-mirror"
    config.AutoOffsetReset = "smallest"
    config.Coordinator = coordinator
    config.WorkerFailedAttemptCallback = FailedAttemptCallback
    config.WorkerFailureCallback = FailedCallback
    if *siesta {
        config.LowLevelClient = kafka.NewSiestaClient(config)
    }

    if protobuf {
        setupProtoConfig(config)
    } else {
        producerConfig.ValueEncoder = kafka.NewKafkaAvroEncoder(*schemaRegistry)
        setupAvroConfig(config)
    }

    producer = kafka.NewSaramaProducer(producerConfig)
    consumer := kafka.NewConsumer(config)

    go consumer.StartStatic(map[string]int {*consumeTopic : 1})

    <-ctrlc
    fmt.Println("Shutdown triggered, closing consumer")
    <-consumer.Close()
    producer.Close()
}

func parseAndValidateArgs() {
    flag.Parse()
    if *brokerList == "" {
        fmt.Println("Broker list is required")
        os.Exit(1)
    }

    if *zookeeper == "" {
        fmt.Println("Zookeeper connection string is required")
        os.Exit(1)
    }

    if *consumeTopic == "" {
        fmt.Println("Consume topic is required")
        os.Exit(1)
    }

    if *produceTopic == "" {
        fmt.Println("Produce topic is required")
        os.Exit(1)
    }

    if *schemaRegistry != "" {
        protobuf = false
    }
}

func setupAvroConfig(config *kafka.ConsumerConfig) {
    config.ValueDecoder = kafka.NewKafkaAvroDecoder(*schemaRegistry)
    config.Strategy = avroStrategy
}

func setupProtoConfig(config *kafka.ConsumerConfig) {
    config.Strategy = logLineProtoStrategy
}

func avroStrategy(_ *kafka.Worker, msg *kafka.Message, id kafka.TaskId) kafka.WorkerResult {
    record := msg.DecodedValue.(*avro.GenericRecord)

    messageTimings := record.Get("timings").([]interface{})
    for _, timing := range msg.DecodedKey.([]int64) {
        messageTimings = append(messageTimings, timing)
    }
    messageTimings = append(messageTimings, time.Now().UnixNano()/int64(time.Millisecond))
    record.Set("timings", messageTimings)

    producer.Input() <- &kafka.ProducerMessage{Topic: *produceTopic, Value: record}

    return kafka.NewSuccessfulResult(id)
}

func logLineProtoStrategy(_ *kafka.Worker, msg *kafka.Message, id kafka.TaskId) kafka.WorkerResult {
    line := &sp.LogLine{}
    proto.Unmarshal(msg.Value, line)
    line.Timings = append(line.Timings, msg.DecodedKey.([]int64)...)
    line.Timings = append(line.Timings, time.Now().UnixNano()/int64(time.Millisecond))

    bytes, err := proto.Marshal(line)
    if err != nil {
        panic(err)
    }

    producer.Input() <- &kafka.ProducerMessage{Topic: *produceTopic, Value: bytes}

    return kafka.NewSuccessfulResult(id)
}

func FailedCallback(wm *kafka.WorkerManager) kafka.FailedDecision {
    kafka.Info("main", "Failed callback")

    return kafka.DoNotCommitOffsetAndStop
}

func FailedAttemptCallback(task *kafka.Task, result kafka.WorkerResult) kafka.FailedDecision {
    kafka.Info("main", "Failed attempt")

    return kafka.CommitOffsetAndContinue
}