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
    "github.com/rcrowley/go-metrics"
)

var zookeeper = flag.String("zookeeper", "", "Zookeeper urls for consumer to use.")
var schemaRegistry = flag.String("schema.registry", "", "Confluent schema registry url")
var topic = flag.String("topic", "", "Topic to consume timings from.")
var siesta = flag.Bool("siesta", false, "Use siesta client.")

var protobuf = true

var timings = make(chan []int64, 100000)

func main() {
    parseAndValidateArgs()
    ctrlc := make(chan os.Signal, 1)
    signal.Notify(ctrlc, os.Interrupt)

    zkConfig := kafka.NewZookeeperConfig()
    zkConfig.ZookeeperConnect = strings.Split(*zookeeper, ",")
    coordinator := kafka.NewZookeeperCoordinator(zkConfig)

    config := kafka.DefaultConsumerConfig()
    config.Groupid = "perf-consumer"
    config.AutoOffsetReset = "smallest"
    config.Coordinator = coordinator
    config.WorkerFailedAttemptCallback = FailedAttemptCallback
    config.WorkerFailureCallback = FailedCallback
    if *siesta {
        config.LowLevelClient = kafka.NewSiestaClient(config)
    }

    if protobuf {
        setupLogLineProtoConfig(config)
    } else {
        setupAvroConfig(config)
    }

    consumer := kafka.NewConsumer(config)

    go consumer.StartStatic(map[string]int {*topic : 2})

    go func() {
        latencies := make([]metrics.Histogram, 0)
        endToEnd := metrics.NewRegisteredHistogram(fmt.Sprint("Latency-end-to-end"), metrics.DefaultRegistry, metrics.NewUniformSample(10000))
        go func() {
            for {
                time.Sleep(1 * time.Second)
                for i, meter := range latencies {
                    fmt.Printf("Step %d: %f\n", i+1, meter.Mean())
                }
                fmt.Printf("End-to-end: %f\n", endToEnd.Mean())
                fmt.Println()
            }
        }()

        initialized := false
        for timing := range timings {
            if !initialized {
                for i := 1; i < len(timing); i++ {
                    latencies = append(latencies, metrics.NewRegisteredHistogram(fmt.Sprintf("Latency-step-%d", i), metrics.DefaultRegistry, metrics.NewUniformSample(10000)))
                }
                initialized = true
            }

            if len(timing) - 1 != len(latencies) {
                fmt.Println("Got wrong latencies, skipping..")
                continue
            }

            for i := 1; i < len(timing); i++ {
                latencies[i-1].Update(int64(timing[i] - timing[i-1]))
            }
            endToEnd.Update(int64(timing[len(timing)-1] - timing[0]))
        }
    }()

    <-ctrlc
    fmt.Println("Shutdown triggered, closing consumer")
    <-consumer.Close()
    close(timings)
}

func parseAndValidateArgs() {
    flag.Parse()
    if *zookeeper == "" {
        fmt.Println("Zookeeper connection string is required")
        os.Exit(1)
    }

    if *topic == "" {
        fmt.Println("Topic is required")
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

func setupLogLineProtoConfig(config *kafka.ConsumerConfig) {
    config.Strategy = logLineProtoStrategy
}

func avroStrategy(_ *kafka.Worker, msg *kafka.Message, id kafka.TaskId) kafka.WorkerResult {
    record := msg.DecodedValue.(*avro.GenericRecord)

    newTimings := make([]int64, 0)
    for _, timing := range record.Get("timings").([]interface{}) {
        newTimings = append(newTimings, timing.(int64))
    }
    timings <- newTimings

    return kafka.NewSuccessfulResult(id)
}

func logLineProtoStrategy(_ *kafka.Worker, msg *kafka.Message, id kafka.TaskId) kafka.WorkerResult {
    line := &sp.LogLine{}
    proto.Unmarshal(msg.Value, line)
    timings <- line.Timings

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