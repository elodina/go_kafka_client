/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
    uuid "github.com/satori/go.uuid"
    "fmt"
    "github.com/Shopify/sarama"
    metrics "github.com/rcrowley/go-metrics"
    kafkaClient "github.com/stealthly/go_kafka_client"
    "net"
    "os"
    "os/signal"
    "runtime"
    "strconv"
    "time"
)

func resolveConfig() (string, string, time.Duration, string, time.Duration, int, time.Duration, int, int) {
    rawConfig, err := kafkaClient.LoadConfiguration("producers.properties")
    if err != nil {
        panic(err)
    }

    //zkConnect := rawConfig["zookeeper_connect"]
    brokerConnect := rawConfig["broker_connect"]
    topic := rawConfig["topic"]
    //numPartitions, _ := strconv.Atoi(rawConfig["num_partitions"])
    sleepTime, _ := time.ParseDuration(rawConfig["sleep_time"])
    flushInterval, _ := time.ParseDuration(rawConfig["flush_interval"])

    flushMsgCount, _ := strconv.Atoi(rawConfig["flush_msg_count"])
    flushFrequency, _ := time.ParseDuration(rawConfig["flush_frequency"])
    producerCount, _ := strconv.Atoi(rawConfig["producer_count"])
    maxMessagesPerReq, _ := strconv.Atoi(rawConfig["max_messages_per_req"])

    return brokerConnect, topic, sleepTime, rawConfig["graphite_connect"], flushInterval, flushMsgCount, flushFrequency, producerCount, maxMessagesPerReq
}

func startMetrics(graphiteConnect string, graphiteFlushInterval time.Duration) {
    addr, err := net.ResolveTCPAddr("tcp", graphiteConnect)
    if err != nil {
        panic(err)
    }
    go metrics.GraphiteWithConfig(metrics.GraphiteConfig{
        Addr:          addr,
        Registry:      metrics.DefaultRegistry,
        FlushInterval: graphiteFlushInterval,
        DurationUnit:  time.Second,
        Prefix:        "metrics",
        Percentiles:   []float64{0.5, 0.75, 0.95, 0.99, 0.999},
    })
}

func main() {
    fmt.Println(("Starting Producer"))
    runtime.GOMAXPROCS(runtime.NumCPU())
    numMessage := 0

    brokerConnect, topic, sleepTime, graphiteConnect, graphiteFlushInterval, flushMsgCount, flushFrequency, producerCount, maxMessagesPerReq := resolveConfig()

    _ = graphiteConnect
    _ = graphiteFlushInterval
    startMetrics(graphiteConnect, graphiteFlushInterval)
    produceRate := metrics.NewRegisteredMeter("ProduceRate", metrics.DefaultRegistry)

    //kafkaClient.CreateMultiplePartitionsTopic(zkConnect, topic, numPartitions)

    //p := producer.NewKafkaProducer(topic, []string{brokerConnect})

    //defer producer.Close()
    //defer p.Close()

    saramaError := make(chan *sarama.ProducerError)
    saramaSuccess := make(chan *sarama.ProducerMessage)

    clientConfig := sarama.NewConfig()
    clientConfig.ClientID = uuid.NewV1().String()
    clientConfig.Producer.Flush.Messages = flushMsgCount
    clientConfig.Producer.Flush.Frequency = flushFrequency
    clientConfig.Producer.Flush.MaxMessages = maxMessagesPerReq
    clientConfig.Producer.Return.Successes = true
    clientConfig.Producer.RequiredAcks = sarama.NoResponse //WaitForAll
    clientConfig.Producer.Timeout = 1000 * time.Millisecond
    client, err := sarama.NewClient([]string{brokerConnect}, clientConfig)
    for i := 0; i < producerCount; i++ {
        if err != nil {
            panic(err)
        }
        
        //		config.Compression = 2
        producer, err := sarama.NewAsyncProducerFromClient(client)
        go func() {
            if err != nil {
                panic(err)
            }
            for {
                message := &sarama.ProducerMessage{Topic: topic, Key: sarama.StringEncoder(fmt.Sprintf("%d", numMessage)), Value: sarama.StringEncoder(fmt.Sprintf("message %d!", numMessage))}
                numMessage++
                producer.Input() <- message
                time.Sleep(sleepTime)
            }
        }()

        go func() {
            for {
                select {
                case error := <-producer.Errors():
                    saramaError <- error
                case success := <-producer.Successes():
                    saramaSuccess <- success
                }
            }
        }()
    }

    ctrlc := make(chan os.Signal, 1)
    signal.Notify(ctrlc, os.Interrupt)
    go func() {
        start := time.Now()
        count := 0
        for {
            select {
            case err := <-saramaError:
                fmt.Println(err)
            case <-saramaSuccess:
                produceRate.Mark(1)
                count++
                elapsed := time.Since(start)
                if elapsed.Seconds() >= 1 {
                    fmt.Println(fmt.Sprintf("Per Second %d", count))
                    count = 0
                    start = time.Now()
                }
            }
        }
    }()
    <-ctrlc
}
