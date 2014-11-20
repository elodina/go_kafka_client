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
	"github.com/stealthly/go-kafka/producer"
	"github.com/stealthly/go_kafka_client"
	"time"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"net"
	metrics "github.com/rcrowley/go-metrics"
)

func main() {
	addr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:2003")
	if err != nil {
		panic(err)
	}
	go metrics.Graphite(metrics.DefaultRegistry, 10e9, "metrics", addr)

	topic := fmt.Sprintf("go-kafka-topic-%d", time.Now().Unix())
	numMessage := 0

	go_kafka_client.CreateMultiplePartitionsTopic("192.168.86.5:2181", topic, 6)

	p := producer.NewKafkaProducer(topic, []string{"192.168.86.10:9092"}, nil)
	defer p.Close()
	go func() {
		for {
			if err := p.Send(fmt.Sprintf("message %d!", numMessage)); err != nil {
				panic(err)
			}
			numMessage++
			sleepTime := time.Duration(rand.Intn(50) + 1) * time.Millisecond
			time.Sleep(sleepTime)
		}
	}()

	ctrlc := make(chan os.Signal, 1)
	signal.Notify(ctrlc, os.Interrupt)
	consumer1 := startNewConsumer(topic, 1)
	time.Sleep(10 * time.Second)
	consumer2 := startNewConsumer(topic, 2)
	<-ctrlc
	fmt.Println("Shutdown triggered, closing all alive consumers")
	<-consumer1.Close()
	<-consumer2.Close()
	fmt.Println("Successfully shut down all consumers")
}

func startNewConsumer(topic string, consumerIndex int) *go_kafka_client.Consumer {
	consumerId := fmt.Sprintf("consumer%d", consumerIndex)
	consumer := createConsumer(consumerId)
	topics := map[string]int {topic : 3}
	go func() {
		consumer.StartStatic(topics)
	}()
	return consumer
}

func createConsumer(consumerid string) *go_kafka_client.Consumer{
	config := go_kafka_client.DefaultConsumerConfig()
	config.ZookeeperConnect = []string{"192.168.86.5:2181"}
	config.ConsumerId = consumerid
	config.AutoOffsetReset = "smallest"
	config.FetchBatchSize = 20
	config.FetchBatchTimeout = 3 * time.Second
	config.WorkerTaskTimeout = 10 * time.Second
	config.Strategy = Strategy
	config.WorkerRetryThreshold = 100
	config.WorkerFailureCallback = FailedCallback
	config.WorkerFailedAttemptCallback = FailedAttemptCallback
	config.WorkerCloseTimeout = 1 * time.Second

	consumer := go_kafka_client.NewConsumer(config)
	return consumer
}

func Strategy(worker *go_kafka_client.Worker, msg *go_kafka_client.Message, id go_kafka_client.TaskId) go_kafka_client.WorkerResult {
	go_kafka_client.Infof("main", "Got a message: %s", string(msg.Value))
//	sleepTime := time.Duration(rand.Intn(2) + 1) * time.Second
//	time.Sleep(sleepTime)

	return go_kafka_client.NewSuccessfulResult(id)
}

func FailedCallback(wm *go_kafka_client.WorkerManager) go_kafka_client.FailedDecision {
	go_kafka_client.Info("main", "Failed callback")

	return go_kafka_client.DoNotCommitOffsetAndStop
}

func FailedAttemptCallback(task *go_kafka_client.Task, result go_kafka_client.WorkerResult) go_kafka_client.FailedDecision {
	go_kafka_client.Info("main", "Failed attempt")

	return go_kafka_client.CommitOffsetAndContinue
}
