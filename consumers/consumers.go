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

package consumers

import (
	"time"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"net"
	"strconv"
	"github.com/stealthly/go-kafka/producer"
	kafkaClient "github.com/stealthly/go_kafka_client"
	metrics "github.com/rcrowley/go-metrics"
)

func resolveConfig() (*kafkaClient.ConsumerConfig, string, int, string, int) {
	rawConfig, _ := kafkaClient.LoadConfiguration("consumers.properties")
	numConsumers, _ := strconv.Atoi(rawConfig["NumConsumers"])
	zkTimeout, _ := time.ParseDuration(rawConfig["ZookeeperTimeout"])

	numWorkers, _ := strconv.Atoi(rawConfig["NumWorkers"])
	maxWorkerRetries, _ := strconv.Atoi(rawConfig["MaxWorkerRetries"])
	workerBackoff, _ := time.ParseDuration(rawConfig["WorkerBackoff"])
	workerRetryThreshold, _ := strconv.Atoi(rawConfig["WorkerRetryThreshold"])
	workerConsideredFailedTimeWindow, _ := time.ParseDuration(rawConfig["WorkerConsideredFailedTimeWindow"])
	workerBatchTimeout, _ := time.ParseDuration(rawConfig["WorkerBatchTimeout"])
	workerTaskTimeout, _ := time.ParseDuration(rawConfig["WorkerTaskTimeout"])
	workerManagersStopTimeout, _ := time.ParseDuration(rawConfig["WorkerManagersStopTimeout"])

	rebalanceMaxRetries, _ := strconv.Atoi(rawConfig["RebalanceMaxRetries"])
	rebalanceBackoff, _ := time.ParseDuration(rawConfig["RebalanceBackoff"])
	partitionAssignmentStrategy, _ := rawConfig["PartitionAssignmentStrategy"]
	excludeInternalTopics, _ := strconv.ParseBool(rawConfig["ExcludeInternalTopics"])

	numConsumerFetchers, _ := strconv.Atoi(rawConfig["NumConsumerFetchers"])
	fetchBatchSize, _ := strconv.Atoi(rawConfig["FetchBatchSize"])
	fetchMessageMaxBytes, _ := strconv.Atoi(rawConfig["FetchMessageMaxBytes"])
	fetchMinBytes, _ := strconv.Atoi(rawConfig["FetchMinBytes"])
	fetchBatchTimeout, _ := time.ParseDuration(rawConfig["FetchBatchTimeout"])
	requeueAskNextBackoff, _ := time.ParseDuration(rawConfig["RequeueAskNextBackoff"])
	fetchWaitMaxMs, _ := strconv.Atoi(rawConfig["FetchWaitMaxMs"])
	socketTimeout, _ := time.ParseDuration(rawConfig["SocketTimeout"])
	queuedMaxMessages, _ := strconv.Atoi(rawConfig["QueuedMaxMessages"])
	refreshLeaderBackoff, _ := time.ParseDuration(rawConfig["RefreshLeaderBackoff"])
	stepsAhead, _ := strconv.Atoi(rawConfig["StepsAhead"])

	offsetsCommitMaxRetries, _ := strconv.Atoi(rawConfig["OffsetsCommitMaxRetries"])

	flushInterval, _ := strconv.Atoi(rawConfig["FlushInterval"])
	return &kafkaClient.ConsumerConfig{
		ClientId: rawConfig["ClientId"],
		Groupid: rawConfig["GroupId"],
		ZookeeperConnect: []string{rawConfig["ZookeeperConnect"]},
		ZookeeperTimeout: zkTimeout,
		NumWorkers: numWorkers,
		MaxWorkerRetries: maxWorkerRetries,
		WorkerBackoff: workerBackoff,
		WorkerRetryThreshold: int32(workerRetryThreshold),
		WorkerConsideredFailedTimeWindow: workerConsideredFailedTimeWindow,
		WorkerBatchTimeout: workerBatchTimeout,
		WorkerTaskTimeout: workerTaskTimeout,
		WorkerManagersStopTimeout: workerManagersStopTimeout,
		RebalanceMaxRetries: int32(rebalanceMaxRetries),
		RebalanceBackoff: rebalanceBackoff,
		PartitionAssignmentStrategy: partitionAssignmentStrategy,
		ExcludeInternalTopics: excludeInternalTopics,
		NumConsumerFetchers: numConsumerFetchers,
		FetchBatchSize: fetchBatchSize,
		FetchMessageMaxBytes: int32(fetchMessageMaxBytes),
		FetchMinBytes: int32(fetchMinBytes),
		FetchBatchTimeout: fetchBatchTimeout,
		RequeueAskNextBackoff: requeueAskNextBackoff,
		FetchWaitMaxMs: int32(fetchWaitMaxMs),
		SocketTimeout: socketTimeout,
		QueuedMaxMessages: int32(queuedMaxMessages),
		RefreshLeaderBackoff: refreshLeaderBackoff,
		StepsAhead: stepsAhead,
		OffsetsStorage: rawConfig["OffsetsStorage"],
		AutoOffsetReset: rawConfig["AutoOffsetReset"],
		OffsetsCommitMaxRetries: int32(offsetsCommitMaxRetries),
	}, rawConfig["ConsumerIdPattern"], numConsumers, rawConfig["GraphiteConnect"], flushInterval
}

func main() {
	addr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:2003")
	if err != nil {
		panic(err)
	}

	go metrics.GraphiteWithConfig(metrics.GraphiteConfig{
	Addr:          addr,
	Registry:      metrics.DefaultRegistry,
	FlushInterval: 10e9,
	DurationUnit:  time.Second,
	Prefix:        "metrics",
	Percentiles:   []float64{0.5, 0.75, 0.95, 0.99, 0.999},
})

	topic := fmt.Sprintf("go-kafka-topic-%d", time.Now().Unix())
	numMessage := 0

	kafkaClient.CreateMultiplePartitionsTopic("192.168.86.5:2181", topic, 6)
	time.Sleep(4 * time.Second)

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

func startNewConsumer(topic string, consumerIndex int) *kafkaClient.Consumer {
	consumerId := fmt.Sprintf("consumer%d", consumerIndex)
	consumer := createConsumer(consumerId)
	topics := map[string]int {topic : 3}
	go func() {
		consumer.StartStatic(topics)
	}()
	return consumer
}

func createConsumer(consumerid string) *kafkaClient.Consumer {
	config := kafkaClient.DefaultConsumerConfig()
	config.ZookeeperConnect = []string{"192.168.86.5:2181"}
	config.ConsumerId = consumerid
	config.AutoOffsetReset = "smallest"
	config.FetchBatchSize = 20
	config.FetchBatchTimeout = 3*time.Second
	config.WorkerTaskTimeout = 10*time.Second
	config.Strategy = Strategy
	config.WorkerRetryThreshold = 100
	config.WorkerFailureCallback = FailedCallback
	config.WorkerFailedAttemptCallback = FailedAttemptCallback
	config.WorkerCloseTimeout = 1*time.Second

	consumer := kafkaClient.NewConsumer(config)
	return consumer
}

func Strategy(worker *kafkaClient.Worker, msg *kafkaClient.Message, id kafkaClient.TaskId) kafkaClient.WorkerResult {
	kafkaClient.Infof("main", "Got a message: %s", string(msg.Value))
	//	sleepTime := time.Duration(rand.Intn(2) + 1) * time.Second
	//	time.Sleep(sleepTime)

	return kafkaClient.NewSuccessfulResult(id)
}

func FailedCallback(wm *kafkaClient.WorkerManager) kafkaClient.FailedDecision {
	kafkaClient.Info("main", "Failed callback")

	return kafkaClient.DoNotCommitOffsetAndStop
}

func FailedAttemptCallback(task *kafkaClient.Task, result kafkaClient.WorkerResult) kafkaClient.FailedDecision {
	kafkaClient.Info("main", "Failed attempt")

	return kafkaClient.CommitOffsetAndContinue
}
