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
	"os"
	"os/signal"
	"net"
	"strconv"
	kafkaClient "github.com/stealthly/go_kafka_client"
	metrics "github.com/rcrowley/go-metrics"
)

func resolveConfig() (*kafkaClient.ConsumerConfig, string, string, int, string, time.Duration) {
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

	flushInterval, _ := time.ParseDuration(rawConfig["FlushInterval"])
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
	}, rawConfig["ConsumerIdPattern"], rawConfig["Topic"], numConsumers, rawConfig["GraphiteConnect"], flushInterval
}

func main() {
	config, consumerIdPattern, topic, numConsumers, graphiteConnect, graphiteFlushInterval := resolveConfig()
	startMetrics(graphiteConnect, graphiteFlushInterval)

	ctrlc := make(chan os.Signal, 1)
	signal.Notify(ctrlc, os.Interrupt)

	consumers := make([]*kafkaClient.Consumer, numConsumers)
	for i := 0; i < numConsumers; i++ {
		consumers[i] = startNewConsumer(config, topic, consumerIdPattern, i)
		time.Sleep(5 * time.Second)
	}

	<-ctrlc
	fmt.Println("Shutdown triggered, closing all alive consumers")
	for _, consumer := range consumers {
		<-consumer.Close()
	}
	fmt.Println("Successfully shut down all consumers")
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

func startNewConsumer(config *kafkaClient.ConsumerConfig, topic string, consumerIdPattern string, consumerIndex int) *kafkaClient.Consumer {
	config.ConsumerId = fmt.Sprintf(consumerIdPattern, consumerIndex)
	config.Strategy = Strategy
	config.WorkerFailureCallback = FailedCallback
	config.WorkerFailedAttemptCallback = FailedAttemptCallback
	consumer := kafkaClient.NewConsumer(config)
	topics := map[string]int {topic : config.NumConsumerFetchers}
	go func() {
		consumer.StartStatic(topics)
	}()
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
