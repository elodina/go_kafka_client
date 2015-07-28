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
	"fmt"
	metrics "github.com/rcrowley/go-metrics"
	kafkaClient "github.com/stealthly/go_kafka_client"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"
)

func resolveConfig() (*kafkaClient.ConsumerConfig, string, int, string, time.Duration) {
	rawConfig, err := kafkaClient.LoadConfiguration("consumers.properties")
	if err != nil {
		panic("Failed to load configuration file")
	}
	logLevel := rawConfig["log_level"]
	setLogLevel(logLevel)
	numConsumers, _ := strconv.Atoi(rawConfig["num_consumers"])
	zkTimeout, _ := time.ParseDuration(rawConfig["zookeeper_timeout"])

	numWorkers, _ := strconv.Atoi(rawConfig["num_workers"])
	maxWorkerRetries, _ := strconv.Atoi(rawConfig["max_worker_retries"])
	workerBackoff, _ := time.ParseDuration(rawConfig["worker_backoff"])
	workerRetryThreshold, _ := strconv.Atoi(rawConfig["worker_retry_threshold"])
	workerConsideredFailedTimeWindow, _ := time.ParseDuration(rawConfig["worker_considered_failed_time_window"])
	workerTaskTimeout, _ := time.ParseDuration(rawConfig["worker_task_timeout"])
	workerManagersStopTimeout, _ := time.ParseDuration(rawConfig["worker_managers_stop_timeout"])

	rebalanceBarrierTimeout, _ := time.ParseDuration(rawConfig["rebalance_barrier_timeout"])
	rebalanceMaxRetries, _ := strconv.Atoi(rawConfig["rebalance_max_retries"])
	rebalanceBackoff, _ := time.ParseDuration(rawConfig["rebalance_backoff"])
	partitionAssignmentStrategy, _ := rawConfig["partition_assignment_strategy"]
	excludeInternalTopics, _ := strconv.ParseBool(rawConfig["exclude_internal_topics"])

	numConsumerFetchers, _ := strconv.Atoi(rawConfig["num_consumer_fetchers"])
	fetchBatchSize, _ := strconv.Atoi(rawConfig["fetch_batch_size"])
	fetchMessageMaxBytes, _ := strconv.Atoi(rawConfig["fetch_message_max_bytes"])
	fetchMinBytes, _ := strconv.Atoi(rawConfig["fetch_min_bytes"])
	fetchBatchTimeout, _ := time.ParseDuration(rawConfig["fetch_batch_timeout"])
	requeueAskNextBackoff, _ := time.ParseDuration(rawConfig["requeue_ask_next_backoff"])
	fetchWaitMaxMs, _ := strconv.Atoi(rawConfig["fetch_wait_max_ms"])
	socketTimeout, _ := time.ParseDuration(rawConfig["socket_timeout"])
	queuedMaxMessages, _ := strconv.Atoi(rawConfig["queued_max_messages"])
	refreshLeaderBackoff, _ := time.ParseDuration(rawConfig["refresh_leader_backoff"])
	fetchMetadataRetries, _ := strconv.Atoi(rawConfig["fetch_metadata_retries"])
	fetchMetadataBackoff, _ := time.ParseDuration(rawConfig["fetch_metadata_backoff"])

	time.ParseDuration(rawConfig["fetch_metadata_backoff"])

	offsetsCommitMaxRetries, _ := strconv.Atoi(rawConfig["offsets_commit_max_retries"])

	flushInterval, _ := time.ParseDuration(rawConfig["flush_interval"])
	deploymentTimeout, _ := time.ParseDuration(rawConfig["deployment_timeout"])

	zkConfig := kafkaClient.NewZookeeperConfig()
	zkConfig.ZookeeperConnect = []string{rawConfig["zookeeper_connect"]}
	zkConfig.ZookeeperTimeout = zkTimeout

	config := kafkaClient.DefaultConsumerConfig()
	config.Groupid = rawConfig["group_id"]
	config.NumWorkers = numWorkers
	config.MaxWorkerRetries = maxWorkerRetries
	config.WorkerBackoff = workerBackoff
	config.WorkerRetryThreshold = int32(workerRetryThreshold)
	config.WorkerThresholdTimeWindow = workerConsideredFailedTimeWindow
	config.WorkerTaskTimeout = workerTaskTimeout
	config.WorkerManagersStopTimeout = workerManagersStopTimeout
	config.BarrierTimeout = rebalanceBarrierTimeout
	config.RebalanceMaxRetries = int32(rebalanceMaxRetries)
	config.RebalanceBackoff = rebalanceBackoff
	config.PartitionAssignmentStrategy = partitionAssignmentStrategy
	config.ExcludeInternalTopics = excludeInternalTopics
	config.NumConsumerFetchers = numConsumerFetchers
	config.FetchBatchSize = fetchBatchSize
	config.FetchMessageMaxBytes = int32(fetchMessageMaxBytes)
	config.FetchMinBytes = int32(fetchMinBytes)
	config.FetchBatchTimeout = fetchBatchTimeout
	config.FetchTopicMetadataRetries = fetchMetadataRetries
	config.FetchTopicMetadataBackoff = fetchMetadataBackoff
	config.RequeueAskNextBackoff = requeueAskNextBackoff
	config.FetchWaitMaxMs = int32(fetchWaitMaxMs)
	config.SocketTimeout = socketTimeout
	config.QueuedMaxMessages = int32(queuedMaxMessages)
	config.RefreshLeaderBackoff = refreshLeaderBackoff
	config.Coordinator = kafkaClient.NewZookeeperCoordinator(zkConfig)
	config.AutoOffsetReset = rawConfig["auto_offset_reset"]
	config.OffsetsCommitMaxRetries = offsetsCommitMaxRetries
	config.DeploymentTimeout = deploymentTimeout
	config.OffsetCommitInterval = 10 * time.Second

	return config, rawConfig["topic"], numConsumers, rawConfig["graphite_connect"], flushInterval
}

func setLogLevel(logLevel string) {
	var level kafkaClient.LogLevel
	switch strings.ToLower(logLevel) {
		case "trace":
		level = kafkaClient.TraceLevel
		case "debug":
		level = kafkaClient.DebugLevel
		case "info":
		level = kafkaClient.InfoLevel
		case "warn":
		level = kafkaClient.WarnLevel
		case "error":
		level = kafkaClient.ErrorLevel
		case "critical":
		level = kafkaClient.CriticalLevel
		default:
		level = kafkaClient.InfoLevel
	}
	kafkaClient.Logger = kafkaClient.NewDefaultLogger(level)
}

func main() {
	config, topic, numConsumers, graphiteConnect, graphiteFlushInterval := resolveConfig()
	if graphiteConnect != "" {
		startMetrics(graphiteConnect, graphiteFlushInterval)
	}

	ctrlc := make(chan os.Signal, 1)
	signal.Notify(ctrlc, os.Interrupt)

	consumers := make([]*kafkaClient.Consumer, numConsumers)
	for i := 0; i < numConsumers; i++ {
		consumers[i] = startNewConsumer(*config, topic)
		time.Sleep(10 * time.Second)
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

func startNewConsumer(config kafkaClient.ConsumerConfig, topic string) *kafkaClient.Consumer {
	config.Strategy = GetStrategy(config.Consumerid)
	config.WorkerFailureCallback = FailedCallback
	config.WorkerFailedAttemptCallback = FailedAttemptCallback
	consumer := kafkaClient.NewConsumer(&config)
	topics := map[string]int{topic: config.NumConsumerFetchers}
	go func() {
		consumer.StartStatic(topics)
	}()
	return consumer
}

func GetStrategy(consumerId string) func(*kafkaClient.Worker, *kafkaClient.Message, kafkaClient.TaskId) kafkaClient.WorkerResult {
	consumeRate := metrics.NewRegisteredMeter(fmt.Sprintf("%s-ConsumeRate", consumerId), metrics.DefaultRegistry)
	return func(_ *kafkaClient.Worker, msg *kafkaClient.Message, id kafkaClient.TaskId) kafkaClient.WorkerResult {
		kafkaClient.Tracef("main", "Got a message: %s", string(msg.Value))
		consumeRate.Mark(1)

		return kafkaClient.NewSuccessfulResult(id)
	}
}

func FailedCallback(wm *kafkaClient.WorkerManager) kafkaClient.FailedDecision {
	kafkaClient.Info("main", "Failed callback")

	return kafkaClient.DoNotCommitOffsetAndStop
}

func FailedAttemptCallback(task *kafkaClient.Task, result kafkaClient.WorkerResult) kafkaClient.FailedDecision {
	kafkaClient.Info("main", "Failed attempt")

	return kafkaClient.CommitOffsetAndContinue
}