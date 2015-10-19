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

package go_kafka_client

import (
	"errors"
	"fmt"
	"time"
)

//ConsumerConfig defines configuration options for Consumer
type ConsumerConfig struct {
	/* A string that uniquely identifies a set of consumers within the same consumer group */
	Groupid string

	/* A string that uniquely identifies a consumer within a group. Generated automatically if not set.
	Set this explicitly for only testing purpose. */
	Consumerid string

	/* The socket timeout for network requests. Its value should be at least FetchWaitMaxMs. */
	SocketTimeout time.Duration

	/* The maximum number of bytes to attempt to fetch */
	FetchMessageMaxBytes int32

	/* The number of goroutines used to fetch data */
	NumConsumerFetchers int

	/* Max number of message batches buffered for consumption, each batch can be up to FetchBatchSize */
	QueuedMaxMessages int32

	/* Max number of retries during rebalance */
	RebalanceMaxRetries int32

	/* The minimum amount of data the server should return for a fetch request. If insufficient data is available the request will block */
	FetchMinBytes int32

	/* The maximum amount of time the server will block before answering the fetch request if there isn't sufficient data to immediately satisfy FetchMinBytes */
	FetchWaitMaxMs int32

	/* Backoff time between retries during rebalance */
	RebalanceBackoff time.Duration

	/* Backoff time to refresh the leader of a partition after it loses the current leader */
	RefreshLeaderBackoff time.Duration

	/* Retry the offset commit up to this many times on failure. */
	OffsetsCommitMaxRetries int

	/* Try to commit offset every OffsetCommitInterval. If previous offset commit for a partition is still in progress updates the next offset to commit and continues.
	This way it does not commit all the offset history if the coordinator is slow, but only the highest offsets. */
	OffsetCommitInterval time.Duration

	/* What to do if an offset is out of range.
	SmallestOffset : automatically reset the offset to the smallest offset.
	LargestOffset : automatically reset the offset to the largest offset.
	Defaults to LargestOffset. */
	AutoOffsetReset string

	/* Client id is specified by the kafka consumer client, used to distinguish different clients. */
	Clientid string

	/* Whether messages from internal topics (such as offsets) should be exposed to the consumer. */
	ExcludeInternalTopics bool

	/* Select a strategy for assigning partitions to consumer streams. Possible values: RangeStrategy, RoundRobinStrategy */
	PartitionAssignmentStrategy string

	/* Amount of workers per partition to process consumed messages. */
	NumWorkers int

	/* Times to retry processing a failed message by a worker. */
	MaxWorkerRetries int

	/* Worker retry threshold. Increments each time a worker fails to process a message within MaxWorkerRetries.
	When this threshold is hit within a WorkerThresholdTimeWindow, WorkerFailureCallback is called letting the user to decide whether the consumer should stop. */
	WorkerRetryThreshold int32

	/* Resets WorkerRetryThreshold if it isn't hit within this period. */
	WorkerThresholdTimeWindow time.Duration

	/* Callback executed when WorkerRetryThreshold exceeded within WorkerThresholdTimeWindow */
	WorkerFailureCallback FailedCallback

	/* Callback executed when Worker failed to process the message after MaxWorkerRetries and WorkerRetryThreshold is not hit */
	WorkerFailedAttemptCallback FailedAttemptCallback

	/* Worker timeout to process a single message. */
	WorkerTaskTimeout time.Duration

	/* Backoff between worker attempts to process a single message. */
	WorkerBackoff time.Duration

	/* Maximum wait time to gracefully stop a worker manager */
	WorkerManagersStopTimeout time.Duration

	/* A function which defines a user-specified action on a single message. This function is responsible for actual message processing.
	Consumer panics if Strategy is not set. */
	Strategy WorkerStrategy

	/* Number of messages to accumulate before flushing them to workers */
	FetchBatchSize int

	/* Timeout to accumulate messages. Flushes accumulated batch to workers even if it is not yet full.
	Resets after each flush meaning this won't be triggered if FetchBatchSize is reached before timeout. */
	FetchBatchTimeout time.Duration

	/* Backoff between fetch requests if no messages were fetched from a previous fetch. */
	RequeueAskNextBackoff time.Duration

	/* Buffer size for ask next channel. This value shouldn't be less than number of partitions per fetch routine. */
	AskNextChannelSize int

	/* Maximum fetch retries if no messages were fetched from a previous fetch */
	FetchMaxRetries int

	/* Maximum retries to fetch topic metadata from one broker. */
	FetchTopicMetadataRetries int

	/* Backoff for fetch topic metadata request if the previous request failed. */
	FetchTopicMetadataBackoff time.Duration

	/* Backoff between two fetch requests for one fetch routine. Needed to prevent fetcher from querying the broker too frequently. */
	FetchRequestBackoff time.Duration

	/* Coordinator used to coordinate consumer's actions, e.g. trigger rebalance events, store offsets and consumer metadata etc. */
	Coordinator ConsumerCoordinator

	/* OffsetStorage is used to store and retrieve consumer offsets. */
	OffsetStorage OffsetStorage

	/* Indicates whether the client supports blue-green deployment.
	This config entry is needed because blue-green deployment won't work with RoundRobin partition assignment strategy.
	Defaults to true. */
	BlueGreenDeploymentEnabled bool

	/* Time to wait after consumer has registered itself in group */
	DeploymentTimeout time.Duration

	/* Service coordinator barrier timeout */
	BarrierTimeout time.Duration

	/* Low Level Kafka Client implementation. */
	LowLevelClient LowLevelClient

	/* Message keys decoder */
	KeyDecoder Decoder

	/* Message values decoder */
	ValueDecoder Decoder

	/* Flag for debug mode */
	Debug bool

	/* Metrics Prefix if the client wants to organize the way metric names are emitted. (optional) */
	MetricsPrefix string

	/* Config to skip corrupted messages. If set to true the consumer will increment the topic-partition offset by 1
	   on each corrupted response until the corrupted part of data is over. Turned off by default. */
	SkipCorruptedMessages bool

	/* RoutinePoolSize defines the size of routine pools created within this consumer. */
	RoutinePoolSize int
}

//DefaultConsumerConfig creates a ConsumerConfig with sane defaults. Note that several required config entries (like Strategy and callbacks) are still not set.
func DefaultConsumerConfig() *ConsumerConfig {
	config := &ConsumerConfig{}
	config.Groupid = "go-consumer-group"
	config.SocketTimeout = 30 * time.Second
	config.FetchMessageMaxBytes = 1024 * 1024
	config.NumConsumerFetchers = 1
	config.QueuedMaxMessages = 3
	config.RebalanceMaxRetries = 4
	config.FetchMinBytes = 1
	config.FetchWaitMaxMs = 100
	config.RebalanceBackoff = 5 * time.Second
	config.RefreshLeaderBackoff = 200 * time.Millisecond
	config.OffsetsCommitMaxRetries = 5
	config.OffsetCommitInterval = 3 * time.Second

	config.AutoOffsetReset = LargestOffset
	config.Clientid = "go-client"
	config.ExcludeInternalTopics = true
	config.PartitionAssignmentStrategy = RangeStrategy /* select between "RangeStrategy", and "RoundRobinStrategy" */

	config.NumWorkers = 10
	config.MaxWorkerRetries = 3
	config.WorkerRetryThreshold = 100
	config.WorkerThresholdTimeWindow = 1 * time.Minute
	config.WorkerBackoff = 500 * time.Millisecond
	config.WorkerTaskTimeout = 1 * time.Minute
	config.WorkerManagersStopTimeout = 1 * time.Minute

	config.FetchBatchSize = 100
	config.FetchBatchTimeout = 5 * time.Second

	config.FetchMaxRetries = 5
	config.RequeueAskNextBackoff = 5 * time.Second
	config.AskNextChannelSize = 1000
	config.FetchTopicMetadataRetries = 3
	config.FetchTopicMetadataBackoff = 1 * time.Second
	config.FetchRequestBackoff = 10 * time.Millisecond

	config.Coordinator = NewZookeeperCoordinator(NewZookeeperConfig())
	config.BlueGreenDeploymentEnabled = true
	config.DeploymentTimeout = 0 * time.Second
	config.BarrierTimeout = 30 * time.Second
	config.LowLevelClient = NewSaramaClient(config)

	config.KeyDecoder = &ByteDecoder{}
	config.ValueDecoder = config.KeyDecoder

	config.RoutinePoolSize = 50

	return config
}

func (c *ConsumerConfig) String() string {
	return fmt.Sprintf(`
GroupId: %s
SocketTimeoutMs: %s
FetchMessageMaxBytes: %d
NumConsumerFetchers: %d
QueuedMaxMessages: %d
RebalanceMaxRetries: %d
FetchMinBytes: %d
FetchWaitMaxMs: %d
RebalanceBackoffMs: %d
RefreshLeaderBackoff: %d
OffsetsCommitMaxRetries: %d
AutoOffsetReset: %s
ClientId: %s
ConsumerId: %s
ExcludeInternalTopics: %v
PartitionAssignmentStrategy: %s
NumWorkers: %d
MaxWorkerRetries: %d
WorkerRetryThreshold %d
WorkerThresholdTimeWindow %v
WorkerFailureCallback %v
WorkerFailedAttemptCallback %v
WorkerTaskTimeout %v
WorkerBackoff %v
Strategy %v
FetchBatchSize %d
FetchBatchTimeout %v
`, c.Groupid, c.SocketTimeout,
		c.FetchMessageMaxBytes, c.NumConsumerFetchers, c.QueuedMaxMessages, c.RebalanceMaxRetries,
		c.FetchMinBytes, c.FetchWaitMaxMs,
		c.RebalanceBackoff, c.RefreshLeaderBackoff,
		c.OffsetsCommitMaxRetries,
		c.AutoOffsetReset, c.Clientid, c.Consumerid,
		c.ExcludeInternalTopics, c.PartitionAssignmentStrategy, c.NumWorkers,
		c.MaxWorkerRetries, c.WorkerRetryThreshold,
		c.WorkerThresholdTimeWindow, c.WorkerFailureCallback, c.WorkerFailedAttemptCallback,
		c.WorkerTaskTimeout, c.WorkerBackoff,
		c.Strategy, c.FetchBatchSize, c.FetchBatchTimeout)
}

// Validate this ConsumerConfig. Returns a corresponding error if the ConsumerConfig is invalid and nil otherwise.
func (c *ConsumerConfig) Validate() error {
	if c.Groupid == "" {
		return errors.New("Groupid cannot be empty")
	}

	if c.Consumerid == "" {
		c.Consumerid = uuid()
	}

	if c.NumConsumerFetchers <= 0 {
		return errors.New("NumConsumerFetchers should be at least 1")
	}

	if c.QueuedMaxMessages < 0 {
		return errors.New("QueuedMaxMessages cannot be less than 0")
	}

	if c.RebalanceMaxRetries < 0 {
		return errors.New("RebalanceMaxRetries cannot be less than 0")
	}

	if c.OffsetsCommitMaxRetries < 0 {
		return errors.New("OffsetsCommitMaxRetries cannot be less than 0")
	}

	if c.AutoOffsetReset != SmallestOffset && c.AutoOffsetReset != LargestOffset {
		return fmt.Errorf("AutoOffsetReset must be either \"%s\" or \"%s\"", SmallestOffset, LargestOffset)
	}

	if c.Clientid == "" {
		return errors.New("Clientid cannot be empty")
	}

	if c.PartitionAssignmentStrategy != RangeStrategy && c.PartitionAssignmentStrategy != RoundRobinStrategy {
		return fmt.Errorf("PartitionAssignmentStrategy must be either \"%s\" or \"%s\"", RangeStrategy, RoundRobinStrategy)
	}

	if c.NumWorkers <= 0 {
		return errors.New("NumWorkers should be at least 1")
	}

	if c.MaxWorkerRetries < 0 {
		return errors.New("MaxWorkerRetries cannot be less than 0")
	}

	if c.WorkerFailureCallback == nil {
		return errors.New("Please provide a WorkerFailureCallback")
	}

	if c.WorkerFailedAttemptCallback == nil {
		return errors.New("Please provide a WorkerFailedAttemptCallback")
	}

	if c.WorkerThresholdTimeWindow < time.Millisecond {
		return errors.New("WorkerThresholdTimeWindow must be at least 1ms")
	}

	if c.Strategy == nil {
		return errors.New("Please provide a Strategy")
	}

	if c.FetchBatchSize <= 0 {
		return errors.New("FetchBatchSize should be at least 1")
	}

	if c.FetchMaxRetries < 0 {
		return errors.New("FetchMaxRetries cannot be less than 0")
	}

	if c.FetchTopicMetadataRetries < 0 {
		return errors.New("FetchTopicMetadataRetries cannot be less than 0")
	}

	if c.Coordinator == nil {
		return errors.New("Please provide a Coordinator")
	}

	if c.OffsetStorage == nil {
		// This is for folks who already use this client
		if zookeeper, ok := c.Coordinator.(*ZookeeperCoordinator); ok {
			c.OffsetStorage = zookeeper
		} else {
			return errors.New("Please provide an OffsetStorage")
		}
	}

	if c.BlueGreenDeploymentEnabled && c.PartitionAssignmentStrategy != RangeStrategy {
		return errors.New("In order to use Blue-Green deployment Range partition assignment strategy should be used")
	}

	if c.LowLevelClient == nil {
		return errors.New("Low level client is not set")
	}

	if c.KeyDecoder == nil {
		return errors.New("Key decoder is not set")
	}

	if c.ValueDecoder == nil {
		return errors.New("Value decoder is not set")
	}

	return nil
}

// ConsumerConfigFromFile is a helper function that loads a consumer's configuration from file.
// The file accepts the following fields:
//  group.id
//  consumer.id
//  fetch.message.max.bytes
//  num.consumer.fetchers
//  rebalance.max.retries
//  queued.max.message.chunks
//  fetch.min.bytes
//  fetch.wait.max.ms
//  rebalance.backoff
//  refresh.leader.backoff
//  offset.commit.max.retries
//  offset.commit.interval
//  offsets.storage
//  auto.offset.reset
//  exclude.internal.topics
//  partition.assignment.strategy
//  num.workers
//  max.worker.retries
//  worker.retry.threshold
//  worker.threshold.time.window
//  worker.task.timeout
//  worker.backoff
//  worker.managers.stop.timeout
//  fetch.batch.size
//  fetch.batch.timeout
//  requeue.ask.next.backoff
//  fetch.max.retries
//  fetch.topic.metadata.retries
//  fetch.topic.metadata.backoff
//  fetch.request.backoff
//  blue.green.deployment.enabled
// The configuration file entries should be constructed in key=value syntax. A # symbol at the beginning
// of a line indicates a comment. Blank lines are ignored. The file should end with a newline character.
func ConsumerConfigFromFile(filename string) (*ConsumerConfig, error) {
	c, err := LoadConfiguration(filename)
	if err != nil {
		return nil, err
	}

	config := DefaultConsumerConfig()
	setStringConfig(&config.Groupid, c["group.id"])
	setStringConfig(&config.Consumerid, c["consumer.id"])
	if err := setDurationConfig(&config.SocketTimeout, c["socket.timeout"]); err != nil {
		return nil, err
	}
	if err := setInt32Config(&config.FetchMessageMaxBytes, c["fetch.message.max.bytes"]); err != nil {
		return nil, err
	}
	if err := setIntConfig(&config.NumConsumerFetchers, c["num.consumer.fetchers"]); err != nil {
		return nil, err
	}
	if err := setInt32Config(&config.QueuedMaxMessages, c["queued.max.message.chunks"]); err != nil {
		return nil, err
	}
	if err := setInt32Config(&config.RebalanceMaxRetries, c["rebalance.max.retries"]); err != nil {
		return nil, err
	}
	if err := setInt32Config(&config.FetchMinBytes, c["fetch.min.bytes"]); err != nil {
		return nil, err
	}
	if err := setInt32Config(&config.FetchWaitMaxMs, c["fetch.wait.max.ms"]); err != nil {
		return nil, err
	}
	if err := setDurationConfig(&config.RebalanceBackoff, c["rebalance.backoff"]); err != nil {
		return nil, err
	}
	if err := setDurationConfig(&config.RefreshLeaderBackoff, c["refresh.leader.backoff"]); err != nil {
		return nil, err
	}
	if err := setIntConfig(&config.OffsetsCommitMaxRetries, c["offset.commit.max.retries"]); err != nil {
		return nil, err
	}
	if err := setDurationConfig(&config.OffsetCommitInterval, c["offset.commit.interval"]); err != nil {
		return nil, err
	}
	setStringConfig(&config.AutoOffsetReset, c["auto.offset.reset"])
	setBoolConfig(&config.ExcludeInternalTopics, c["exclude.internal.topics"])
	setStringConfig(&config.PartitionAssignmentStrategy, c["partition.assignment.strategy"])
	if err := setIntConfig(&config.NumWorkers, c["num.workers"]); err != nil {
		return nil, err
	}
	if err := setIntConfig(&config.MaxWorkerRetries, c["max.worker.retries"]); err != nil {
		return nil, err
	}
	if err := setInt32Config(&config.WorkerRetryThreshold, c["worker.retry.threshold"]); err != nil {
		return nil, err
	}
	if err := setDurationConfig(&config.WorkerThresholdTimeWindow, c["worker.threshold.time.window"]); err != nil {
		return nil, err
	}
	if err := setDurationConfig(&config.WorkerTaskTimeout, c["worker.task.timeout"]); err != nil {
		return nil, err
	}
	if err := setDurationConfig(&config.WorkerBackoff, c["worker.backoff"]); err != nil {
		return nil, err
	}
	if err := setDurationConfig(&config.WorkerManagersStopTimeout, c["worker.managers.stop.timeout"]); err != nil {
		return nil, err
	}
	if err := setIntConfig(&config.FetchBatchSize, c["fetch.batch.size"]); err != nil {
		return nil, err
	}
	if err := setDurationConfig(&config.FetchBatchTimeout, c["fetch.batch.timeout"]); err != nil {
		return nil, err
	}
	if err := setDurationConfig(&config.RequeueAskNextBackoff, c["requeue.ask.next.backoff"]); err != nil {
		return nil, err
	}
	if err := setIntConfig(&config.FetchMaxRetries, c["fetch.max.retries"]); err != nil {
		return nil, err
	}
	if err := setIntConfig(&config.FetchTopicMetadataRetries, c["fetch.topic.metadata.retries"]); err != nil {
		return nil, err
	}
	if err := setDurationConfig(&config.FetchTopicMetadataBackoff, c["fetch.topic.metadata.backoff"]); err != nil {
		return nil, err
	}
	if err := setDurationConfig(&config.FetchRequestBackoff, c["fetch.request.backoff"]); err != nil {
		return nil, err
	}
	if err := setDurationConfig(&config.DeploymentTimeout, c["deployment.timeout"]); err != nil {
		return nil, err
	}
	if err := setDurationConfig(&config.BarrierTimeout, c["barrier.timeout"]); err != nil {
		return nil, err
	}
	if err := setIntConfig(&config.RoutinePoolSize, c["routine.pool.size"]); err != nil {
		return nil, err
	}
	setBoolConfig(&config.BlueGreenDeploymentEnabled, c["blue.green.deployment.enabled"])

	return config, nil
}
