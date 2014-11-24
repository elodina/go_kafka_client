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

package go_kafka_client

import (
	"time"
	"fmt"
)

type ConsumerConfig struct {
	/** a string that uniquely identifies a set of consumers within the same consumer group */
	Groupid string

	/** consumer id: generated automatically if not set.
   	 *  Set this explicitly for only testing purpose.
   	 */
	ConsumerId string

	/** the socket timeout for network requests. Its value should be at least fetch.wait.max.ms. */
	SocketTimeout time.Duration

	/** the number of byes of messages to attempt to fetch */
	FetchMessageMaxBytes int32

	/** the number threads used to fetch data */
	NumConsumerFetchers int

	/** max number of message chunks buffered for consumption, each chunk can be up to fetch.message.max.bytes*/
	QueuedMaxMessages int32

	/** max number of retries during rebalance */
	RebalanceMaxRetries int32

	/** the minimum amount of data the server should return for a fetch request. If insufficient data is available the request will block */
	FetchMinBytes int32

	/** the maximum amount of time the server will block before answering the fetch request if there isn't sufficient data to immediately satisfy fetch.min.bytes */
	FetchWaitMaxMs int32

	/** backoff time between retries during rebalance */
	RebalanceBackoff time.Duration

	/** backoff time to refresh the leader of a partition after it loses the current leader */
	RefreshLeaderBackoff time.Duration

	/** Retry the offset commit up to this many times on failure. This retry count only applies to offset commits during
		* shut-down. It does not apply to commits from the auto-commit thread. It also does not apply to attempts to query
		* for the offset coordinator before committing offsets. i.e., if a consumer metadata request fails for any reason,
		* it is retried and that retry does not count toward this limit. */
	OffsetsCommitMaxRetries int32

	/** Specify whether offsets should be committed to "zookeeper" (default) or "kafka" */
	OffsetsStorage string

	/* what to do if an offset is out of range.
		 smallest : automatically reset the offset to the smallest offset
		 largest : automatically reset the offset to the largest offset
		 anything else: throw exception to the consumer */
	AutoOffsetReset string

	/**
	   * Client id is specified by the kafka consumer client, used to distinguish different clients
	   */
	ClientId string

	/** Whether messages from int32ernal topics (such as offsets) should be exposed to the consumer. */
	ExcludeInternalTopics bool

	/** Select a strategy for assigning partitions to consumer streams. Possible values: range, roundrobin */
	PartitionAssignmentStrategy string

	/* Zookeeper hosts */
	ZookeeperConnect []string

	/** Zookeeper read timeout */
	ZookeeperTimeout time.Duration

	/* Amount of workers */
	NumWorkers int

	/* Times to retry failed message processing by worker */
	MaxWorkerRetries int

	/* Worker retry threshold */
	WorkerRetryThreshold int32

	/* Time period in which workers could be considered failed if WorkerRetryThreshold is exceeded */
	WorkerConsideredFailedTimeWindow time.Duration

	/* Callback executed when WorkerRetryThreshold exceeded within WorkerConsideredFailedTimeWindow */
	WorkerFailureCallback FailedCallback

	/* Callback executed when Worker failed to process the message after MaxWorkerRetries and WorkerRetryThreshold is not hit */
	WorkerFailedAttemptCallback FailedAttemptCallback

	/* Task timeout */
	WorkerTaskTimeout time.Duration

	/* Backoff for worker message processing */
	WorkerBackoff time.Duration

	/* Timeout for processing the whole batch by cosumer */
	WorkerBatchTimeout time.Duration

	/* Worker managers stop timeout */
	WorkerManagersStopTimeout time.Duration

	/* Worker strategy */
	Strategy WorkerStrategy

	/* Batch size */
	FetchBatchSize int

	/* Timeout to accumulate messages */
	FetchBatchTimeout time.Duration

	/* Backoff to requeue ask next if no messages were fetched */
	RequeueAskNextBackoff time.Duration

	/* Fetch max retries */
	FetchMaxRetries int
}

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
	config.OffsetsStorage = "zookeeper"

	config.AutoOffsetReset = "largest"
	config.ClientId = ""
	config.ConsumerId = "consumer1"
	config.ExcludeInternalTopics = true
	config.PartitionAssignmentStrategy = "range"/* select between "range", and "roundrobin" */
	config.ZookeeperConnect = []string{"localhost"}
	config.ZookeeperTimeout = 1 * time.Second

	config.NumWorkers = 10
	config.MaxWorkerRetries = 3
	config.WorkerRetryThreshold = 100
	config.WorkerBackoff = 500 * time.Millisecond
	config.WorkerBatchTimeout = 5 * time.Minute
	config.WorkerTaskTimeout = 1 * time.Minute
	config.WorkerManagersStopTimeout = 1 * time.Minute

	config.FetchBatchSize = 100
	config.FetchBatchTimeout = 5 * time.Second

	config.FetchMaxRetries = 5
	config.RequeueAskNextBackoff = 1 * time.Second

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
OffsetsStorage: %s
AutoOffsetReset: %s
ClientId: %s
ConsumerId: %s
ExcludeInternalTopics: %v
PartitionAssignmentStrategy: %s
ZookeeperConnect: %s
ZookeeperTimeout: %d
NumWorkers: %d
MaxWorkerRetries: %d
WorkerRetryThreshold %d
WorkerConsideredFailedTimeWindow %v
WorkerFailureCallback %v
WorkerFailedAttemptCallback %v
WorkerTaskTimeout %v
WorkerBackoff %v
WorkerBatchTimeout %v
Strategy %v
FetchBatchSize %d
FetchBatchTimeout %v
`, c.Groupid, c.SocketTimeout,
   c.FetchMessageMaxBytes, c.NumConsumerFetchers, c.QueuedMaxMessages, c.RebalanceMaxRetries,
   c.FetchMinBytes, c.FetchWaitMaxMs,
   c.RebalanceBackoff, c.RefreshLeaderBackoff,
   c.OffsetsCommitMaxRetries, c.OffsetsStorage,
   c.AutoOffsetReset, c.ClientId, c.ConsumerId,
   c.ExcludeInternalTopics, c.PartitionAssignmentStrategy, c.ZookeeperConnect,
   c.ZookeeperTimeout, c.NumWorkers, c.MaxWorkerRetries, c.WorkerRetryThreshold,
   c.WorkerConsideredFailedTimeWindow, c.WorkerFailureCallback, c.WorkerFailedAttemptCallback,
   c.WorkerTaskTimeout, c.WorkerBackoff, c.WorkerBatchTimeout,
   c.Strategy, c.FetchBatchSize, c.FetchBatchTimeout)
}
