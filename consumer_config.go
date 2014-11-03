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

import "time"

type ConsumerConfig struct {
	/** a string that uniquely identifies a set of consumers within the same consumer group */
	Groupid string

	/** consumer id: generated automatically if not set.
   	 *  Set this explicitly for only testing purpose.
   	 */
	ConsumerId string

	/** the socket timeout for network requests. Its value should be at least fetch.wait.max.ms. */
	SocketTimeoutMs int32

	/** the socket receive buffer for network requests */
	SocketReceiveBufferBytes int32

	/** the number of byes of messages to attempt to fetch */
	FetchMessageMaxBytes int32

	/** the number threads used to fetch data */
	NumConsumerFetchers int32

	/** if true, periodically commit to zookeeper the offset of messages already fetched by the consumer */
	AutoCommitEnable bool

	/** the frequency in ms that the consumer offsets are committed to zookeeper */
	AutoCommitIntervalMs int32

	/** max number of message chunks buffered for consumption, each chunk can be up to fetch.message.max.bytes*/
	QueuedMaxMessages int32

	/** max number of retries during rebalance */
	RebalanceMaxRetries int32

	/** the minimum amount of data the server should return for a fetch request. If insufficient data is available the request will block */
	FetchMinBytes int32

	/** the maximum amount of time the server will block before answering the fetch request if there isn't sufficient data to immediately satisfy fetch.min.bytes */
	FetchWaitMaxMs int32

	/** backoff time between retries during rebalance */
	RebalanceBackoffMs int32

	/** backoff time to refresh the leader of a partition after it loses the current leader */
	RefreshLeaderBackoff time.Duration

	/** backoff time to reconnect the offsets channel or to retry offset fetches/commits */
	OffsetsChannelBackoffMs int32
	/** socket timeout to use when reading responses for Offset Fetch/Commit requests. This timeout will also be used for
	   *  the ConsumerMetdata requests that are used to query for the offset coordinator. */
	OffsetsChannelSocketTimeoutMs int32

	/** Retry the offset commit up to this many times on failure. This retry count only applies to offset commits during
		* shut-down. It does not apply to commits from the auto-commit thread. It also does not apply to attempts to query
		* for the offset coordinator before committing offsets. i.e., if a consumer metadata request fails for any reason,
		* it is retried and that retry does not count toward this limit. */
	OffsetsCommitMaxRetries int32

	/** Specify whether offsets should be committed to "zookeeper" (default) or "kafka" */
	OffsetsStorage string

	/** If you are using "kafka" as offsets.storage, you can dual commit offsets to ZooKeeper (in addition to Kafka). This
		* is required during migration from zookeeper-based offset storage to kafka-based offset storage. With respect to any
		* given consumer group, it is safe to turn this off after all instances within that group have been migrated to
		* the new jar that commits offsets to the broker (instead of directly to ZooKeeper). */
	DualCommitEnabled bool

	/* what to do if an offset is out of range.
		 smallest : automatically reset the offset to the smallest offset
		 largest : automatically reset the offset to the largest offset
		 anything else: throw exception to the consumer */
	AutoOffsetReset string

	/** throw a timeout exception to the consumer if no message is available for consumption after the specified int32erval */
	ConsumerTimeoutMs int32

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
}

func DefaultConsumerConfig() *ConsumerConfig {
	config := &ConsumerConfig{}
	config.Groupid = "go-consumer-group"
	config.SocketTimeoutMs = 30 * 1000
	config.SocketReceiveBufferBytes = 64 * 1024
	config.FetchMessageMaxBytes = 1024 * 1024
	config.NumConsumerFetchers = 1
	config.AutoCommitEnable = true
	config.AutoCommitIntervalMs = 60 * 1000
	config.QueuedMaxMessages = 2
	config.RebalanceMaxRetries = 4
	config.ConsumerTimeoutMs = -1
	config.FetchMinBytes = 1
	config.FetchWaitMaxMs = 100
	config.RebalanceBackoffMs = 2000
	config.RefreshLeaderBackoff = 200 * time.Millisecond
	config.OffsetsChannelBackoffMs = 1000
	config.OffsetsChannelSocketTimeoutMs = 10000
	config.OffsetsCommitMaxRetries = 5
	config.OffsetsStorage = "zookeeper"

	config.AutoOffsetReset = "largest"
	config.ClientId = ""
	config.ConsumerId = "consumer1"
	config.ExcludeInternalTopics = true
	config.PartitionAssignmentStrategy = "range"/* select between "range", and "roundrobin" */
	config.ZookeeperConnect = []string{"localhost"}
	config.ZookeeperTimeout = 1 * time.Second

	return config
}
