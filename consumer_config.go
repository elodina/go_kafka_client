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

type ConsumerConfig struct {
	RefreshMetadataBackoffMs int32
	SocketTimeout int32
	SocketBufferSize int32
	FetchSize int32
	MaxFetchSize int32
	NumConsumerFetchers int32
	DefaultFetcherBackoffMs int32
	AutoCommit bool
	AutoCommitInterval int32
	MaxQueuedChunks int32
	MaxRebalanceRetries int32
	AutoOffsetReset string
	ConsumerTimeoutMs int32
	MinFetchBytes int32
	MaxFetchWaitMs int32
	MirrorTopicsWhitelist string
	MirrorTopicsBlacklist string
	MirrorConsumerNumThreads int32
	OffsetsChannelBackoffMs int32
	OffsetsChannelSocketTimeoutMs int32
	OffsetsCommitMaxRetries int32
	OffsetsStorage string
	MirrorTopicsWhitelistProp string
	MirrorTopicsBlacklistProp string
	ExcludeInternalTopics bool
	DefaultPartitionAssignmentStrategy string /* select between "range", and "roundrobin" */
	MirrorConsumerNumThreadsProp string
	DefaultClientId string
}

func Default(config *ConsumerConfig) {
	config.RefreshMetadataBackoffMs = 200
	config.SocketTimeout = 30 * 1000
	config.SocketBufferSize = 64 * 1024
	config.FetchSize = 1024 * 1024
	config.MaxFetchSize = 10 * config.FetchSize
	config.NumConsumerFetchers = 1
	config.DefaultFetcherBackoffMs = 1000
	config.AutoCommit = true
	config.AutoCommitInterval = 60 * 1000
	config.MaxQueuedChunks = 2
	config.MaxRebalanceRetries = 4
	config.AutoOffsetReset = "largest"
	config.ConsumerTimeoutMs = -1
	config.MinFetchBytes = 1
	config.MaxFetchWaitMs = 100
	config.MirrorTopicsWhitelist = ""
	config.MirrorTopicsBlacklist = ""
	config.MirrorConsumerNumThreads = 1
	config.OffsetsChannelBackoffMs = 1000
	config.OffsetsChannelSocketTimeoutMs = 10000
	config.OffsetsCommitMaxRetries = 5
	config.OffsetsStorage = "zookeeper"

	config.MirrorTopicsWhitelistProp = "mirror.topic.whitelist"
	config.MirrorTopicsBlacklistProp = "mirror.topic.blacklist"
	config.ExcludeInternalTopics = true
	config.DefaultPartitionAssignmentStrategy = "range"/* select between "range", and "roundrobin" */
	config.MirrorConsumerNumThreadsProp = "mirror.consumer.numthreads"
	config.DefaultClientId = ""
}
