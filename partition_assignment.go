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
	"github.com/samuel/go-zookeeper/zk"
)

type AssignStrategy func(context *AssignmentContext) map[TopicAndPartition]ConsumerThreadId

func NewPartitionAssignor(strategy string) AssignStrategy {
	switch strategy {
	case "roundrobin":
		return RoundRobinAssignor
	default:
		return RangeAssignor
	}
}

func RoundRobinAssignor(context *AssignmentContext) map[TopicAndPartition]ConsumerThreadId {
	return make(map[TopicAndPartition]ConsumerThreadId)
}

func RangeAssignor(context *AssignmentContext) map[TopicAndPartition]ConsumerThreadId {
	return make(map[TopicAndPartition]ConsumerThreadId)
}

type AssignmentContext struct {
	MyTopicThreadIds map[string][]*ConsumerThreadId
	PartitionsForTopic map[string][]int
	ConsumersForTopic map[string][]*ConsumerThreadId
	Consumers []string
}

func NewAssignmentContext(group string, consumerId string, excludeInternalTopics bool, zkConnection *zk.Conn) *AssignmentContext {
	topicCount, _ := NewTopicsToNumStreams(group, consumerId, zkConnection, excludeInternalTopics)
	myTopicThreadIds := topicCount.GetConsumerThreadIdsPerTopic()
	topics := make([]string, len(myTopicThreadIds))
	for topic, _ := range myTopicThreadIds {
		topics = append(topics, topic)
	}
	partitionsForTopic, _ := GetPartitionsForTopics(zkConnection, topics)
	consumersForTopic, _ := GetConsumersPerTopic(zkConnection, group, excludeInternalTopics)
	consumers, _ := GetConsumersInGroup(zkConnection, group)
	return &AssignmentContext{
		MyTopicThreadIds: myTopicThreadIds,
		PartitionsForTopic: partitionsForTopic,
		ConsumersForTopic: consumersForTopic,
		Consumers: consumers,
	}
}
