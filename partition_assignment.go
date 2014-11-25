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
	"reflect"
	"fmt"
	"math"
)

type ConsumerGroupContextState struct {
	IsGroupTopicSwitchInProgress bool
	IsGroupTopicSwitchInSync bool
	DesiredPattern string
	DesiredTopicCountMap map[string]int
}

type AssignStrategy func(context *AssignmentContext) map[TopicAndPartition]ConsumerThreadId

func NewPartitionAssignor(strategy string) AssignStrategy {
	switch strategy {
	case "roundrobin":
		return RoundRobinAssignor
	default:
		return RangeAssignor
	}
}

/**
 * The round-robin partition assignor lays out all the available partitions and all the available consumer threads. It
 * then proceeds to do a round-robin assignment from partition to consumer thread. If the subscriptions of all consumer
 * instances are identical, then the partitions will be uniformly distributed. (i.e., the partition ownership counts
 * will be within a delta of exactly one across all consumer threads.)
 *
 * (For simplicity of implementation) the assignor is allowed to assign a given topic-partition to any consumer instance
 * and thread-id within that instance. Therefore, round-robin assignment is allowed only if:
 * a) Every topic has the same number of streams within a consumer instance
 * b) The set of subscribed topics is identical for every consumer instance within the group.
 */
func RoundRobinAssignor(context *AssignmentContext) map[TopicAndPartition]ConsumerThreadId {
	ownershipDecision := make(map[TopicAndPartition]ConsumerThreadId)

	if (len(context.ConsumersForTopic) > 0) {
		var headThreadIds []ConsumerThreadId
		for _, headThreadIds = range context.ConsumersForTopic { break }
		for _, threadIds := range context.ConsumersForTopic {
			if (!reflect.DeepEqual(threadIds, headThreadIds)) {
				panic("Round-robin assignor works only if all consumers in group subscribed to the same topics AND if the stream counts across topics are identical for a given consumer instance.")
			}
		}

		topicsAndPartitions := make([]*TopicAndPartition, 0)
		for topic, partitions := range context.PartitionsForTopic {
			for _, partition := range partitions {
				topicsAndPartitions = append(topicsAndPartitions, &TopicAndPartition{
						Topic: topic,
						Partition: partition,
					})
			}
		}

		fmt.Printf("%v\n", topicsAndPartitions)

		shuffledTopicsAndPartitions := make([]*TopicAndPartition, len(topicsAndPartitions))
		ShuffleArray(&topicsAndPartitions, &shuffledTopicsAndPartitions)
		threadIdsIterator := CircularIterator(&headThreadIds)

		fmt.Printf("%v\n", shuffledTopicsAndPartitions)

		for _, topicPartition := range shuffledTopicsAndPartitions {
			consumerThreadId := threadIdsIterator.Value.(ConsumerThreadId)
			if (consumerThreadId.Consumer == context.ConsumerId) {
				ownershipDecision[*topicPartition] = consumerThreadId
			}
			threadIdsIterator = threadIdsIterator.Next()
		}
	}

	return ownershipDecision
}

/**
 * Range partitioning works on a per-topic basis. For each topic, we lay out the available partitions in numeric order
 * and the consumer threads in lexicographic order. We then divide the number of partitions by the total number of
 * consumer streams (threads) to determine the number of partitions to assign to each consumer. If it does not evenly
 * divide, then the first few consumers will have one extra partition. For example, suppose there are two consumers C1
 * and C2 with two streams each, and there are five available partitions (p0, p1, p2, p3, p4). So each consumer thread
 * will get at least one partition and the first consumer thread will get one extra partition. So the assignment will be:
 * p0 -> C1-0, p1 -> C1-0, p2 -> C1-1, p3 -> C2-0, p4 -> C2-1
 */
func RangeAssignor(context *AssignmentContext) map[TopicAndPartition]ConsumerThreadId {
	ownershipDecision := make(map[TopicAndPartition]ConsumerThreadId)

	for topic, consumerThreadIds := range context.MyTopicThreadIds {
		consumersForTopic := context.ConsumersForTopic[topic]
		partitionsForTopic := context.PartitionsForTopic[topic]

		Debug(context.ConsumerId, partitionsForTopic)

		Tracef(context.ConsumerId, "partitionsForTopic: %d, consumersForTopic: %d", len(partitionsForTopic), len(consumersForTopic))

		nPartsPerConsumer := len(partitionsForTopic) / len(consumersForTopic)
		nConsumersWithExtraPart := len(partitionsForTopic) % len(consumersForTopic)

		Tracef(context.ConsumerId, "nPartsPerConsumer: %d, nConsumersWithExtraPart: %d", nPartsPerConsumer, nConsumersWithExtraPart)

		for _, consumerThreadId := range consumerThreadIds {
			myConsumerPosition := Position(&consumersForTopic, consumerThreadId)
			Tracef(context.ConsumerId, "myConsumerPosition: %d", myConsumerPosition)
			if (myConsumerPosition < 0) {
				panic(fmt.Sprintf("There is no %s in consumers for topic %s", consumerThreadId, topic))
			}
			startPart := nPartsPerConsumer * myConsumerPosition + int(math.Min(float64(myConsumerPosition), float64(nConsumersWithExtraPart)))
			nParts := nPartsPerConsumer
			if (myConsumerPosition+1 <= nConsumersWithExtraPart) {
				nParts = nPartsPerConsumer+1
			}
			Tracef(context.ConsumerId, "startPart: %d, nParts: %d", startPart, nParts)

			if (nParts <= 0) {
				Warnf(context.ConsumerId, "No broker partitions consumed by consumer thread %s for topic %s", consumerThreadId, topic)
			} else {
				for i := startPart; i < startPart+nParts; i++ {
					partition := partitionsForTopic[i]
					Infof(context.ConsumerId, "%s attempting to claim partition %d", consumerThreadId, partition)
					ownershipDecision[TopicAndPartition{ Topic: topic, Partition: partition, }] = consumerThreadId
				}
			}
		}
	}

	return ownershipDecision
}

type AssignmentContext struct {
	ConsumerId string
	Group      string
	MyTopicThreadIds map[string][]ConsumerThreadId
	MyTopicToNumStreams TopicsToNumStreams
	PartitionsForTopic map[string][]int32
	ConsumersForTopic map[string][]ConsumerThreadId
	Consumers  []string
	InTopicSwitch bool
	State *ConsumerGroupContextState
}

func NewAssignmentContext(group string, consumerId string, excludeInternalTopics bool, coordinator ConsumerCoordinator) (*AssignmentContext, error) {
	topicCount, _ := NewTopicsToNumStreams(group, consumerId, coordinator, excludeInternalTopics)
	_, inTopicSwitch := topicCount.(*TopicSwitch)
	myTopicThreadIds := topicCount.GetConsumerThreadIdsPerTopic()
	topics := make([]string, 0)
	for topic, _ := range myTopicThreadIds {
		topics = append(topics, topic)
	}
	partitionsForTopic, _ := coordinator.GetPartitionsForTopics(topics)
	consumersForTopic, _ := coordinator.GetConsumersPerTopic(group, excludeInternalTopics)
	consumers, _ := coordinator.GetConsumersInGroup(group)

	isGroupTopicSwitchInProgress := false
	isGroupTopicSwitchInSync := true
	desiredPattern := ""
	desiredTopicCountMap := make(map[string]int)
	for _, consumerId := range consumers {
		tc, err := NewTopicsToNumStreams(group, consumerId, coordinator, excludeInternalTopics)
		topicSwitch, inTopicSwitch := tc.(*TopicSwitch)
		if (err != nil) {
			return nil, err
		}
		if !inTopicSwitch {
			isGroupTopicSwitchInSync = false
		} else if !isGroupTopicSwitchInProgress && inTopicSwitch {
			isGroupTopicSwitchInProgress = true
			desiredTopicCountMap = topicSwitch.GetTopicsToNumStreamsMap()
			desiredPattern = topicSwitch.Pattern()
		}
	}

	return &AssignmentContext{
		ConsumerId: consumerId,
		Group: group,
		MyTopicThreadIds: myTopicThreadIds,
		MyTopicToNumStreams: topicCount,
		PartitionsForTopic: partitionsForTopic,
		ConsumersForTopic: consumersForTopic,
		Consumers: consumers,
		State: &ConsumerGroupContextState{
			IsGroupTopicSwitchInProgress: isGroupTopicSwitchInProgress,
			IsGroupTopicSwitchInSync: isGroupTopicSwitchInSync,
			DesiredPattern: desiredPattern,
			DesiredTopicCountMap: desiredTopicCountMap,
		},
		InTopicSwitch: inTopicSwitch,
	}, nil
}

func NewStaticAssignmentContext(group string, consumerId string, topicCount TopicsToNumStreams, topicPartitionMap map[string][]int32) *AssignmentContext {
	myTopicThreadIds := topicCount.GetConsumerThreadIdsPerTopic()

	return &AssignmentContext{
		ConsumerId: consumerId,
		Group: group,
		MyTopicThreadIds: myTopicThreadIds,
		MyTopicToNumStreams: topicCount,
		PartitionsForTopic: topicPartitionMap,
		ConsumersForTopic: myTopicThreadIds,
		Consumers: []string{consumerId},
	}
}
