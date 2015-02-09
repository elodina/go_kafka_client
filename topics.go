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

import "strings"

//Information on Consumer subscription. Used to keep it in consumer coordinator.
type TopicsToNumStreams interface {
	//Creates a map descibing consumer subscription where keys are topic names and values are number of fetchers used to fetch these topics.
	GetTopicsToNumStreamsMap() map[string]int

	//Creates a map describing consumer subscription where keys are topic names and values are slices of ConsumerThreadIds used to fetch these topics.
	GetConsumerThreadIdsPerTopic() map[string][]ConsumerThreadId

	//Returns a pattern describing this TopicsToNumStreams.
	Pattern() string
}

// Constructs a new TopicsToNumStreams for consumer with Consumerid id that works within consumer group Groupid.
// Uses Coordinator to get consumer information. Returns error if fails to retrieve consumer information from Coordinator.
func NewTopicsToNumStreams(Groupid string, Consumerid string, Coordinator ConsumerCoordinator, ExcludeInternalTopics bool) (TopicsToNumStreams, error) {
	consumerInfo, err := Coordinator.GetConsumerInfo(Consumerid, Groupid)
	if err != nil {
		return nil, err
	}

	hasWhiteList := whiteListPattern == consumerInfo.Pattern
	hasBlackList := blackListPattern == consumerInfo.Pattern

	if len(consumerInfo.Subscription) == 0 || !(hasWhiteList || hasBlackList) {
		return &StaticTopicsToNumStreams{
			ConsumerId:            Consumerid,
			TopicsToNumStreamsMap: consumerInfo.Subscription,
		}, nil
	} else {
		var regex string
		var numStreams int
		for regex, numStreams = range consumerInfo.Subscription {
			break
		}
		var filter TopicFilter
		if hasWhiteList {
			filter = NewWhiteList(regex)
		} else {
			filter = NewBlackList(regex)
		}

		return &WildcardTopicsToNumStreams{
			Coordinator:           Coordinator,
			ConsumerId:            Consumerid,
			TopicFilter:           filter,
			NumStreams:            numStreams,
			ExcludeInternalTopics: ExcludeInternalTopics,
		}, nil
	}
}

// Constructs a new TopicsToNumStreams for consumer with Consumerid id that works within consumer group Groupid.
// Uses Coordinator to get consumer information. Returns error if fails to retrieve consumer information from Coordinator.
func NewStaticTopicsToNumStreams(consumerId string,
	topics string,
	pattern string,
	numStreams int,
	excludeInternalTopics bool,
	coordinator ConsumerCoordinator) TopicsToNumStreams {
	hasWhiteList := whiteListPattern == pattern
	hasBlackList := blackListPattern == pattern
	if hasWhiteList || hasBlackList {
		regex := topics
		var filter TopicFilter
		if hasWhiteList {
			filter = NewWhiteList(regex)
		} else {
			filter = NewBlackList(regex)
		}

		return &WildcardTopicsToNumStreams{
			Coordinator:           coordinator,
			ConsumerId:            consumerId,
			TopicFilter:           filter,
			NumStreams:            numStreams,
			ExcludeInternalTopics: excludeInternalTopics,
		}
	} else {
		topicsToNumStreamsMap := make(map[string]int)
		partitionsForTopic, err := coordinator.GetPartitionsForTopics(strings.Split(topics, ","))
		if err != nil {
			panic(err)
		}
		for topic := range partitionsForTopic {
			topicsToNumStreamsMap[topic] = numStreams
		}
		return &StaticTopicsToNumStreams{
			ConsumerId:            consumerId,
			TopicsToNumStreamsMap: topicsToNumStreamsMap,
		}
	}
}

func makeConsumerThreadIdsPerTopic(consumerId string, TopicsToNumStreamsMap map[string]int) map[string][]ConsumerThreadId {
	result := make(map[string][]ConsumerThreadId)
	for topic, numConsumers := range TopicsToNumStreamsMap {
		consumers := make([]ConsumerThreadId, numConsumers)
		if numConsumers < 1 {
			panic("Number of consumers should be greater than 0")
		}
		for i := 0; i < numConsumers; i++ {
			consumerThreadId := ConsumerThreadId{consumerId, i}
			exists := false
			for i := 0; i < numConsumers; i++ {
				if consumers[i] == consumerThreadId {
					exists = true
					break
				}
			}
			if !exists {
				consumers[i] = consumerThreadId
			}
		}
		result[topic] = consumers[:len(consumers)]
	}

	return result
}

// TopicsToNumStreams implementation representing a static consumer subscription.
type StaticTopicsToNumStreams struct {
	// Consumer id string.
	ConsumerId string
	// Map where keys are topic names and values are number of fetcher routines responsible for processing these topics.
	TopicsToNumStreamsMap map[string]int
}

//Creates a map describing consumer subscription where keys are topic names and values are slices of ConsumerThreadIds used to fetch these topics.
func (tc *StaticTopicsToNumStreams) GetConsumerThreadIdsPerTopic() map[string][]ConsumerThreadId {
	return makeConsumerThreadIdsPerTopic(tc.ConsumerId, tc.TopicsToNumStreamsMap)
}

//Creates a map descibing consumer subscription where keys are topic names and values are number of fetchers used to fetch these topics.
func (tc *StaticTopicsToNumStreams) GetTopicsToNumStreamsMap() map[string]int {
	return tc.TopicsToNumStreamsMap
}

//Returns a pattern describing this TopicsToNumStreams.
func (tc *StaticTopicsToNumStreams) Pattern() string {
	return staticPattern
}

//TopicsToNumStreams implementation representing either whitelist or blacklist consumer subscription.
type WildcardTopicsToNumStreams struct {
	Coordinator           ConsumerCoordinator
	ConsumerId            string
	TopicFilter           TopicFilter
	NumStreams            int
	ExcludeInternalTopics bool
}

//Creates a map describing consumer subscription where keys are topic names and values are slices of ConsumerThreadIds used to fetch these topics.
func (tc *WildcardTopicsToNumStreams) GetConsumerThreadIdsPerTopic() map[string][]ConsumerThreadId {
	topicsToNumStreams := make(map[string]int)
	topics, err := tc.Coordinator.GetAllTopics()
	if err != nil {
		panic(err)
	}
	for _, topic := range topics {
		if tc.TopicFilter.TopicAllowed(topic, tc.ExcludeInternalTopics) {
			topicsToNumStreams[topic] = tc.NumStreams
		}
	}
	return makeConsumerThreadIdsPerTopic(tc.ConsumerId, topicsToNumStreams)
}

//Creates a map descibing consumer subscription where keys are topic names and values are number of fetchers used to fetch these topics.
func (tc *WildcardTopicsToNumStreams) GetTopicsToNumStreamsMap() map[string]int {
	result := make(map[string]int)
	result[tc.TopicFilter.Regex()] = tc.NumStreams
	return result
}

//Returns a pattern describing this TopicsToNumStreams.
func (tc *WildcardTopicsToNumStreams) Pattern() string {
	switch tc.TopicFilter.(type) {
	case *WhiteList:
		return whiteListPattern
	case *BlackList:
		return blackListPattern
	default:
		panic("unknown topic filter")
	}
}
