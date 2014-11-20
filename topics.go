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
	"fmt"
	"strings"
)

func NewTopicsToNumStreams(group string, consumerId string, zkConnection *zk.Conn, excludeInternalTopics bool) (TopicsToNumStreams, error) {
	consumerInfo, err := GetConsumer(zkConnection, group, consumerId)
	if (err != nil) {
		return nil, err
	}

	hasTopicSwitch := strings.HasPrefix(consumerInfo.Pattern, SwitchToPatternPrefix)
	if (hasTopicSwitch) {
		var pattern string
		switch consumerInfo.Pattern {
		case fmt.Sprintf("%s%s", SwitchToPatternPrefix, WhiteListPattern):
			pattern = WhiteListPattern
		case fmt.Sprintf("%s%s", SwitchToPatternPrefix, BlackListPattern):
			pattern = BlackListPattern
		default:
			pattern = StaticPattern
		}
		return &TopicSwitch{
			ConsumerId: consumerId,
			DesiredPattern: pattern,
			TopicsToNumStreamsMap: consumerInfo.Subscription,
		}, nil
	}

	hasWhiteList := WhiteListPattern == consumerInfo.Pattern
	hasBlackList := BlackListPattern == consumerInfo.Pattern

	if (len(consumerInfo.Subscription) == 0 || !(hasWhiteList || hasBlackList)) {
		return &StaticTopicsToNumStreams{
			ConsumerId: consumerId,
			TopicsToNumStreamsMap: consumerInfo.Subscription,
		}, nil
	} else {
		var regex string
		var numStreams int
		for regex, numStreams = range consumerInfo.Subscription { break }
		var filter TopicFilter
		if (hasWhiteList) {
			filter = NewWhiteList(regex)
		} else {
			filter = NewBlackList(regex)
		}

		return &WildcardTopicsToNumStreams{
			ZkConnection: zkConnection,
			ConsumerId: consumerId,
			TopicFilter: filter,
			NumStreams: numStreams,
			ExcludeInternalTopics: excludeInternalTopics,
		}, nil
	}
}

func makeConsumerThreadIdsPerTopic(consumerId string, TopicsToNumStreamsMap map[string]int) map[string][]ConsumerThreadId {
	result := make(map[string][]ConsumerThreadId)
	for topic, numConsumers := range TopicsToNumStreamsMap {
		consumers := make([]ConsumerThreadId, numConsumers)
		if (numConsumers < 1) {
			panic("Number of consumers should be greater than 0")
		}
		for i := 0; i < numConsumers; i++ {
			consumerThreadId := ConsumerThreadId{consumerId, i}
			exists := false
			for i := 0; i < numConsumers; i++ {
				if (consumers[i] == consumerThreadId) {
					exists = true
					break
				}
			}
			if (!exists) {
				consumers[i] = consumerThreadId
			}
		}
		result[topic] = consumers[:len(consumers)]
	}

	return result
}


type StaticTopicsToNumStreams struct {
	ConsumerId string
	TopicsToNumStreamsMap map[string]int
}

func (tc *StaticTopicsToNumStreams) GetConsumerThreadIdsPerTopic() map[string][]ConsumerThreadId {
	return makeConsumerThreadIdsPerTopic(tc.ConsumerId, tc.TopicsToNumStreamsMap)
}

func (tc *StaticTopicsToNumStreams) GetTopicsToNumStreamsMap() map[string]int {
	return tc.TopicsToNumStreamsMap
}

func (tc *StaticTopicsToNumStreams) Pattern() string {
	return StaticPattern
}


type WildcardTopicsToNumStreams struct {
	ZkConnection *zk.Conn
	ConsumerId            string
	TopicFilter           TopicFilter
	NumStreams            int
	ExcludeInternalTopics bool
}

func (tc *WildcardTopicsToNumStreams) GetConsumerThreadIdsPerTopic() map[string][]ConsumerThreadId {
	topicsToNumStreams := make(map[string]int)
	topics, err := GetTopics(tc.ZkConnection)
	if (err != nil) {
		panic(err)
	}
	for _, topic := range topics {
		if (tc.TopicFilter.IsTopicAllowed(topic, tc.ExcludeInternalTopics)) {
			topicsToNumStreams[topic] = tc.NumStreams
		}
	}
	return makeConsumerThreadIdsPerTopic(tc.ConsumerId, topicsToNumStreams)
}

func (tc *WildcardTopicsToNumStreams) GetTopicsToNumStreamsMap() map[string]int {
	result := make(map[string]int)
	result[tc.TopicFilter.Regex()] = tc.NumStreams
	return result
}

func (tc *WildcardTopicsToNumStreams) Pattern() string {
	switch tc.TopicFilter.(type) {
	case *WhiteList:
		return WhiteListPattern
	case *BlackList:
		return BlackListPattern
	default:
		panic("unknown topic filter")
	}
}


type TopicSwitch struct {
	ConsumerId string
	DesiredPattern string
	TopicsToNumStreamsMap map[string]int
}

func (tc *TopicSwitch) GetConsumerThreadIdsPerTopic() map[string][]ConsumerThreadId {
	return makeConsumerThreadIdsPerTopic(tc.ConsumerId, tc.TopicsToNumStreamsMap)
}

func (tc *TopicSwitch) GetTopicsToNumStreamsMap() map[string]int {
	return tc.TopicsToNumStreamsMap
}

func (tc *TopicSwitch) Pattern() string {
	return tc.DesiredPattern
}
