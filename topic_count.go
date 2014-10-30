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

func NewTopicCount(group string, consumerId string, zkConnection *zk.Conn, excludeInternalTopics bool) (TopicCount, error) {
	consumerInfo, err := GetConsumer(zkConnection, group, consumerId)
	if (err != nil) {
		return nil, err
	}

	hasWhiteList := WhiteListPattern == consumerInfo.Pattern
	hasBlackList := WhiteListPattern == consumerInfo.Pattern

	if (len(consumerInfo.Subscription) == 0 || !(hasWhiteList || hasBlackList)) {
		return &StaticTopicCount{
			ConsumerId: consumerId,
			TopicCountMap: &consumerInfo.Subscription,
		}, nil
	} else {
		regex := ""
		numStreams := 0
		for k, v := range consumerInfo.Subscription {
			regex = k
			numStreams = v
			break
		}
		var filter TopicFilter
		if (hasWhiteList) {
			filter = &WhiteList{
				RawRegex: regex,
			}
		} else {
			filter = &BlackList{
				RawRegex: regex,
			}
		}

		return &WildcardTopicCount{
			ZkConnection: zkConnection,
			ConsumerId: consumerId,
			TopicFilter: filter,
			NumStreams: numStreams,
			ExcludeInternalTopics: excludeInternalTopics,
		}, nil
	}
}

type StaticTopicCount struct {
	ConsumerId string
	TopicCountMap *map[string]int
}

func (tc *StaticTopicCount) GetConsumerThreadIdsPerTopic() map[string][]ConsumerThreadId {
	return make(map[string][]ConsumerThreadId)
}

func (tc *StaticTopicCount) GetTopicCountMap() *map[string]int {
	return tc.TopicCountMap
}

func (tc *StaticTopicCount) Pattern() string {
	return StaticPattern
}

type WildcardTopicCount struct {
	ZkConnection *zk.Conn
	ConsumerId string
	TopicFilter TopicFilter
	NumStreams int
	ExcludeInternalTopics bool
}


func (tc *WildcardTopicCount) GetConsumerThreadIdsPerTopic() map[string][]ConsumerThreadId {
	return make(map[string][]ConsumerThreadId)
}

func (tc *WildcardTopicCount) GetTopicCountMap() map[string]int {
	return make(map[string]int)
}

func (tc *WildcardTopicCount) Pattern() string {
	switch tc.TopicFilter.(type) {
	case *WhiteList:
		return WhiteListPattern
	case *BlackList:
		return BlackListPattern
	default:
		panic("unknown topic filter")
	}
}
