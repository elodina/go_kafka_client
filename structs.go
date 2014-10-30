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
	"fmt"
)

//ConsumerInfo patterns
//TODO any other patterns?
var (
	WhiteListPattern = "white_list"
	BlackListPattern = "black_list"
	StaticPattern    = "black_list"
)

type BrokerInfo struct {
	Version int16
	Id      int32
	Host    string
	Port    uint32
}

type ConsumerInfo struct {
	Version   int16
	Subscription map[string]int
	Pattern   string
	Timestamp int64
}

type TopicsToNumStreams interface {
	GetConsumerThreadIdsPerTopic() map[string][]*ConsumerThreadId
	Pattern() string
}

type ConsumerThreadId struct {
	Consumer string
	ThreadId int
}

type ByName []*ConsumerThreadId

func (a ByName) Len() int { return len(a) }
func (a ByName) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a ByName) Less(i, j int) bool {
	this := fmt.Sprintf("%s-%d", a[i].Consumer, a[i].ThreadId)
	that := fmt.Sprintf("%s-%d", a[j].Consumer, a[j].ThreadId)
	return this < that
}

type TopicFilter interface {
	Regex() string
	IsTopicAllowed(topic string, excludeInternalTopics bool) bool
}
