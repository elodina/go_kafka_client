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
	"github.com/Shopify/sarama"
)

//ConsumerInfo patterns
//TODO any other patterns?
var (
	WhiteListPattern = "white_list"
	BlackListPattern = "black_list"
	StaticPattern    = "static"
	SwitchToPatternPrefix = "switch_to_"
)

type BrokerInfo struct {
	Version int16
	Id      int32
	Host    string
	Port    uint32
}

func (b *BrokerInfo) String() string {
	return fmt.Sprintf("{Version: %d, Id: %d, Host: %s, Port: %d}",
						b.Version, b.Id, b.Host, b.Port)
}

type ConsumerInfo struct {
	Version   int16
	Subscription map[string]int
	Pattern   string
	Timestamp int64
}

func (c *ConsumerInfo) String() string {
	return fmt.Sprintf("{Version: %d, Subscription: %v, Pattern: %s, Timestamp: %d}",
						c.Version, c.Subscription, c.Pattern, c.Timestamp)
}

type TopicInfo struct {
	Version int16
	Partitions map[string][]int32
}

func (t *TopicInfo) String() string {
	return fmt.Sprintf("{Version: %d, Partitions: %v}",
						t.Version, t.Partitions)
}

type TopicsToNumStreams interface {
	GetTopicsToNumStreamsMap() map[string]int
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
func (c *ConsumerThreadId) String() string {
	return fmt.Sprintf("%s-%d", c.Consumer, c.ThreadId)
}

type TopicFilter interface {
	Regex() string
	IsTopicAllowed(topic string, excludeInternalTopics bool) bool
}

type TopicAndPartition struct {
	Topic string
	Partition int32
}

func (tp *TopicAndPartition) String() string {
	return fmt.Sprintf("{Topic: %s, Partition: %d}", tp.Topic, tp.Partition)
}

type SharedBlockChannel struct {
	chunks chan *TopicPartitionData
	closed bool
}

type PartitionTopicInfo struct {
	Topic string
	Partition int32
	Accumulator *BatchAccumulator
	ConsumedOffset int64
	FetchedOffset int64
	FetchSize int
	ClientId string
}

func (p *PartitionTopicInfo) String() string {
	return fmt.Sprintf("{Topic: %s, Partition: %d, ConsumedOffset: %d, FetchedOffset: %d, FetchSize: %d, ClientId: %s}",
						p.Topic, p.Partition, p.ConsumedOffset, p.FetchedOffset, p.FetchSize, p.ClientId)
}

type BrokerAndInitialOffset struct {
	Broker *BrokerInfo
	InitOffset int64
}

func (b *BrokerAndInitialOffset) String() string {
	return fmt.Sprintf("{Broker: %s, InitialOffset: %d}", b.Broker, b.InitOffset)
}

type BrokerAndFetcherId struct {
	Broker *BrokerInfo
	FetcherId int
}

func (b *BrokerAndFetcherId) String() string {
	return fmt.Sprintf("{Broker: %s, FetcherId: %d}", b.Broker, b.FetcherId)
}

type TopicAndThreadId struct {
	Topic string
	ThreadId *ConsumerThreadId
}

func (tt *TopicAndThreadId) String() string {
	return fmt.Sprintf("{Topic: %s, ThreadId: %s}", tt.Topic, tt.ThreadId)
}

type PartitionFetchInfo struct {
	Offset int64
	FetchSize int32
}

func (p *PartitionFetchInfo) String() string {
	return fmt.Sprintf("{Offset: %d, FetchSize: %d}", p.Offset, p.FetchSize)
}

type TopicPartitionData struct {
	TopicPartition TopicAndPartition
	Data *sarama.FetchResponseBlock
}

type WorkerManagerAndNextBatchChannel struct {
	WorkerMgr *WorkerManager
	AskNextBatch chan bool
}

type intArray []int32
func (s intArray) Len() int { return len(s) }
func (s intArray) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s intArray) Less(i, j int) bool { return s[i] < s[j] }
