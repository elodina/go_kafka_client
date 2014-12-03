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

const (
	whiteListPattern = "white_list"
	blackListPattern = "black_list"
	staticPattern    = "static"
	switchToPatternPrefix = "switch_to_"
)

//Single Kafka message that is sent to user-defined Strategy
type Message struct {
	Key       []byte
	Value     []byte
	Topic     string
	Partition int32
	Offset    int64
}

func (m *Message) String() string {
	return fmt.Sprintf("Message{Topic: %s, Partition: %d, Offset: %d}", m.Topic, m.Partition, m.Offset)
}

//General information about Kafka broker. Used to keep it in consumer coordinator.
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

//General information about Kafka consumer. Used to keep it in consumer coordinator.
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

//General information about Kafka topic. Used to keep it in consumer coordinator.
type TopicInfo struct {
	Version int16
	Partitions map[string][]int32
}

func (t *TopicInfo) String() string {
	return fmt.Sprintf("{Version: %d, Partitions: %v}",
						t.Version, t.Partitions)
}

//Information on Consumer subscription. Used to keep it in consumer coordinator.
type TopicsToNumStreams interface {
	GetTopicsToNumStreamsMap() map[string]int
	GetConsumerThreadIdsPerTopic() map[string][]ConsumerThreadId
	Pattern() string
}

//Consumer routine id. Used to keep track of what consumer routine consumes a particular topic-partition in consumer coordinator.
type ConsumerThreadId struct {
	Consumer string
	ThreadId int
}

func (c *ConsumerThreadId) String() string {
	return fmt.Sprintf("%s-%d", c.Consumer, c.ThreadId)
}

type byName []ConsumerThreadId

func (a byName) Len() int { return len(a) }
func (a byName) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a byName) Less(i, j int) bool {
	this := fmt.Sprintf("%s-%d", a[i].Consumer, a[i].ThreadId)
	that := fmt.Sprintf("%s-%d", a[j].Consumer, a[j].ThreadId)
	return this < that
}

//Either a WhiteList or BlackList consumer topic filter.
type TopicFilter interface {
	regex() string
	topicAllowed(topic string, excludeInternalTopics bool) bool
}

//Type representing a single Kafka topic and partition
type TopicAndPartition struct {
	Topic string
	Partition int32
}

func (tp *TopicAndPartition) String() string {
	return fmt.Sprintf("{Topic: %s, Partition: %d}", tp.Topic, tp.Partition)
}

type partitionTopicInfo struct {
	Topic string
	Partition int32
	Buffer *messageBuffer
	FetchedOffset int64
}

func (p *partitionTopicInfo) String() string {
	return fmt.Sprintf("{Topic: %s, Partition: %d, FetchedOffset: %d, Buffer: %s}",
						p.Topic, p.Partition, p.FetchedOffset, p.Buffer)
}

type brokerAndInitialOffset struct {
	Broker *BrokerInfo
	InitOffset int64
}

func (b *brokerAndInitialOffset) String() string {
	return fmt.Sprintf("{Broker: %s, InitialOffset: %d}", b.Broker, b.InitOffset)
}

type brokerAndFetcherId struct {
	Broker *BrokerInfo
	FetcherId int
}

func (b *brokerAndFetcherId) String() string {
	return fmt.Sprintf("{Broker: %s, FetcherId: %d}", b.Broker, b.FetcherId)
}

type partitionFetchInfo struct {
	Offset int64
	FetchSize int32
}

func (p *partitionFetchInfo) String() string {
	return fmt.Sprintf("{Offset: %d, FetchSize: %d}", p.Offset, p.FetchSize)
}

//Fetched data from Kafka broker for a particular topic and partition
type TopicPartitionData struct {
	TopicPartition TopicAndPartition
	Data *sarama.FetchResponseBlock
}

type intArray []int32
func (s intArray) Len() int { return len(s) }
func (s intArray) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s intArray) Less(i, j int) bool { return s[i] < s[j] }

//TODO document this!
type ConsumerCoordinator interface {
	Connect() error
	RegisterConsumer(consumerid string, group string, topicCount TopicsToNumStreams) error
	DeregisterConsumer(consumerid string, group string) error
	GetConsumerInfo(consumerid string, group string) (*ConsumerInfo, error)
	GetConsumersPerTopic(group string, excludeInternalTopics bool) (map[string][]ConsumerThreadId, error)
	GetConsumersInGroup(group string) ([]string, error)
	GetAllTopics() ([]string, error)
	GetPartitionsForTopics(topics []string) (map[string][]int32, error)
	GetAllBrokers() ([]*BrokerInfo, error)
	GetOffsetForTopicPartition(group string, topicPartition *TopicAndPartition) (int64, error)
	NotifyConsumerGroup(group string, consumerId string) error
	SubscribeForChanges(group string) (<-chan bool, error)
	Unsubscribe()
	ClaimPartitionOwnership(group string, topic string, partition int32, consumerThreadId ConsumerThreadId) (bool, error)
	ReleasePartitionOwnership(group string, topic string, partition int32) error
	CommitOffset(group string, topicPartition *TopicAndPartition, offset int64) error
}
