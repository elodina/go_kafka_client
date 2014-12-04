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
	//Creates a map descibing consumer subscription where keys are topic names and values are number of fetchers used to fetch these topics.
	GetTopicsToNumStreamsMap() map[string]int

	//Creates a map describing consumer subscription where keys are topic names and values are slices of ConsumerThreadIds used to fetch these topics.
	GetConsumerThreadIdsPerTopic() map[string][]ConsumerThreadId

	//Returns a pattern describing this TopicsToNumStreams.
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

type DeployedTopics struct {
	//comma separated list of topics to consume from
	Topics string
	//either black_list, white_list or static
	Pattern string
}

type intArray []int32
func (s intArray) Len() int { return len(s) }
func (s intArray) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s intArray) Less(i, j int) bool { return s[i] < s[j] }

/* ConsumerCoordinator is used to coordinate actions of multiple consumers within the same consumer group.
It is responsible for keeping track of alive consumers, manages their offsets and assigns partitions to consume.
The current default ConsumerCoordinator is ZookeeperCoordinator. More of them can be added in future. */
type ConsumerCoordinator interface {
	/* Establish connection to this ConsumerCoordinator. Returns an error if fails to connect, nil otherwise. */
	Connect() error

	/* Registers a new consumer with Consumerid id and TopicCount subscription that is a part of consumer group Group in this ConsumerCoordinator. Returns an error if registration failed, nil otherwise. */
	RegisterConsumer(Consumerid string, Group string, TopicCount TopicsToNumStreams) error

	/* Deregisters consumer with Consumerid id that is a part of consumer group Group form this ConsumerCoordinator. Returns an error if deregistration failed, nil otherwise. */
	DeregisterConsumer(Consumerid string, Group string) error

	/* Gets the information about consumer with Consumerid id that is a part of consumer group Group from this ConsumerCoordinator.
	Returns ConsumerInfo on success and error otherwise (For example if consumer with given Consumerid does not exist). */
	GetConsumerInfo(Consumerid string, Group string) (*ConsumerInfo, error)

	/* Gets the information about consumers per topic in consumer group Group excluding internal topics (such as offsets) if ExcludeInternalTopics = true.
	Returns a map where keys are topic names and values are slices of consumer ids and fetcher ids associated with this topic and error on failure. */
	GetConsumersPerTopic(Group string, ExcludeInternalTopics bool) (map[string][]ConsumerThreadId, error)

	/* Gets the list of all consumer ids within a consumer group Group. Returns a slice containing all consumer ids in group and error on failure. */
	GetConsumersInGroup(Group string) ([]string, error)

	/* Gets the list of all topics registered in this ConsumerCoordinator. Returns a slice conaining topic names and error on failure. */
	GetAllTopics() ([]string, error)

	/* Gets the information about existing partitions for a given Topics.
	Returns a map where keys are topic names and values are slices of partition ids associated with this topic and error on failure. */
	GetPartitionsForTopics(Topics []string) (map[string][]int32, error)

	/* Gets the information about all Kafka brokers registered in this ConsumerCoordinator.
	Returns a slice of BrokerInfo and error on failure. */
	GetAllBrokers() ([]*BrokerInfo, error)

	/* Gets the offset for a given TopicPartition and consumer group Group.
	Returns offset on sucess, error otherwise. */
	GetOffsetForTopicPartition(Group string, TopicPartition *TopicAndPartition) (int64, error)

	//TODO not sure if we still need this
	NotifyConsumerGroup(Group string, ConsumerId string) error

	/* Subscribes for any change that should trigger consumer rebalance on consumer group Group in this ConsumerCoordinator or trigger topic switch.
	Returns a read-only channel of CoordinatorEvent that will get values on any significant coordinator event (e.g. new consumer appeared, new broker appeared etc.) and error if failed to subscribe. */
	SubscribeForChanges(Group string) (<-chan CoordinatorEvent, error)

	GetNewDeployedTopics(Group string) ([]*DeployedTopics, error)

	/* Tells the ConsumerCoordinator to unsubscribe from events for the consumer it is associated with. */
	Unsubscribe()

	/* Tells the ConsumerCoordinator to claim partition topic Topic and partition Partition for ConsumerThreadId fetcher that works within a consumer group Group.
	Returns true if claim is successful, false and error explaining failure otherwise. */
	ClaimPartitionOwnership(Group string, Topic string, Partition int32, ConsumerThreadId ConsumerThreadId) (bool, error)

	/* Tells the ConsumerCoordinator to release partition ownership on topic Topic and partition Partition for consumer group Group.
	Returns error if failed to released partition ownership. */
	ReleasePartitionOwnership(Group string, Topic string, Partition int32) error

	/* Tells the ConsumerCoordinator to commit offset Offset for topic and partition TopicPartition for consumer group Group.
	Returns error if failed to commit offset. */
	CommitOffset(Group string, TopicPartition *TopicAndPartition, Offset int64) error
}

type CoordinatorEvent string

const (
	Regular = "Regular"
	NewTopicDeployed = "NewTopicDeployed"
)
