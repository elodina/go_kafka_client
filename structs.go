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

import (
	"fmt"
	"time"
)

const (
	whiteListPattern = "white_list"
	blackListPattern = "black_list"
	staticPattern    = "static"
)

//Single Kafka message that is sent to user-defined Strategy
type Message struct {
	// Partition key.
	Key []byte
	// Message value.
	Value []byte
	// Decoded message key
	DecodedKey interface{}
	// Decoded message value
	DecodedValue interface{}
	// Topic this message came from.
	Topic string

	// Partition this message came from.
	Partition int32

	// Message offset.
	Offset int64

	// HighwaterMarkOffset is an offset of the last message in this topic-partition.
	HighwaterMarkOffset int64
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

type byId []*BrokerInfo

func (a byId) Len() int           { return len(a) }
func (a byId) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byId) Less(i, j int) bool { return a[i].Id < a[j].Id }

//General information about Kafka consumer. Used to keep it in consumer coordinator.
type ConsumerInfo struct {
	Version      int16          `json:"version"`
	Subscription map[string]int `json:"subscription"`
	Pattern      string         `json:"pattern"`
	Timestamp    int64          `json:"timestamp,string"`
}

func (c *ConsumerInfo) String() string {
	return fmt.Sprintf("{Version: %d, Subscription: %v, Pattern: %s, Timestamp: %d}",
		c.Version, c.Subscription, c.Pattern, c.Timestamp)
}

//General information about Kafka topic. Used to keep it in consumer coordinator.
type TopicInfo struct {
	Version    int16
	Partitions map[string][]int32
}

func (t *TopicInfo) String() string {
	return fmt.Sprintf("{Version: %d, Partitions: %v}",
		t.Version, t.Partitions)
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

func (a byName) Len() int      { return len(a) }
func (a byName) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a byName) Less(i, j int) bool {
	this := fmt.Sprintf("%s-%d", a[i].Consumer, a[i].ThreadId)
	that := fmt.Sprintf("%s-%d", a[j].Consumer, a[j].ThreadId)
	return this < that
}

//Either a WhiteList or BlackList consumer topic filter.
type TopicFilter interface {
	Regex() string
	TopicAllowed(topic string, excludeInternalTopics bool) bool
}

//Type representing a single Kafka topic and partition
type TopicAndPartition struct {
	Topic     string
	Partition int32
}

func (tp *TopicAndPartition) String() string {
	return fmt.Sprintf("{Topic: %s, Partition: %d}", tp.Topic, tp.Partition)
}

type partitionTopicInfo struct {
	Topic         string
	Partition     int32
	Buffer        *messageBuffer
	FetchedOffset int64
}

func (p *partitionTopicInfo) String() string {
	return fmt.Sprintf("{Topic: %s, Partition: %d, FetchedOffset: %d, Buffer: %s}",
		p.Topic, p.Partition, p.FetchedOffset, p.Buffer)
}

type intArray []int32

func (s intArray) Len() int           { return len(s) }
func (s intArray) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s intArray) Less(i, j int) bool { return s[i] < s[j] }

// ConsumerCoordinator is used to coordinate actions of multiple consumers within the same consumer group.
// It is responsible for keeping track of alive consumers, manages their offsets and assigns partitions to consume.
// The current default ConsumerCoordinator is ZookeeperCoordinator. More of them can be added in future.
type ConsumerCoordinator interface {
	/* Establish connection to this ConsumerCoordinator. Returns an error if fails to connect, nil otherwise. */
	Connect() error

	/* Close connection to this ConsumerCoordinator. */
	Disconnect()

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

	/* Subscribes for any change that should trigger consumer rebalance on consumer group Group in this ConsumerCoordinator or trigger topic switch.
	Returns a read-only channel of CoordinatorEvent that will get values on any significant coordinator event (e.g. new consumer appeared, new broker appeared etc.) and error if failed to subscribe. */
	SubscribeForChanges(Group string) (<-chan CoordinatorEvent, error)

	/* Requests that a blue/green deployment be done.*/
	RequestBlueGreenDeployment(blue BlueGreenDeployment, green BlueGreenDeployment) error

	/* Gets all deployed topics for consume group Group from consumer coordinator.
	Returns a map where keys are notification ids and values are DeployedTopics. May also return an error (e.g. if failed to reach coordinator). */
	GetBlueGreenRequest(Group string) (map[string]*BlueGreenDeployment, error)

	/* Implements classic barrier synchronization primitive via service coordinator facilities */
	AwaitOnStateBarrier(consumerId string, group string, stateHash string, barrierSize int, api string, timeout time.Duration) bool

	/* Removes state barrier */
	RemoveStateBarrier(group string, stateHash string, api string) error

	/* Tells the ConsumerCoordinator to unsubscribe from events for the consumer it is associated with. */
	Unsubscribe()

	/* Tells the ConsumerCoordinator to claim partition topic Topic and partition Partition for ConsumerThreadId fetcher that works within a consumer group Group.
	Returns true if claim is successful, false and error explaining failure otherwise. */
	ClaimPartitionOwnership(Group string, Topic string, Partition int32, ConsumerThreadId ConsumerThreadId) (bool, error)

	/* Tells the ConsumerCoordinator to release partition ownership on topic Topic and partition Partition for consumer group Group.
	Returns error if failed to released partition ownership. */
	ReleasePartitionOwnership(Group string, Topic string, Partition int32) error

	/* Removes old api objects */
	RemoveOldApiRequests(group string) error
}

// CoordinatorEvent is sent by consumer coordinator representing some state change.
type CoordinatorEvent string

const (
	// A regular coordinator event that should normally trigger consumer rebalance.
	Regular CoordinatorEvent = "Regular"

	// Coordinator event that should trigger consumer re-registrer
	Reinitialize CoordinatorEvent = "Reinitialize"

	// A coordinator event that informs a consumer group of new deployed topics.
	BlueGreenRequest CoordinatorEvent = "BlueGreenRequest"
)

// OffsetStorage is used to store and retrieve consumer offsets.
type OffsetStorage interface {
	// Gets the offset for a given group, topic and partition.
	// May return an error if fails to retrieve the offset.
	GetOffset(group string, topic string, partition int32) (int64, error)

	// Commits the given offset for a given group, topic and partition.
	// May return an error if fails to commit the offset.
	CommitOffset(group string, topic string, partition int32, offset int64) error
}

// Represents a consumer state snapshot.
type StateSnapshot struct {
	// Metrics are a map where keys are event names and values are maps holding event values grouped by meters (count, min, max, etc.).
	Metrics map[string]map[string]float64
	// Offsets are a map where keys are topics and values are maps where keys are partitions and values are offsets for these topic-partitions.
	Offsets map[string]map[int32]int64
}

type FailedMessage struct {
	message *ProducerMessage
	err     error
}
