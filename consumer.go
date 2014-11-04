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
	"time"
	"github.com/samuel/go-zookeeper/zk"
	"os"
	"os/signal"
	"sync"
	"fmt"
	"github.com/Shopify/sarama"
)

type Consumer struct {
	config        *ConsumerConfig
	topic         string
	group         string
	zookeeper     []string
	fetcher         *consumerFetcherManager
	messages      chan *Message
	unsubscribe   chan bool
	closeFinished chan bool
	zkConn          *zk.Conn
	rebalanceLock sync.Mutex
	isShuttingdown bool
	topicThreadIdsAndChannels map[*TopicAndThreadId]chan *sarama.FetchResponseBlock
	topicRegistry map[string]map[int]*PartitionTopicInfo
	checkPointedZkOffsets map[*TopicAndPartition]int64
}

type Message struct {
	Key       []byte
	Value     []byte
	Topic     string
	Partition int32
	Offset    int64
}

func NewConsumer(config *ConsumerConfig) *Consumer {
	c := &Consumer{
		config : config,
		//		topic : topic,
		//		group : group,
		//		zookeeper : zookeeper,
		messages : make(chan *Message),
		unsubscribe : make(chan bool),
		closeFinished : make(chan bool),
		topicThreadIdsAndChannels : make(map[*TopicAndThreadId]chan *sarama.FetchResponseBlock),
	}

	c.addShutdownHook()

	c.connectToZookeeper()
	//	c.registerInZookeeper()
	c.fetcher = newConsumerFetcherManager(config, c.zkConn, c.messages)

	return c
}

func (c *Consumer) Messages() <-chan *Message {
	return c.messages
}

func (c *Consumer) SwitchTopic(newTopic string) {
	c.fetcher.SwitchTopic(newTopic)
}

func (c *Consumer) Close() <-chan bool {
	Logger.Println("Closing consumer")
	go func() {
		<-c.fetcher.Close()
		c.unsubscribe <- true
		err := DeregisterConsumer(c.zkConn, c.group, c.config.ConsumerId)
		if err != nil {
			panic(err)
		}
		c.closeFinished <- true
	}()
	return c.closeFinished
}

func (c *Consumer) Ack(offset int64, topic string, partition int32) error {
	Logger.Printf("Acking offset %d for topic %s and partition %d", offset, topic, partition)
	return nil
}

func (c *Consumer) addShutdownHook() {
	s := make(chan os.Signal, 1)
	signal.Notify(s, os.Interrupt)
	go func() {
		<-s
		c.Close()
	}()
}

func (c *Consumer) connectToZookeeper() {
	Logger.Println("Connecting to zk")
	if conn, _, err := zk.Connect(c.config.ZookeeperConnect, c.config.ZookeeperTimeout); err != nil {
		panic(err)
	} else {
		c.zkConn = conn
	}
}

func (c *Consumer) registerInZookeeper() {
	Logger.Println("Registering in zk")
	subscription := make(map[string]int)
	subscription[c.topic] = 1

	consumerInfo := &ConsumerInfo{
		Version : int16(1),
		Subscription : subscription,
		Pattern : WhiteListPattern,
		Timestamp : time.Now().Unix(),
	}

	if err := RegisterConsumer(c.zkConn, c.group, c.config.ConsumerId, consumerInfo); err != nil {
		panic(err)
	}

	c.subscribeForChanges(c.group)
}

func (c *Consumer) subscribeForChanges(group string) {
	Logger.Println("Subscribing for changes for", NewZKGroupDirs(group).ConsumerRegistryDir)

	consumersWatcher, err := GetConsumersInGroupWatcher(c.zkConn, group)
	if err != nil {
		panic(err)
	}
	topicsWatcher, err := GetTopicsWatcher(c.zkConn)
	if err != nil {
		panic(err)
	}
	brokersWatcher, err := GetAllBrokersInClusterWatcher(c.zkConn)
	if err != nil {
		panic(err)
	}

	go func() {
		for {
			select {
			case e := <-topicsWatcher: {
				InLock(&c.rebalanceLock, func() { triggerRebalanceIfNeeded(e, c) })
			}
			case e := <-consumersWatcher: {
				InLock(&c.rebalanceLock, func() { triggerRebalanceIfNeeded(e, c) })
			}
			case e := <-brokersWatcher: {
				InLock(&c.rebalanceLock, func() { triggerRebalanceIfNeeded(e, c) })
			}
			case <-c.unsubscribe: {
				Logger.Println("Unsubscribing from changes")
				break
			}
			}
		}
	}()
}

func triggerRebalanceIfNeeded(e zk.Event, c *Consumer) {
	emptyEvent := zk.Event{}
	if e != emptyEvent {
		c.rebalance(e)
	} else {
		time.Sleep(2 * time.Second)
	}
}

func (c *Consumer) rebalance(_ zk.Event) {
	partitionAssignor := NewPartitionAssignor(c.config.PartitionAssignmentStrategy)
	if (!c.isShuttingdown) {
		Logger.Printf("rebalance triggered for %s\n", c.config.ConsumerId)
		var success = false
		var err error
		for i := 0; i < int(c.config.RebalanceMaxRetries); i++ {
			topicPerThreadIdsMap, err := NewTopicsToNumStreams(c.group, c.config.ConsumerId, c.zkConn, c.config.ExcludeInternalTopics)
			if (err != nil) {
				Logger.Println(err)
				time.Sleep(time.Millisecond * c.config.RebalanceBackoffMs)
				continue
			}
			Logger.Printf("%v\n", topicPerThreadIdsMap)

			brokers, err := GetAllBrokersInCluster(c.zkConn)
			if (err != nil) {
				Logger.Println(err)
				time.Sleep(time.Millisecond * c.config.RebalanceBackoffMs)
				continue
			}
			Logger.Printf("%v\n", brokers)

			//TODO: close fetchers
			c.releasePartitionOwnership(c.topicRegistry)

			assignmentContext := NewAssignmentContext(c.config.Groupid, c.config.ConsumerId, c.config.ExcludeInternalTopics, c.zkConn)
			partitionOwnershipDecision := partitionAssignor(assignmentContext)
			topicPartitions := make([]*TopicAndPartition, len(partitionOwnershipDecision))
			for topicPartition, _ := range partitionOwnershipDecision {
				topicPartitions = append(topicPartitions, &topicPartition)
			}

			offsetsFetchResponse, err := c.fetchOffsets(topicPartitions)
			if (err != nil) {
				break
			}

			currentTopicRegistry := make(map[string]map[int]*PartitionTopicInfo)

			if (c.isShuttingdown) {
				Logger.Printf("Aborting consumer '%s' rebalancing, since shutdown sequence started.\n", c.config.ConsumerId)
				return
			} else {
				for _, topicPartition := range topicPartitions {
					offset := offsetsFetchResponse.Blocks[topicPartition.Topic][int32(topicPartition.Partition)].Offset
					threadId := partitionOwnershipDecision[*topicPartition]
					c.addPartitionTopicInfo(currentTopicRegistry, topicPartition, offset, threadId)
				}
			}

			if (c.reflectPartitionOwnershipDecision(partitionOwnershipDecision)) {
				c.topicRegistry = currentTopicRegistry
				//TODO: update fetcher
			} else {
				time.Sleep(time.Millisecond * c.config.RebalanceBackoffMs)
				continue
			}

			success = true
		}

		if (!success) {
			panic(err)
		}
	} else {
		Logger.Printf("Rebalance was triggered during consumer '%s' shutdown sequence. Ignoring...\n", c.config.ConsumerId)
	}
}

func (c *Consumer) fetchOffsets(topicPartitions []*TopicAndPartition) (*sarama.OffsetFetchResponse, error) {
	if (len(topicPartitions) == 0) {
		return &sarama.OffsetFetchResponse{}, nil
	} else {
		blocks := make(map[string]map[int32]*sarama.OffsetFetchResponseBlock)
		if (c.config.OffsetsStorage == "zookeeper") {
			for _, topicPartition := range topicPartitions {
				offset, err := GetOffsetForTopicPartition(c.zkConn, c.group, topicPartition)
				_, exists := blocks[topicPartition.Topic]
				if (!exists) {
					blocks[topicPartition.Topic] = make(map[int32]*sarama.OffsetFetchResponseBlock)
				}
				if (err != nil) {
					return nil, err
				} else {
					blocks[topicPartition.Topic][int32(topicPartition.Partition)] = &sarama.OffsetFetchResponseBlock {
						Offset: offset,
						Metadata: "",
						Err: sarama.NoError,
					}
				}
			}
		} else {
			panic(fmt.Sprintf("Offset storage '%s' is not supported", c.config.OffsetsStorage))
		}

		return &sarama.OffsetFetchResponse{ Blocks: blocks, }, nil
	}
}

func (c *Consumer) addPartitionTopicInfo(currentTopicRegistry map[string]map[int]*PartitionTopicInfo,
										 topicPartition *TopicAndPartition, offset int64,
										 consumerThreadId *ConsumerThreadId) {
	partTopicInfoMap, exists := currentTopicRegistry[topicPartition.Topic]
	if (!exists) {
		partTopicInfoMap = make(map[int]*PartitionTopicInfo)
		currentTopicRegistry[topicPartition.Topic] = partTopicInfoMap
	}

	//TODO: queue for specific threadId should be retrieved here and passed to partTopicInfo

	partTopicInfo := &PartitionTopicInfo{
		Topic: topicPartition.Topic,
		Partition: topicPartition.Partition,
		ConsumedOffset: offset,
		FetchedOffset: offset,
		FetchSize: int(c.config.FetchMessageMaxBytes),
		ClientId: c.config.ConsumerId,
	}

	partTopicInfoMap[topicPartition.Partition] = partTopicInfo
	c.checkPointedZkOffsets[topicPartition] = offset
}

func (c *Consumer) reflectPartitionOwnershipDecision(partitionOwnershipDecision map[TopicAndPartition]*ConsumerThreadId) bool {
	successfullyOwnedPartitions := make(map[string]int)
	for topicPartition, consumerThreadId := range partitionOwnershipDecision {
		success, err := ClaimPartitionOwnership(c.zkConn, c.group, topicPartition.Topic, topicPartition.Partition, consumerThreadId)
		if (err != nil) {
			panic(err)
		}
		if (success) {
			successfullyOwnedPartitions[topicPartition.Topic] = topicPartition.Partition
		}
	}

	if (len(partitionOwnershipDecision) > len(successfullyOwnedPartitions)) {
		for topic, partition := range successfullyOwnedPartitions {
			DeletePartitionOwnership(c.zkConn, c.group, topic, partition)
		}
		return false
	}

	return true
}

func (c *Consumer) releasePartitionOwnership(localTopicRegistry map[string]map[int]*PartitionTopicInfo) {
	Logger.Println("Releasing partition ownership")
	for topic, partitionInfos := range localTopicRegistry {
		for partition, _ := range partitionInfos {
			err := DeletePartitionOwnership(c.zkConn, c.group, topic, partition)
			if (err != nil) {
				panic(err)
			}
		}
		delete(localTopicRegistry, topic)
	}
}
