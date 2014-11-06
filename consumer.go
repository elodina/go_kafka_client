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
	"reflect"
)

var InvalidOffset int64 = -1

var SmallestOffset = "smallest"

type Consumer struct {
	config        *ConsumerConfig
	zookeeper      []string
	fetcher         *consumerFetcherManager
	messages       chan *Message
	unsubscribe    chan bool
	closeFinished  chan bool
	zkConn          *zk.Conn
	rebalanceLock  sync.Mutex
	isShuttingdown bool
	topicChannels map[string][]<-chan []*Message
	topicThreadIdsAndChannels map[TopicAndThreadId]chan *sarama.FetchResponseBlock
	topicRegistry map[string]map[int]*PartitionTopicInfo
	checkPointedZkOffsets map[*TopicAndPartition]int64
	closeChannels []chan bool
}

type Message struct {
	Key       []byte
	Value     []byte
	Topic     string
	Partition int32
	Offset    int64
}

func NewConsumer(config *ConsumerConfig) *Consumer {
	Infof(config.ConsumerId, "Starting new consumer with configuration: %s", config)
	c := &Consumer{
		config : config,
		messages : make(chan *Message),
		unsubscribe : make(chan bool, 1),
		closeFinished : make(chan bool),
		topicChannels : make(map[string][]<-chan []*Message),
		topicThreadIdsAndChannels : make(map[TopicAndThreadId]chan *sarama.FetchResponseBlock),
		topicRegistry: make(map[string]map[int]*PartitionTopicInfo),
		checkPointedZkOffsets: make(map[*TopicAndPartition]int64),
		closeChannels: make([]chan bool, 0),
	}

	c.addShutdownHook()

	c.connectToZookeeper()
	c.fetcher = newConsumerFetcherManager(config, c.zkConn, c.messages)

	return c
}

func (c *Consumer) String() string {
	return c.config.ConsumerId
}

func (c *Consumer) CreateMessageStreams(topicCountMap map[string]int) map[string][]<-chan []*Message {
	staticTopicCount := &StaticTopicsToNumStreams {
		ConsumerId : c.config.ConsumerId,
		TopicsToNumStreamsMap : topicCountMap,
	}

	var channelsAndStreams []*ChannelAndStream = nil
	for _, threadIdSet := range staticTopicCount.GetConsumerThreadIdsPerTopic() {
		channelsAndStreamsForThread := make([]*ChannelAndStream, len(threadIdSet))
		for i := 0; i < len(channelsAndStreamsForThread); i++ {
			closeChannel := make(chan bool, 1)
			c.closeChannels = append(c.closeChannels, closeChannel)
			channelsAndStreamsForThread[i] = NewChannelAndStream(c.config, closeChannel)
		}
		channelsAndStreams = append(channelsAndStreams, channelsAndStreamsForThread...)
	}

	RegisterConsumer(c.zkConn, c.config.Groupid, c.config.ConsumerId, &ConsumerInfo{
			Version : int16(1),
			Subscription : staticTopicCount.GetTopicsToNumStreamsMap(),
			Pattern : staticTopicCount.Pattern(),
			Timestamp : time.Now().Unix(),
		})

	c.ReinitializeConsumer(staticTopicCount, channelsAndStreams)

	return c.topicChannels
}

func (c *Consumer) ReinitializeConsumer(topicCount TopicsToNumStreams, channelsAndStreams []*ChannelAndStream) {
	consumerThreadIdsPerTopic := topicCount.GetConsumerThreadIdsPerTopic()

	//TODO wildcard handling
	allChannelsAndStreams := channelsAndStreams
	topicThreadIds := make([]TopicAndThreadId, 0)
	for topic, threadIds := range consumerThreadIdsPerTopic {
		for _, threadId := range threadIds {
			topicThreadIds = append(topicThreadIds, TopicAndThreadId{topic, threadId})
		}
	}

	if len(topicThreadIds) != len(allChannelsAndStreams) {
		panic("Mismatch between thread ID count and channel count")
	}
	threadStreamPairs := make(map[TopicAndThreadId]*ChannelAndStream)
	for i := 0; i < len(topicThreadIds); i++ {
		threadStreamPairs[topicThreadIds[i]] = allChannelsAndStreams[i]
	}

	for topicThread, channelStream := range threadStreamPairs {
		c.topicThreadIdsAndChannels[topicThread] = channelStream.Blocks
	}

	topicToStreams := make(map[string][]<-chan []*Message)
	for topicThread, channelStream := range threadStreamPairs {
		topic := topicThread.Topic
		if topicToStreams[topic] == nil {
			topicToStreams[topic] = make([]<-chan []*Message, 0)
		}
		topicToStreams[topic] = append(topicToStreams[topic], channelStream.Messages)
	}
	c.topicChannels = topicToStreams

	c.subscribeForChanges(c.config.Groupid)
	//TODO more subscriptions

	c.rebalance()
}

func (c *Consumer) Messages() <-chan *Message {
	return c.messages
}

func (c *Consumer) SwitchTopic(newTopic string) {
	c.fetcher.SwitchTopic(newTopic)
}

func (c *Consumer) Close() <-chan bool {
	Info(c, "Closing consumer")
	c.isShuttingdown = true
	go func() {
		Info(c, "Closing channels")
		for _, ch := range c.closeChannels {
			ch <- true
		}
		Info(c, "Closing fetcher")
		<-c.fetcher.Close()
		Info(c, "Unsubscribing")
		c.unsubscribe <- true
		Info(c, "Finished")
		c.closeFinished <- true
	}()
	return c.closeFinished
}

func (c *Consumer) updateFetcher() {
	allPartitionInfos := make([]*PartitionTopicInfo, 0)
	for _, partitionAndInfo := range c.topicRegistry {
		for _, partitionInfo := range partitionAndInfo {
			allPartitionInfos = append(allPartitionInfos, partitionInfo)
		}
	}

	c.fetcher.startConnections(allPartitionInfos)
}

func (c *Consumer) Ack(offset int64, topic string, partition int32) error {
	Infof(c, "Acking offset %d for topic %s and partition %d", offset, topic, partition)
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
	Infof(c, "Connecting to ZK at %s\n", c.config.ZookeeperConnect)
	if conn, _, err := zk.Connect(c.config.ZookeeperConnect, c.config.ZookeeperTimeout); err != nil {
		panic(err)
	} else {
		c.zkConn = conn
	}
}

func (c *Consumer) subscribeForChanges(group string) {
	Infof(c, "Subscribing for changes for %s", NewZKGroupDirs(group).ConsumerRegistryDir)

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
				Info(c, "Unsubscribing from changes")
				c.releasePartitionOwnership(c.topicRegistry)
				err := DeregisterConsumer(c.zkConn, c.config.Groupid, c.config.ConsumerId)
				if err != nil {
					panic(err)
				}
				break
			}
			}
		}
	}()
}

func triggerRebalanceIfNeeded(e zk.Event, c *Consumer) {
	emptyEvent := zk.Event{}
	if e != emptyEvent {
		c.rebalance()
	} else {
		time.Sleep(2 * time.Second)
	}
}

func (c *Consumer) rebalance() {
	partitionAssignor := NewPartitionAssignor(c.config.PartitionAssignmentStrategy)
	if (!c.isShuttingdown) {
		Infof(c, "rebalance triggered for %s\n", c.config.ConsumerId)
		var success = false
		var err error
		for i := 0; i < int(c.config.RebalanceMaxRetries); i++ {
			topicPerThreadIdsMap, err := NewTopicsToNumStreams(c.config.Groupid, c.config.ConsumerId, c.zkConn, c.config.ExcludeInternalTopics)
			if (err != nil) {
				Warn(c, err.Error())
				time.Sleep(c.config.RebalanceBackoffMs)
				continue
			}
			Infof(c, "%v\n", topicPerThreadIdsMap)

			brokers, err := GetAllBrokersInCluster(c.zkConn)
			if (err != nil) {
				Warn(c, err.Error())
				time.Sleep(c.config.RebalanceBackoffMs)
				continue
			}
			Infof(c, "%v\n", brokers)

			//TODO: close fetchers
			c.releasePartitionOwnership(c.topicRegistry)

			assignmentContext := NewAssignmentContext(c.config.Groupid, c.config.ConsumerId, c.config.ExcludeInternalTopics, c.zkConn)
			partitionOwnershipDecision := partitionAssignor(assignmentContext)
			topicPartitions := make([]*TopicAndPartition, 0)
			for topicPartition, _ := range partitionOwnershipDecision {
				topicPartitions = append(topicPartitions, &topicPartition)
			}

			offsetsFetchResponse, err := c.fetchOffsets(topicPartitions)
			if (err != nil) {
				Error(c, err.Error())
				break
			}

			currentTopicRegistry := make(map[string]map[int]*PartitionTopicInfo)

			if (c.isShuttingdown) {
				Warnf(c, "Aborting consumer '%s' rebalancing, since shutdown sequence started.", c.config.ConsumerId)
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
				c.updateFetcher()
			} else {
				Warnf(c, "Failed to reflect partition ownership for consumer %s", c.config.ConsumerId)
				time.Sleep(c.config.RebalanceBackoffMs)
				continue
			}

			success = true
			break
		}

		if (!success && !c.isShuttingdown) {
			panic(err)
		}
	} else {
		Infof(c, "Rebalance was triggered during consumer '%s' shutdown sequence. Ignoring...\n", c.config.ConsumerId)
	}
}

func (c *Consumer) fetchOffsets(topicPartitions []*TopicAndPartition) (*sarama.OffsetFetchResponse, error) {
	if (len(topicPartitions) == 0) {
		return &sarama.OffsetFetchResponse{}, nil
	} else {
		blocks := make(map[string]map[int32]*sarama.OffsetFetchResponseBlock)
		if (c.config.OffsetsStorage == "zookeeper") {
			for _, topicPartition := range topicPartitions {
				offset, err := GetOffsetForTopicPartition(c.zkConn, c.config.Groupid, topicPartition)
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

	topicAndThreadId := TopicAndThreadId{topicPartition.Topic, consumerThreadId}
	var blocks chan *sarama.FetchResponseBlock = nil
	for topicThread, blocksChannel := range c.topicThreadIdsAndChannels {
		if reflect.DeepEqual(topicAndThreadId, topicThread) {
			blocks = blocksChannel
		}
	}

	partTopicInfo := &PartitionTopicInfo{
		Topic: topicPartition.Topic,
		Partition: topicPartition.Partition,
		BlockChannel: blocks,
		ConsumedOffset: offset,
		FetchedOffset: offset,
		FetchSize: int(c.config.FetchMessageMaxBytes),
		ClientId: c.config.ConsumerId,
	}

	partTopicInfoMap[topicPartition.Partition] = partTopicInfo
	c.checkPointedZkOffsets[topicPartition] = offset
}

func (c *Consumer) reflectPartitionOwnershipDecision(partitionOwnershipDecision map[TopicAndPartition]*ConsumerThreadId) bool {
	Infof(c, "Consumer %s is trying to reflect partition ownership decision: %v\n", c.config.ConsumerId, partitionOwnershipDecision)
	successfullyOwnedPartitions := make([]*TopicAndPartition, 0)
	for topicPartition, consumerThreadId := range partitionOwnershipDecision {
		success, err := ClaimPartitionOwnership(c.zkConn, c.config.Groupid, topicPartition.Topic, topicPartition.Partition, consumerThreadId)
		if (err != nil) {
			panic(err)
		}
		if (success) {
			Debugf(c, "Consumer %s, successfully claimed partition %d for topic %s", c.config.ConsumerId, topicPartition.Partition, topicPartition.Topic)
			successfullyOwnedPartitions = append(successfullyOwnedPartitions, &topicPartition)
		} else {
			Warnf(c, "Consumer %s failed to claim partition %d for topic %s", c.config.ConsumerId, topicPartition.Partition, topicPartition.Topic)
		}
	}

	if (len(partitionOwnershipDecision) > len(successfullyOwnedPartitions)) {
		Warnf(c, "Consumer %s failed to reflect all partitions %d of %d", c.config.ConsumerId, len(successfullyOwnedPartitions), len(partitionOwnershipDecision))
		for _, topicPartition := range successfullyOwnedPartitions {
			DeletePartitionOwnership(c.zkConn, c.config.Groupid, topicPartition.Topic, topicPartition.Partition)
		}
		return false
	}

	return true
}

func (c *Consumer) releasePartitionOwnership(localTopicRegistry map[string]map[int]*PartitionTopicInfo) {
	Info(c, "Releasing partition ownership")
	for topic, partitionInfos := range localTopicRegistry {
		for partition, _ := range partitionInfos {
			err := DeletePartitionOwnership(c.zkConn, c.config.Groupid, topic, partition)
			if (err != nil) {
				panic(err)
			}
		}
		delete(localTopicRegistry, topic)
	}
}

func IsOffsetInvalid(offset int64) bool {
	return offset <= InvalidOffset
}

func NewChannelAndStream(config *ConsumerConfig, closeChannel chan bool) *ChannelAndStream {
	cs := &ChannelAndStream {
		Blocks : make(chan *sarama.FetchResponseBlock, config.QueuedMaxMessages),
		Messages : make(chan []*Message),
		closeChannel : closeChannel,
	}

	go cs.processIncomingBlocks()
	return cs
}

func (cs *ChannelAndStream) processIncomingBlocks() {
	Debug("cs", "Started processing blocks")
	for {
		select {
			case <-cs.closeChannel: {
				return
			}
			case b := <-cs.Blocks: {
				Debugf("cs", "Got block: %v", b)
				if b != nil {
					messages := make([]*Message, 0)
					for _, message := range b.MsgSet.Messages {
						msg := &Message {
							Key : message.Msg.Key,
							Value : message.Msg.Value,
							Offset : message.Offset,
						}
						messages = append(messages, msg)
					}
					if len(messages) > 0 {
						cs.Messages <- messages
					}
				}
			}
		}
	}
}
