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
	fetcher         *consumerFetcherManager
	unsubscribe    chan bool
	closeFinished  chan bool
	zkConn          *zk.Conn
	rebalanceLock  sync.Mutex
	isShuttingdown bool
	topicThreadIdsAndAccumulators map[TopicAndThreadId]*BatchAccumulator
	TopicRegistry map[string]map[int32]*PartitionTopicInfo
	checkPointedZkOffsets map[*TopicAndPartition]int64
	batchChannel chan []*Message
	restartStreams chan []chan []*Message
	offsetsCommitter *OffsetsCommitter
	workerManagers map[TopicAndPartition]*WorkerManager
	askNextBatch           chan TopicAndPartition
	stopStreams            chan bool
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
		unsubscribe : make(chan bool),
		closeFinished : make(chan bool),
		topicThreadIdsAndAccumulators : make(map[TopicAndThreadId]*BatchAccumulator),
		TopicRegistry: make(map[string]map[int32]*PartitionTopicInfo),
		checkPointedZkOffsets: make(map[*TopicAndPartition]int64),
		restartStreams: make(chan []chan []*Message),
		workerManagers: make(map[TopicAndPartition]*WorkerManager),
		askNextBatch: make(chan TopicAndPartition),
		stopStreams: make(chan bool),
	}

	c.addShutdownHook()

	c.connectToZookeeper()
	c.fetcher = newConsumerFetcherManager(c.config, c.zkConn, c.askNextBatch)

	return c
}

func (c *Consumer) String() string {
	return c.config.ConsumerId
}

func (c *Consumer) StartStatic(topicCountMap map[string]int) {
	go c.createMessageStreams(topicCountMap)

	c.startStreams()
}

func (c *Consumer) StartWildcard(topicFilter TopicFilter, numStreams int) {
	go c.createMessageStreamsByFilterN(topicFilter, numStreams)
	c.startStreams()
}

func (c *Consumer) startStreams() {
	c.batchChannel = make(chan []*Message)
	stopRedirectingChannels := RedirectChannelsTo(make([]<-chan []*Message, 0), c.batchChannel)
	for {
		Debug(c, "Inside select")
		select {
		case <-c.stopStreams: {
			Debug(c, "Stop streams")
			stopRedirectingChannels <- true
			return
		}
		case newAllStreams := <-c.restartStreams: {
			Debug(c, "Restart streams")
			stopRedirectingChannels <- true
			stopRedirectingChannels = RedirectChannelsTo(newAllStreams, c.batchChannel)
		}
		case batch := <-c.batchChannel: {
			Debugf(c, "Got batch: %s", batch)
			Debug(c, c.workerManagers)
			topicPartition := TopicAndPartition{batch[0].Topic, batch[0].Partition}
			wm, ok := c.workerManagers[topicPartition]
			if !ok {
				panic(fmt.Sprintf("%s - Failed to get wm for %s", c, topicPartition) )
			} else {
				wm.InputChannel<-batch
			}
			Debugf(c, "Sent batch for processing: %s", batch)
		}
		}
	}
}

func (c *Consumer) createMessageStreams(topicCountMap map[string]int) {
	topicCount := &StaticTopicsToNumStreams {
		ConsumerId : c.config.ConsumerId,
		TopicsToNumStreamsMap : topicCountMap,
	}

	c.RegisterInZK(topicCount)
	c.ReinitializeAccumulatorsAndChannels(topicCount)
	c.ReinitializeConsumer()
}

func (c *Consumer) createMessageStreamsByFilterN(topicFilter TopicFilter, numStreams int) {
	allTopics, err := GetTopics(c.zkConn)
	if err != nil {
		panic(err)
	}
	filteredTopics := make([]string, 0)
	for _, topic := range allTopics {
		if topicFilter.IsTopicAllowed(topic, c.config.ExcludeInternalTopics) {
			filteredTopics = append(filteredTopics, topic)
		}
	}
	topicCount := &WildcardTopicsToNumStreams{
		ZkConnection : c.zkConn,
		ConsumerId : c.config.ConsumerId,
		TopicFilter : topicFilter,
		NumStreams : numStreams,
		ExcludeInternalTopics : c.config.ExcludeInternalTopics,
	}

	c.RegisterInZK(topicCount)
	c.ReinitializeAccumulatorsAndChannels(topicCount)
	c.ReinitializeConsumer()

	//TODO subscriptions?
}

func (c *Consumer) createMessageStreamsByFilter(topicFilter TopicFilter) {
	c.createMessageStreamsByFilterN(topicFilter, c.config.NumConsumerFetchers)
}

func (c *Consumer) RegisterInZK(topicCount TopicsToNumStreams) {
	RegisterConsumer(c.zkConn, c.config.Groupid, c.config.ConsumerId, &ConsumerInfo{
			Version : int16(1),
			Subscription : topicCount.GetTopicsToNumStreamsMap(),
			Pattern : topicCount.Pattern(),
			Timestamp : time.Now().Unix(),
		})
}

func (c *Consumer) ReinitializeConsumer() {
	//TODO more subscriptions
	c.rebalance()
	c.initializeWorkerManagersAndOffsetsCommitter()
	c.subscribeForChanges(c.config.Groupid)
}

func (c *Consumer) ReinitializeAccumulatorsAndChannels(topicCount TopicsToNumStreams) {
	var accumulators []*BatchAccumulator = nil
	switch tc := topicCount.(type) {
	case *StaticTopicsToNumStreams: {
			for _, threadIdSet := range tc.GetConsumerThreadIdsPerTopic() {
				accumulatorsForThread := make([]*BatchAccumulator, len(threadIdSet))
				for i := 0; i < len(accumulatorsForThread); i++ {
					accumulatorsForThread[i] = NewBatchAccumulator(c.config)
				}
				accumulators = append(accumulators, accumulatorsForThread...)
			}
		}
		case *WildcardTopicsToNumStreams: {
			for i := 0; i < tc.NumStreams; i++ {
				accumulators = append(accumulators, NewBatchAccumulator(c.config))
			}
		}
	}

	consumerThreadIdsPerTopic := topicCount.GetConsumerThreadIdsPerTopic()

	allAccumulators := make([]*BatchAccumulator, 0)
	switch topicCount.(type) {
	case *StaticTopicsToNumStreams: {
		allAccumulators = accumulators
	}
	case *WildcardTopicsToNumStreams: {
		for _, _ = range consumerThreadIdsPerTopic {
			for _, accumulator := range accumulators {
				allAccumulators = append(allAccumulators, accumulator)
			}
		}
	}
	}
	topicThreadIds := make([]TopicAndThreadId, 0)
	for topic, threadIds := range consumerThreadIdsPerTopic {
		for _, threadId := range threadIds {
			topicThreadIds = append(topicThreadIds, TopicAndThreadId{topic, threadId})
		}
	}

	if len(topicThreadIds) != len(allAccumulators) {
		panic("Mismatch between thread ID count and channel count")
	}
	threadAccumulatorPairs := make(map[TopicAndThreadId]*BatchAccumulator)
	for i := 0; i < len(topicThreadIds); i++ {
		threadAccumulatorPairs[topicThreadIds[i]] = allAccumulators[i]
	}

	for topicThread, accumulator := range threadAccumulatorPairs {
		c.topicThreadIdsAndAccumulators[topicThread] = accumulator
	}
}

func (c *Consumer) initializeWorkerManagersAndOffsetsCommitter() {
	workerChannels := make([]chan map[TopicAndPartition]int64, 0)
	c.workerManagers = make(map[TopicAndPartition]*WorkerManager)
	for topic, partitions := range c.TopicRegistry {
		for partition := range partitions {
			workerChannel := make(chan map[TopicAndPartition]int64)
			manager := NewWorkerManager(c.config, workerChannel, c.fetcher, c.askNextBatch)
			c.workerManagers[TopicAndPartition{topic, partition}] = manager
			go manager.Start()
			workerChannels = append(workerChannels, workerChannel)
		}
	}
	c.offsetsCommitter = NewOffsetsCommiter(c.config, workerChannels, c.zkConn)
	go c.offsetsCommitter.Start()
}

func (c *Consumer) SwitchTopic(topicCountMap map[string]int, pattern string) {
	Infof(c, "Switching to %s with pattern '%s'", topicCountMap, pattern)
	//TODO: whitelist/blacklist pattern handling
	staticTopicCount := &TopicSwitch {
		ConsumerId : c.config.ConsumerId,
		TopicsToNumStreamsMap : topicCountMap,
		DesiredPattern: pattern,
	}

	RegisterConsumer(c.zkConn, c.config.Groupid, c.config.ConsumerId, &ConsumerInfo{
		Version : int16(1),
		Subscription : staticTopicCount.GetTopicsToNumStreamsMap(),
		Pattern : fmt.Sprintf("%s%s", SwitchToPatternPrefix, staticTopicCount.Pattern()),
		Timestamp : time.Now().Unix(),
	})
	err := NotifyConsumerGroup(c.zkConn, c.config.Groupid, c.config.ConsumerId)
	if err != nil {
		panic(err)
	}
}

func (c *Consumer) Close() <-chan bool {
	Info(c, "Consumer closing started...")
	c.isShuttingdown = true
	go func() {
		c.unsubscribe <- true

		Info(c, "Stopping worker manager...")
		if !c.stopWorkerManagers() {
			panic("Graceful shutdown failed")
		}

		//Deregistering consumer
		DeregisterConsumer(c.zkConn, c.config.Groupid, c.config.ConsumerId)

		Info(c, "Stopping offsets committer...")
		c.offsetsCommitter.Stop()
		Info(c, "Closing fetcher manager...")
		<-c.fetcher.Close()
		c.stopStreams <- true
		c.closeFinished <- true
	}()
	return c.closeFinished
}

func (c *Consumer) stopWorkerManagers() bool {
	wmsAreStopped := make(chan bool)
	wmStopChannels := make([]chan bool, 0)
	for _, wm := range c.workerManagers {
		wmStopChannels = append(wmStopChannels, wm.Stop())
	}
	Debugf(c, "Worker channels length: %d", len(wmStopChannels))
	NotifyWhenThresholdIsReached(wmStopChannels, wmsAreStopped, len(wmStopChannels))
	select {
	case <-wmsAreStopped: {
		Info(c, "All workers have been gracefully stopped")
		c.workerManagers = make(map[TopicAndPartition]*WorkerManager)
		return true
	}
	case <-time.After(c.config.WorkerManagersStopTimeout): {
		Errorf(c, "Workers failed to stop whithin timeout of %s", c.config.WorkerManagersStopTimeout)
		return false
	}
	}
}

func (c *Consumer) updateFetcher(numStreams int) {
	Debug(c, "Updating fetcher")
	newAllStreams := make([]chan []*Message, 0)
	allPartitionInfos := make([]*PartitionTopicInfo, 0)
	Debugf(c, "Topic Registry = %s", c.TopicRegistry)
	for _, partitionAndInfo := range c.TopicRegistry {
		for _, partitionInfo := range partitionAndInfo {
			newAllStreams = append(newAllStreams, partitionInfo.Accumulator.OutputChannel)
			allPartitionInfos = append(allPartitionInfos, partitionInfo)
		}
	}

	Debug(c, "Restarting streams")
	c.restartStreams <- newAllStreams
	Debug(c, "Restarted streams")
	c.fetcher.startConnections(allPartitionInfos, numStreams)
	Debug(c, "Updated fetcher")
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
		Info(c, "Got a shutdown event")
		<-c.Close()
		Info(c, "Successfully closed a consumer!")
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

func (c *Consumer)ensureZkPathsExist(group string) {
	dirs := NewZKGroupDirs(group)
	CreateOrUpdatePathParentMayNotExist(c.zkConn, dirs.ConsumerDir, make([]byte, 0))
	CreateOrUpdatePathParentMayNotExist(c.zkConn, dirs.ConsumerGroupDir, make([]byte, 0))
	CreateOrUpdatePathParentMayNotExist(c.zkConn, dirs.ConsumerRegistryDir, make([]byte, 0))
	CreateOrUpdatePathParentMayNotExist(c.zkConn, dirs.ConsumerChangesDir, make([]byte, 0))
}

func (c *Consumer) subscribeForChanges(group string) {
	c.ensureZkPathsExist(group)
	Infof(c, "Subscribing for changes for %s", group)

	consumersWatcher, err := GetConsumersInGroupWatcher(c.zkConn, group)
	if err != nil {
		panic(err)
	}
	consumerGroupChangesWatcher, err := GetConsumerGroupChangesWatcher(c.zkConn, group)
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
				Trace(c, e)
				if e.State == zk.StateDisconnected {
					Debug(c, "Topic registry watcher session ended, reconnecting...")
					watcher, err := GetTopicsWatcher(c.zkConn)
					if err != nil {
						panic(err)
					}
					topicsWatcher = watcher
				} else {
					InLock(&c.rebalanceLock, func() { triggerRebalanceIfNeeded(e, c) })
				}
			}
			case e := <-consumersWatcher: {
				Trace(c, e)
				if e.State == zk.StateDisconnected {
					Debug(c, "Consumer registry watcher session ended, reconnecting...")
					watcher, err := GetConsumersInGroupWatcher(c.zkConn, group)
					if err != nil {
						panic(err)
					}
					consumersWatcher = watcher
				} else {
					InLock(&c.rebalanceLock, func() { triggerRebalanceIfNeeded(e, c) })
				}
			}
			case e := <-brokersWatcher: {
				Trace(c, e)
				if e.State == zk.StateDisconnected {
					Debug(c, "Broker registry watcher session ended, reconnecting...")
					watcher, err := GetAllBrokersInClusterWatcher(c.zkConn)
					if err != nil {
						panic(err)
					}
					brokersWatcher = watcher
				} else {
					InLock(&c.rebalanceLock, func() { triggerRebalanceIfNeeded(e, c) })
				}
			}
			case e := <-consumerGroupChangesWatcher: {
				Trace(c, e)
				if e.State == zk.StateDisconnected {
					Debug(c, "Consumer changes watcher session ended, reconnecting...")
					watcher, err := GetConsumerGroupChangesWatcher(c.zkConn, group)
					if err != nil {
						panic(err)
					}
					consumerGroupChangesWatcher = watcher
				} else {
					InLock(&c.rebalanceLock, func() { triggerRebalanceIfNeeded(e, c) })
				}
			}
			case <-c.unsubscribe: {
				Info(c, "Unsubscribing from Zookeeper changes...")
				c.releasePartitionOwnership(c.TopicRegistry)
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
		for i := 0; i < int(c.config.RebalanceMaxRetries); i++ {
			if (tryRebalance(c, partitionAssignor)) {
				success = true
				break
			} else {
				time.Sleep(c.config.RebalanceBackoff)
			}
		}

		if (!success && !c.isShuttingdown) {
			panic(fmt.Sprintf("Failed to rebalance after %d retries", c.config.RebalanceMaxRetries))
		}
	} else {
		Infof(c, "Rebalance was triggered during consumer '%s' shutdown sequence. Ignoring...", c.config.ConsumerId)
	}
}

func tryRebalance(c *Consumer, partitionAssignor AssignStrategy) bool {
	//Don't hurry to delete it, we need it for closing the fetchers
	topicPerThreadIdsMap, err := NewTopicsToNumStreams(c.config.Groupid, c.config.ConsumerId, c.zkConn, c.config.ExcludeInternalTopics)
	if (err != nil) {
		Errorf(c, "Failed to get topic count map: %s", err)
		return false
	}
	Infof(c, "%v\n", topicPerThreadIdsMap)

	brokers, err := GetAllBrokersInCluster(c.zkConn)
	if (err != nil) {
		Errorf(c, "Failed to get broker list: %s", err)
		return false
	}
	Infof(c, "%v\n", brokers)

	c.fetcher.CloseAllFetchers()
	Debug(c, "About to stop worker managers")
	if len(c.workerManagers) > 0 {
		c.stopWorkerManagers()
		c.offsetsCommitter.Stop()
	}
	Debug(c, "Successfully stopped worker managers")
	Debug(c, c.TopicRegistry)
	c.releasePartitionOwnership(c.TopicRegistry)

	assignmentContext, err := NewAssignmentContext(c.config.Groupid, c.config.ConsumerId, c.config.ExcludeInternalTopics, c.zkConn)
	if err != nil {
		Errorf(c, "Failed to initialize assignment context: %s", err)
		return false
	}
	if assignmentContext.State.IsGroupTopicSwitchInProgress {
		Info(c, "Consumer group is in process of switching to new topics")
		if !assignmentContext.InTopicSwitch {
			c.SwitchTopic(assignmentContext.State.DesiredTopicCountMap, assignmentContext.State.DesiredPattern)
			return true
		}

		if !assignmentContext.State.IsGroupTopicSwitchInSync {
			zkGroupInSync, err := IsConsumerGroupInSync(c.zkConn, c.config.Groupid)
			if err != nil {
				Error(c, "Failed to get group sync state")
				return false
			}
			if !zkGroupInSync {
				Infof(c, "Group is not sync yet. Waiting...")
				return true
			}
		} else {
			err = CreateConsumerGroupSync(c.zkConn, c.config.Groupid)
			if err != nil {
				Error(c, "Failed to initialize assignment context")
				return false
			}
		}

		RegisterConsumer(c.zkConn, assignmentContext.Group, assignmentContext.ConsumerId, &ConsumerInfo{
			Version : int16(1),
			Subscription : assignmentContext.State.DesiredTopicCountMap,
			Pattern : assignmentContext.State.DesiredPattern,
			Timestamp : time.Now().Unix(),
		})
		err = NotifyConsumerGroup(c.zkConn, c.config.Groupid, c.config.ConsumerId)
		if (err != nil) {
			Errorf(c, "Failed to notify consumer group: %s", err)
			return false
		}
	} else {
		err = DeleteConsumerGroupSync(c.zkConn, c.config.Groupid)
		if err != nil {
			Errorf(c, "Failed to delete consumer group sync: %s", err)
		}
		err = PurgeObsoleteConsumerGroupNotifications(c.zkConn, c.config.Groupid)
		if err != nil {
			Errorf(c, "Failed to delete obsolete notifications for consumer group: %s", err)
		}
	}

	partitionOwnershipDecision := partitionAssignor(assignmentContext)
	topicPartitions := make([]*TopicAndPartition, 0)
	for topicPartition, _ := range partitionOwnershipDecision {
		topicPartitions = append(topicPartitions, &TopicAndPartition{topicPartition.Topic, topicPartition.Partition})
	}

	offsetsFetchResponse, err := c.fetchOffsets(topicPartitions)
	if (err != nil) {
		Errorf(c, "Failed to fetch offsets during rebalance: %s", err)
		return false
	}

	currentTopicRegistry := make(map[string]map[int32]*PartitionTopicInfo)

	if (c.isShuttingdown) {
		Warnf(c, "Aborting consumer '%s' rebalancing, since shutdown sequence started.", c.config.ConsumerId)
		return true
	} else {
		c.ReinitializeAccumulatorsAndChannels(assignmentContext.MyTopicToNumStreams)
		for _, topicPartition := range topicPartitions {
			offset := offsetsFetchResponse.Blocks[topicPartition.Topic][topicPartition.Partition].Offset
			threadId := partitionOwnershipDecision[*topicPartition]
			c.addPartitionTopicInfo(currentTopicRegistry, topicPartition, offset, threadId)
		}
	}

	if (c.reflectPartitionOwnershipDecision(partitionOwnershipDecision)) {
		c.TopicRegistry = currentTopicRegistry
		c.initializeWorkerManagersAndOffsetsCommitter()
		switch topicCount := assignmentContext.MyTopicToNumStreams.(type) {
			case *StaticTopicsToNumStreams: {
				var numStreams int
				for _, v := range topicCount.GetConsumerThreadIdsPerTopic() {
					numStreams = len(v)
					break
				}
				c.updateFetcher(numStreams)
			}
			case *WildcardTopicsToNumStreams: {
				c.updateFetcher(topicCount.NumStreams)
			}
		}
	} else {
		Errorf(c, "Failed to reflect partition ownership during rebalance")
		return false
	}

	return true
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

func (c *Consumer) addPartitionTopicInfo(currentTopicRegistry map[string]map[int32]*PartitionTopicInfo,
	topicPartition *TopicAndPartition, offset int64,
	consumerThreadId *ConsumerThreadId) {
	partTopicInfoMap, exists := currentTopicRegistry[topicPartition.Topic]
	if (!exists) {
		partTopicInfoMap = make(map[int32]*PartitionTopicInfo)
		currentTopicRegistry[topicPartition.Topic] = partTopicInfoMap
	}

	topicAndThreadId := TopicAndThreadId{topicPartition.Topic, consumerThreadId}
	var accumulator *BatchAccumulator = nil
	for topicThread, acc := range c.topicThreadIdsAndAccumulators {
		if reflect.DeepEqual(topicAndThreadId, topicThread) {
			accumulator = acc
		}
	}

	partTopicInfo := &PartitionTopicInfo{
		Topic: topicPartition.Topic,
		Partition: topicPartition.Partition,
		Accumulator: accumulator,
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

func (c *Consumer) releasePartitionOwnership(localTopicRegistry map[string]map[int32]*PartitionTopicInfo) {
	Info(c, "Releasing partition ownership")
	for topic, partitionInfos := range localTopicRegistry {
		for partition, _ := range partitionInfos {
			err := DeletePartitionOwnership(c.zkConn, c.config.Groupid, topic, partition)
			if (err != nil) {
				if err == zk.ErrNoNode {
					Warn(c, err)
				} else {
					panic(err)
				}
			}
		}
		delete(localTopicRegistry, topic)
	}
}

func IsOffsetInvalid(offset int64) bool {
	return offset <= InvalidOffset
}
