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
	"sync"
	"fmt"
	"github.com/Shopify/sarama"
	"reflect"
	metrics "github.com/rcrowley/go-metrics"
)

var InvalidOffset int64 = -1

var SmallestOffset = "smallest"
var LargestOffset = "largest"

var ZookeeperOffsetStorage = "zookeeper"
var KafkaOffsetStorage = "kafka"

type Consumer struct {
	config        *ConsumerConfig
	fetcher         *consumerFetcherManager
	unsubscribe    chan bool
	unsubscribeFinished chan bool
	closeFinished  chan bool
	rebalanceLock  sync.Mutex
	isShuttingdown bool
	topicThreadIdsAndAccumulators map[TopicAndThreadId]*BatchAccumulator
	TopicRegistry map[string]map[int32]*PartitionTopicInfo
	batchChannel chan []*Message
	reconnectChannels chan bool
	workerManagers map[TopicAndPartition]*WorkerManager
	workerManagersLock sync.Mutex
	askNextBatch           chan TopicAndPartition
	stopStreams            chan bool

	numWorkerManagersGauge metrics.Gauge
	batchesSentToWorkerManagerCounter metrics.Counter
	activeWorkersCounter metrics.Counter
	pendingWMsTasksCounter metrics.Counter
	wmsBatchDurationTimer metrics.Timer
	wmsIdleTimer metrics.Timer
}

func NewConsumer(config *ConsumerConfig) *Consumer {
	if err := config.Validate(); err != nil {
		panic(err)
	}

	Infof(config.Consumerid, "Starting new consumer with configuration: %s", config)
	c := &Consumer{
		config : config,
		unsubscribe : make(chan bool),
		unsubscribeFinished : make(chan bool),
		closeFinished : make(chan bool),
		topicThreadIdsAndAccumulators : make(map[TopicAndThreadId]*BatchAccumulator),
		TopicRegistry: make(map[string]map[int32]*PartitionTopicInfo),
		reconnectChannels: make(chan bool),
		workerManagers: make(map[TopicAndPartition]*WorkerManager),
		askNextBatch: make(chan TopicAndPartition),
		stopStreams: make(chan bool),
	}

	c.config.Coordinator.Connect()
	c.fetcher = newConsumerFetcherManager(c.config, c.askNextBatch)

	c.numWorkerManagersGauge = metrics.NewRegisteredGauge(fmt.Sprintf("NumWorkerManagers-%s", c.String()), metrics.DefaultRegistry)
	c.batchesSentToWorkerManagerCounter = metrics.NewRegisteredCounter(fmt.Sprintf("BatchesSentToWM-%s", c.String()), metrics.DefaultRegistry)
	c.activeWorkersCounter = metrics.NewRegisteredCounter(fmt.Sprintf("WMsActiveWorkers-%s", c.String()), metrics.DefaultRegistry)
	c.pendingWMsTasksCounter = metrics.NewRegisteredCounter(fmt.Sprintf("WMsPendingTasks-%s", c.String()), metrics.DefaultRegistry)
	c.wmsBatchDurationTimer = metrics.NewRegisteredTimer(fmt.Sprintf("WMsBatchDuration-%s", c.String()), metrics.DefaultRegistry)
	c.wmsIdleTimer = metrics.NewRegisteredTimer(fmt.Sprintf("WMsIdleTime-%s", c.String()), metrics.DefaultRegistry)

	return c
}

func (c *Consumer) String() string {
	return c.config.Consumerid
}

func (c *Consumer) StartStatic(topicCountMap map[string]int) {
	go c.createMessageStreams(topicCountMap)

	c.startStreams()
}

func (c *Consumer) StartWildcard(topicFilter TopicFilter, numStreams int) {
	go c.createMessageStreamsByFilterN(topicFilter, numStreams)
	c.startStreams()
}

func (c *Consumer) StartStaticPartitions(topicPartitionMap map[string][]int32) {
	topicsToNumStreamsMap := make(map[string]int)
	for topic := range topicPartitionMap {
		topicsToNumStreamsMap[topic] = c.config.NumConsumerFetchers
	}

	topicCount := &StaticTopicsToNumStreams {
		ConsumerId : c.config.Consumerid,
		TopicsToNumStreamsMap : topicsToNumStreamsMap,
	}

	c.config.Coordinator.RegisterConsumer(c.config.Consumerid, c.config.Groupid, topicCount)
	assignmentContext := NewStaticAssignmentContext(c.config.Groupid, c.config.Consumerid, topicCount, topicPartitionMap)
	partitionOwnershipDecision := NewPartitionAssignor(c.config.PartitionAssignmentStrategy)(assignmentContext)

	topicPartitions := make([]*TopicAndPartition, 0)
	for topicPartition, _ := range partitionOwnershipDecision {
		topicPartitions = append(topicPartitions, &TopicAndPartition{topicPartition.Topic, topicPartition.Partition})
	}

	offsetsFetchResponse, err := c.fetchOffsets(topicPartitions)
	if (err != nil) {
		panic(fmt.Sprintf("Failed to fetch offsets during rebalance: %s", err))
	}
	c.ReinitializeAccumulatorsAndChannels(topicCount)
	for _, topicPartition := range topicPartitions {
		offset := offsetsFetchResponse.Blocks[topicPartition.Topic][topicPartition.Partition].Offset
		threadId := partitionOwnershipDecision[*topicPartition]
		c.addPartitionTopicInfo(c.TopicRegistry, topicPartition, offset, threadId)
	}

	if (c.reflectPartitionOwnershipDecision(partitionOwnershipDecision)) {
		c.initializeWorkerManagers()
		go c.updateFetcher(c.config.NumConsumerFetchers)
	} else {
		panic("Could not reflect partition ownership")
	}

	c.startStreams()
}

func (c *Consumer) startStreams() {
	stopRedirects := make(map[TopicAndPartition]chan bool)
	for {
		Debug(c, "Inside select")
		select {
		case <-c.stopStreams: {
			Debug(c, "Stop streams")
			c.disconnectChannels(stopRedirects)
			return
		}
		case <-c.reconnectChannels: {
			Debug(c, "Restart streams")
			c.disconnectChannels(stopRedirects)
			c.connectChannels(stopRedirects)
		}
		}
	}
}

func (c *Consumer) connectChannels(stopRedirects map[TopicAndPartition]chan bool) {
	InLock(&c.workerManagersLock, func() {
		Tracef(c, "connect channels registry: %v", c.TopicRegistry)
		for topic, partitions := range c.TopicRegistry {
			for partition, info := range partitions {
				topicPartition := TopicAndPartition{topic, partition}
				from, exists := info.Accumulator.MessageBuffers[topicPartition]
				if !exists {
					Infof(c, "MB > Failed to pipe message buffer to workermanager on partition %s", topicPartition)
					continue
				}
				to, exists := c.workerManagers[topicPartition]
				if !exists {
					Infof(c, "WM > Failed to pipe message buffer to workermanager on partition %s", topicPartition)
					continue
				}
				Tracef(c, "Piping %s", topicPartition)
				stopRedirects[topicPartition] = Pipe(from.OutputChannel, to.InputChannel)
			}
		}
	})
}

func (c *Consumer) disconnectChannels(stopRedirects map[TopicAndPartition]chan bool) {
	for tp, stopRedirect := range stopRedirects {
		stopRedirect <- true
		delete(stopRedirects, tp)
	}
}

func (c *Consumer) createMessageStreams(topicCountMap map[string]int) {
	topicCount := &StaticTopicsToNumStreams {
		ConsumerId : c.config.Consumerid,
		TopicsToNumStreamsMap : topicCountMap,
	}

	c.config.Coordinator.RegisterConsumer(c.config.Consumerid, c.config.Groupid, topicCount)
	c.ReinitializeConsumer()
}

func (c *Consumer) createMessageStreamsByFilterN(topicFilter TopicFilter, numStreams int) {
	allTopics, err := c.config.Coordinator.GetAllTopics()
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
		Coordinator : c.config.Coordinator,
		ConsumerId : c.config.Consumerid,
		TopicFilter : topicFilter,
		NumStreams : numStreams,
		ExcludeInternalTopics : c.config.ExcludeInternalTopics,
	}

	c.config.Coordinator.RegisterConsumer(c.config.Consumerid, c.config.Groupid, topicCount)
	c.ReinitializeConsumer()

	//TODO subscriptions?
}

func (c *Consumer) createMessageStreamsByFilter(topicFilter TopicFilter) {
	c.createMessageStreamsByFilterN(topicFilter, c.config.NumConsumerFetchers)
}

func (c *Consumer) ReinitializeConsumer() {
	//TODO more subscriptions
	c.rebalance()
	c.subscribeForChanges(c.config.Groupid)
}

func (c *Consumer) ReinitializeAccumulatorsAndChannels(topicCount TopicsToNumStreams) {
	var accumulators []*BatchAccumulator = nil
	switch tc := topicCount.(type) {
	case *StaticTopicsToNumStreams: {
			for _, threadIdSet := range tc.GetConsumerThreadIdsPerTopic() {
				accumulatorsForThread := make([]*BatchAccumulator, len(threadIdSet))
				for i := 0; i < len(accumulatorsForThread); i++ {
					accumulatorsForThread[i] = NewBatchAccumulator(c.config, c.askNextBatch, c.reconnectChannels)
				}
				accumulators = append(accumulators, accumulatorsForThread...)
			}
		}
		case *WildcardTopicsToNumStreams: {
			for i := 0; i < tc.NumStreams; i++ {
				accumulators = append(accumulators, NewBatchAccumulator(c.config, c.askNextBatch, c.reconnectChannels))
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
		if _, exists := c.topicThreadIdsAndAccumulators[topicThreadIds[i]]; !exists {
			threadAccumulatorPairs[topicThreadIds[i]] = allAccumulators[i]
		} else {
			threadAccumulatorPairs[topicThreadIds[i]] = c.topicThreadIdsAndAccumulators[topicThreadIds[i]]
		}
	}

	c.topicThreadIdsAndAccumulators = threadAccumulatorPairs
}

func (c *Consumer) initializeWorkerManagers() {
	InLock(&c.workerManagersLock, func(){
		Debugf(c, "Initializing worker managers from topic registry: %s", c.TopicRegistry)
		for topic, partitions := range c.TopicRegistry {
			for partition := range partitions {
				topicPartition := TopicAndPartition{topic, partition}
				workerManager, exists := c.workerManagers[topicPartition]
				if !exists {
					workerManager = NewWorkerManager(fmt.Sprintf("WM-%s-%d", topic, partition), c.config, topicPartition, c.wmsIdleTimer,
													c.wmsBatchDurationTimer, c.activeWorkersCounter, c.pendingWMsTasksCounter)
					c.workerManagers[topicPartition] = workerManager
				}
				go workerManager.Start()
			}
		}
		c.removeObsoleteWorkerManagers()
	})
	c.numWorkerManagersGauge.Update(int64(len(c.workerManagers)))

}

func (c *Consumer) removeObsoleteWorkerManagers() {
	//safely copy current workerManagers map to temp map
	obsoleteWms := make(map[TopicAndPartition]*WorkerManager)
	for tp, wm := range c.workerManagers {
		obsoleteWms[tp] = wm
	}
	//remove all valid partitions from temp map, so only obsolete left after
	for topic, partitions := range c.TopicRegistry {
		for partition := range partitions {
			delete(obsoleteWms, TopicAndPartition{topic, partition})
		}
	}
	//stopping and removing obsolete worker managers from consumer registry
	for tp := range obsoleteWms {
		select {
		case <-c.workerManagers[tp].Stop():
		case <-time.After(5 * time.Second):
		}
		delete(c.workerManagers, tp)
	}
}

func (c *Consumer) SwitchTopic(topicCountMap map[string]int, pattern string) {
	Infof(c, "Switching to %s with pattern '%s'", topicCountMap, pattern)
	//TODO: whitelist/blacklist pattern handling
	switchTopicCount := &TopicSwitch {
		ConsumerId : c.config.Consumerid,
		TopicsToNumStreamsMap : topicCountMap,
		DesiredPattern: fmt.Sprintf("%s%s", SwitchToPatternPrefix, pattern),
	}

	c.config.Coordinator.RegisterConsumer(c.config.Consumerid, c.config.Groupid, switchTopicCount)

	err := c.config.Coordinator.NotifyConsumerGroup(c.config.Groupid, c.config.Consumerid)
	if err != nil {
		panic(err)
	}
}

func (c *Consumer) Close() <-chan bool {
	Info(c, "Consumer closing started...")
	c.isShuttingdown = true
	go func() {
		c.Unsubscribe()

		Info(c, "Closing fetcher manager...")
		<-c.fetcher.Close()
		Info(c, "Stopping worker manager...")
		if !c.stopWorkerManagers() {
			panic("Graceful shutdown failed")
		}

		Info(c, "Stopping offsets committer...")
		c.stopStreams <- true
		c.closeFinished <- true
	}()
	return c.closeFinished
}

func (c *Consumer) Idle() {
	c.fetcher.CloseAllFetchers()
	Debug(c, "About to stop worker managers")
	if len(c.workerManagers) > 0 {
		c.stopWorkerManagers()
	}
	Debug(c, "Successfully stopped worker managers")
	Debug(c, c.TopicRegistry)
	c.releasePartitionOwnership(c.TopicRegistry)
}

func (c *Consumer) stopWorkerManagers() bool {
	success := false
	InLock(&c.workerManagersLock, func(){
		if len(c.workerManagers) > 0 {
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
				success = true
			}
			case <-time.After(c.config.WorkerManagersStopTimeout): {
				Errorf(c, "Workers failed to stop whithin timeout of %s", c.config.WorkerManagersStopTimeout)
				success = false
			}
			}
		} else {
			Debug(c, "No worker managers to close")
			success = true
		}
	})

	return success
}

func (c *Consumer) updateFetcher(numStreams int) {
	Debugf(c, "Updating fetcher with numStreams = %d", numStreams)
	allPartitionInfos := make([]*PartitionTopicInfo, 0)
	Debugf(c, "Topic Registry = %s", c.TopicRegistry)
	for _, partitionAndInfo := range c.TopicRegistry {
		for _, partitionInfo := range partitionAndInfo {
			allPartitionInfos = append(allPartitionInfos, partitionInfo)
		}
	}

	Debug(c, "Restarted streams")
	c.fetcher.startConnections(allPartitionInfos, numStreams)
	Debug(c, "Updated fetcher")
	Debug(c, "Restarting streams")
	c.reconnectChannels <- true
}

func (c *Consumer) Ack(offset int64, topic string, partition int32) error {
	Infof(c, "Acking offset %d for topic %s and partition %d", offset, topic, partition)
	return nil
}

func (c *Consumer) subscribeForChanges(group string) {
	changes, err := c.config.Coordinator.SubscribeForChanges(group)
	if err != nil {
		panic(err)
	}

	go func() {
		for {
			select {
				case <-changes: {
					InLock(&c.rebalanceLock, func() { c.rebalance() })
				}
				case <-c.unsubscribe: {
					return
				}
			}
		}
	}()
}

func (c *Consumer) Unsubscribe() {
	c.unsubscribe <- true
	coordinator := c.config.Coordinator
	coordinator.Unsubscribe()
	c.releasePartitionOwnership(c.TopicRegistry)
	coordinator.DeregisterConsumer(c.config.Consumerid, c.config.Groupid)
}

func (c *Consumer) rebalance() {
	partitionAssignor := NewPartitionAssignor(c.config.PartitionAssignmentStrategy)
	if (!c.isShuttingdown) {
		Infof(c, "rebalance triggered for %s\n", c.config.Consumerid)
		var success = false
		for i := 0; i <= int(c.config.RebalanceMaxRetries); i++ {
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
		Infof(c, "Rebalance was triggered during consumer '%s' shutdown sequence. Ignoring...", c.config.Consumerid)
	}
}

func tryRebalance(c *Consumer, partitionAssignor AssignStrategy) bool {
	//Don't hurry to delete it, we need it for closing the fetchers
	topicPerThreadIdsMap, err := NewTopicsToNumStreams(c.config.Groupid, c.config.Consumerid, c.config.Coordinator, c.config.ExcludeInternalTopics)
	if (err != nil) {
		Errorf(c, "Failed to get topic count map: %s", err)
		return false
	}
	Infof(c, "%v\n", topicPerThreadIdsMap)

	brokers, err := c.config.Coordinator.GetAllBrokers()
	if (err != nil) {
		Errorf(c, "Failed to get broker list: %s", err)
		return false
	}
	Infof(c, "%v\n", brokers)
	c.releasePartitionOwnership(c.TopicRegistry)

	assignmentContext, err := NewAssignmentContext(c.config.Groupid, c.config.Consumerid, c.config.ExcludeInternalTopics, c.config.Coordinator)
	if err != nil {
		Errorf(c, "Failed to initialize assignment context: %s", err)
		return false
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
		Warnf(c, "Aborting consumer '%s' rebalancing, since shutdown sequence started.", c.config.Consumerid)
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
		c.initializeWorkerManagers()
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
				offset, err := c.config.Coordinator.GetOffsetForTopicPartition(c.config.Groupid, topicPartition)
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
	consumerThreadId ConsumerThreadId) {
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
		ClientId: c.config.Consumerid,
	}

	partTopicInfoMap[topicPartition.Partition] = partTopicInfo
}

func (c *Consumer) reflectPartitionOwnershipDecision(partitionOwnershipDecision map[TopicAndPartition]ConsumerThreadId) bool {
	Infof(c, "Consumer is trying to reflect partition ownership decision: %v\n", partitionOwnershipDecision)
	successfullyOwnedPartitions := make([]*TopicAndPartition, 0)
	for topicPartition, consumerThreadId := range partitionOwnershipDecision {
		success, err := c.config.Coordinator.ClaimPartitionOwnership(c.config.Groupid, topicPartition.Topic, topicPartition.Partition, consumerThreadId)
		if (err != nil) {
			panic(err)
		}
		if (success) {
			Debugf(c, "Consumer successfully claimed partition %d for topic %s", topicPartition.Partition, topicPartition.Topic)
			successfullyOwnedPartitions = append(successfullyOwnedPartitions, &topicPartition)
		} else {
			Warnf(c, "Consumer failed to claim partition %d for topic %s", topicPartition.Partition, topicPartition.Topic)
		}
	}

	if (len(partitionOwnershipDecision) > len(successfullyOwnedPartitions)) {
		Warnf(c, "Consumer failed to reflect all partitions %d of %d", len(successfullyOwnedPartitions), len(partitionOwnershipDecision))
		for _, topicPartition := range successfullyOwnedPartitions {
			c.config.Coordinator.ReleasePartitionOwnership(c.config.Groupid, topicPartition.Topic, topicPartition.Partition)
		}
		return false
	}

	return true
}

func (c *Consumer) releasePartitionOwnership(localTopicRegistry map[string]map[int32]*PartitionTopicInfo) {
	Info(c, "Releasing partition ownership")
	for topic, partitionInfos := range localTopicRegistry {
		for partition, _ := range partitionInfos {
			if err := c.config.Coordinator.ReleasePartitionOwnership(c.config.Groupid, topic, partition); err != nil {
				panic(err)
			}
		}
		delete(localTopicRegistry, topic)
	}
}

func IsOffsetInvalid(offset int64) bool {
	return offset <= InvalidOffset
}
