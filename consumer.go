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

/* Package go_kafka_client provides a high-level Kafka consumer implementation and introduces different approach than Java/Scala high-level consumer.
 Primary differences include:
 - workers concept enforcing at least once processing before committing offsets;
 - improved rebalancing algorithm - closes obsolete connections and opens new connections without stopping the whole consumer;
 - supports graceful shutdown notifying client when it is over;
 - batch processing;
 - supports static partitions configuration allowing to start a consumer with a predefined set of partitions never caring about rebalancing; */
package go_kafka_client

import (
	"time"
	"sync"
	"fmt"
	"github.com/Shopify/sarama"
	metrics "github.com/rcrowley/go-metrics"
)

const (
	InvalidOffset int64 = -1

	//Reset the offset to the smallest offset if it is out of range
	SmallestOffset = "smallest"
	//Reset the offset to the largest offset if it is out of range
	LargestOffset = "largest"

	//Zookeeper offset storage configuration string
	ZookeeperOffsetStorage = "zookeeper"
	//Kafka offset storage configuration string
	KafkaOffsetStorage = "kafka"
)

// Consumer is a high-level Kafka consumer designed to work within a consumer group.
// It subscribes to coordinator events and is able to balance load within a consumer group.
type Consumer struct {
	config        *ConsumerConfig
	fetcher         *consumerFetcherManager
	unsubscribe    chan bool
	unsubscribeFinished chan bool
	closeFinished  chan bool
	rebalanceLock  sync.Mutex
	isShuttingdown bool
	topicPartitionsAndBuffers map[TopicAndPartition]*messageBuffer
	topicRegistry map[string]map[int32]*partitionTopicInfo
	connectChannels chan bool
	disconnectChannelsForPartition chan TopicAndPartition
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

	newDeployedTopics []*DeployedTopics
}

/* NewConsumer creates a new Consumer with a given configuration. Creating a Consumer does not start fetching immediately. */
func NewConsumer(config *ConsumerConfig) *Consumer {
	if err := config.Validate(); err != nil {
		panic(err)
	}

	Infof(config.Consumerid, "Creating new consumer with configuration: %s", config)
	c := &Consumer{
		config : config,
		unsubscribe : make(chan bool),
		unsubscribeFinished : make(chan bool),
		closeFinished : make(chan bool),
		topicPartitionsAndBuffers: make(map[TopicAndPartition]*messageBuffer),
		topicRegistry: make(map[string]map[int32]*partitionTopicInfo),
		connectChannels: make(chan bool),
		disconnectChannelsForPartition: make(chan TopicAndPartition),
		workerManagers: make(map[TopicAndPartition]*WorkerManager),
		askNextBatch: make(chan TopicAndPartition),
		stopStreams: make(chan bool),
	}

	if err := c.config.Coordinator.Connect(); err != nil {
		panic(err)
	}
	c.fetcher = newConsumerFetcherManager(c.config, c.askNextBatch, newBarrier(int32(c.config.NumConsumerFetchers), c.applyNewDeployedTopics))

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

/* Starts consuming specified topics using a configured amount of goroutines for each topic. */
func (c *Consumer) StartStatic(topicCountMap map[string]int) {
	go c.createMessageStreams(topicCountMap)

	c.startStreams()
}

/* Starts consuming all topics which correspond to a given topicFilter using numStreams goroutines for each topic. */
func (c *Consumer) StartWildcard(topicFilter TopicFilter, numStreams int) {
	go c.createMessageStreamsByFilterN(topicFilter, numStreams)
	c.startStreams()
}

/* Starts consuming given topic-partitions using ConsumerConfig.NumConsumerFetchers goroutines for each topic. */
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
	assignmentContext := newStaticAssignmentContext(c.config.Groupid, c.config.Consumerid, []string{c.config.Consumerid}, topicCount, topicPartitionMap)
	partitionOwnershipDecision := newPartitionAssignor(c.config.PartitionAssignmentStrategy)(assignmentContext)

	topicPartitions := make([]*TopicAndPartition, 0)
	for topicPartition, _ := range partitionOwnershipDecision {
		topicPartitions = append(topicPartitions, &TopicAndPartition{topicPartition.Topic, topicPartition.Partition})
	}

	offsetsFetchResponse, err := c.fetchOffsets(topicPartitions)
	if (err != nil) {
		panic(fmt.Sprintf("Failed to fetch offsets during rebalance: %s", err))
	}
	for _, topicPartition := range topicPartitions {
		offset := offsetsFetchResponse.Blocks[topicPartition.Topic][topicPartition.Partition].Offset
		threadId := partitionOwnershipDecision[*topicPartition]
		c.addPartitionTopicInfo(c.topicRegistry, topicPartition, offset, threadId)
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
		select {
		case <-c.stopStreams: {
			Debug(c, "Stop streams")
			c.disconnectChannels(stopRedirects)
			return
		}
		case tp := <-c.disconnectChannelsForPartition: {
			Tracef(c, "Disconnecting %s", tp)
			stopRedirects[tp] <- true
			delete(stopRedirects, tp)
		}
		case <-c.connectChannels: {
			Debug(c, "Restart streams")
			c.pipeChannels(stopRedirects)
		}
		}
	}
}

func (c *Consumer) pipeChannels(stopRedirects map[TopicAndPartition]chan bool) {
	inLock(&c.workerManagersLock, func() {
		Tracef(c, "connect channels registry: %v", c.topicRegistry)
		for topic, partitions := range c.topicRegistry {
			for partition, info := range partitions {
				topicPartition := TopicAndPartition{topic, partition}
				if _, exists := stopRedirects[topicPartition]; !exists {
					to, exists := c.workerManagers[topicPartition]
					if !exists {
						Infof(c, "WM > Failed to pipe message buffer to workermanager on partition %s", topicPartition)
						continue
					}
					Tracef(c, "Piping %s", topicPartition)
					stopRedirects[topicPartition] = pipe(info.Buffer.OutputChannel, to.InputChannel)
				}
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
	c.reinitializeConsumer()
}

func (c *Consumer) createMessageStreamsByFilterN(topicFilter TopicFilter, numStreams int) {
	allTopics, err := c.config.Coordinator.GetAllTopics()
	if err != nil {
		panic(err)
	}
	filteredTopics := make([]string, 0)
	for _, topic := range allTopics {
		if topicFilter.topicAllowed(topic, c.config.ExcludeInternalTopics) {
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
	c.reinitializeConsumer()

	//TODO subscriptions?
}

func (c *Consumer) createMessageStreamsByFilter(topicFilter TopicFilter) {
	c.createMessageStreamsByFilterN(topicFilter, c.config.NumConsumerFetchers)
}

func (c *Consumer) reinitializeConsumer() {
	//TODO more subscriptions
	c.rebalance()
	c.subscribeForChanges(c.config.Groupid)
}

func (c *Consumer) initializeWorkerManagers() {
	inLock(&c.workerManagersLock, func(){
		Debugf(c, "Initializing worker managers from topic registry: %s", c.topicRegistry)
		for topic, partitions := range c.topicRegistry {
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
	for topic, partitions := range c.topicRegistry {
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

// Tells the Consumer to close all existing connections and stop.
// This method is NOT blocking but returns a channel which will get a single value once the closing is finished.
func (c *Consumer) Close() <-chan bool {
	Info(c, "Consumer closing started...")
	c.isShuttingdown = true
	go func() {
		c.unsubscribeFromChanges()

		Info(c, "Closing fetcher manager...")
		<-c.fetcher.close()
		Info(c, "Stopping worker manager...")
		if !c.stopWorkerManagers() {
			panic("Graceful shutdown failed")
		}

		c.stopStreams <- true
		c.closeFinished <- true
	}()
	return c.closeFinished
}

func (c *Consumer) applyNewDeployedTopics() {
	inLock(&c.rebalanceLock, func(){
		Debug(c, "Releasing parition ownership")
		c.releasePartitionOwnership(c.topicRegistry)
		Debug(c, "Released parition ownership")

		currentDeployedTopics := c.newDeployedTopics[0]
		topicCount := NewStaticTopicsToNumStreams(c.config.Consumerid,
												  currentDeployedTopics.Topics,
												  currentDeployedTopics.Pattern,
												  c.config.NumConsumerFetchers,
												  c.config.ExcludeInternalTopics,
												  c.config.Coordinator)

		myTopicThreadIds := topicCount.GetConsumerThreadIdsPerTopic()
		topics := make([]string, 0)
		for topic, _ := range myTopicThreadIds {
			topics = append(topics, topic)
		}
		topicPartitionMap, err := c.config.Coordinator.GetPartitionsForTopics(topics)
		if err != nil {
			panic(err)
		}

		c.config.Coordinator.RegisterConsumer(c.config.Consumerid, c.config.Groupid, topicCount)
		consumersInGroup, err := c.config.Coordinator.GetConsumersInGroup(c.config.Groupid)
		if err != nil {
			panic(err)
		}
		assignmentContext := newStaticAssignmentContext(c.config.Groupid, c.config.Consumerid, consumersInGroup, topicCount, topicPartitionMap)
		partitionAssignor := newPartitionAssignor(c.config.PartitionAssignmentStrategy)
		partitionOwnershipDecision := partitionAssignor(assignmentContext)
		topicPartitions := make([]*TopicAndPartition, 0)
		for topicPartition, _ := range partitionOwnershipDecision {
			topicPartitions = append(topicPartitions, &TopicAndPartition{topicPartition.Topic, topicPartition.Partition})
		}
		currentTopicRegistry := make(map[string]map[int32]*partitionTopicInfo)
		for _, topicPartition := range topicPartitions {
			threadId := partitionOwnershipDecision[*topicPartition]
			c.addPartitionTopicInfo(currentTopicRegistry, topicPartition, InvalidOffset, threadId)
		}

		if c.reflectPartitionOwnershipDecision(partitionOwnershipDecision) {
			c.topicRegistry = currentTopicRegistry
			c.initFetchersAndWorkers(assignmentContext)
			c.newDeployedTopics = append(c.newDeployedTopics[:0], c.newDeployedTopics[1:]...)
		} else {
			panic("Failed to switch to new deployed topic")
		}
	})
}

func (c *Consumer) stopWorkerManagers() bool {
	success := false
	inLock(&c.workerManagersLock, func(){
		if len(c.workerManagers) > 0 {
			wmsAreStopped := make(chan bool)
			wmStopChannels := make([]chan bool, 0)
			for _, wm := range c.workerManagers {
				wmStopChannels = append(wmStopChannels, wm.Stop())
			}
			Debugf(c, "Worker channels length: %d", len(wmStopChannels))
			notifyWhenThresholdIsReached(wmStopChannels, wmsAreStopped, len(wmStopChannels))
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
	allPartitionInfos := make([]*partitionTopicInfo, 0)
	Debugf(c, "Topic Registry = %s", c.topicRegistry)
	for _, partitionAndInfo := range c.topicRegistry {
		for _, partitionInfo := range partitionAndInfo {
			allPartitionInfos = append(allPartitionInfos, partitionInfo)
		}
	}

	Debug(c, "Restarted streams")
	c.fetcher.startConnections(allPartitionInfos, numStreams)
	Debug(c, "Updated fetcher")
	c.connectChannels <- true
}

func (c *Consumer) subscribeForChanges(group string) {
	changes, err := c.config.Coordinator.SubscribeForChanges(group)
	if err != nil {
		panic(err)
	}

	go func() {
		for {
			select {
				case eventType := <-changes: {
					if eventType == NewTopicDeployed {
						Info(c, "New topic deployed")
						deployedTopics, err := c.config.Coordinator.GetNewDeployedTopics(group)
						Info(c, "There are new deployed topics")
						if err != nil {
							panic(err)
						}
						c.newDeployedTopics = deployedTopics
						c.fetcher.switchTopic <- true
					} else {
						inLock(&c.rebalanceLock, func() { c.rebalance() })
					}
				}
				case <-c.unsubscribe: {
					return
				}
			}
		}
	}()
}

func (c *Consumer) unsubscribeFromChanges() {
	c.unsubscribe <- true
	coordinator := c.config.Coordinator
	coordinator.Unsubscribe()
	c.releasePartitionOwnership(c.topicRegistry)
	coordinator.DeregisterConsumer(c.config.Consumerid, c.config.Groupid)
}

func (c *Consumer) rebalance() {
	partitionAssignor := newPartitionAssignor(c.config.PartitionAssignmentStrategy)
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

func tryRebalance(c *Consumer, partitionAssignor assignStrategy) bool {
	brokers, err := c.config.Coordinator.GetAllBrokers()
	if (err != nil) {
		Errorf(c, "Failed to get broker list: %s", err)
		return false
	}
	Infof(c, "%v\n", brokers)
	c.releasePartitionOwnership(c.topicRegistry)

	assignmentContext, err := newAssignmentContext(c.config.Groupid, c.config.Consumerid, c.config.ExcludeInternalTopics, c.config.Coordinator)
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

	currenttopicRegistry := make(map[string]map[int32]*partitionTopicInfo)

	if (c.isShuttingdown) {
		Warnf(c, "Aborting consumer '%s' rebalancing, since shutdown sequence started.", c.config.Consumerid)
		return true
	} else {
		for _, topicPartition := range topicPartitions {
			offset := offsetsFetchResponse.Blocks[topicPartition.Topic][topicPartition.Partition].Offset
			threadId := partitionOwnershipDecision[*topicPartition]
			c.addPartitionTopicInfo(currenttopicRegistry, topicPartition, offset, threadId)
		}
	}

	if (c.reflectPartitionOwnershipDecision(partitionOwnershipDecision)) {
		c.topicRegistry = currenttopicRegistry
		c.initFetchersAndWorkers(assignmentContext)
	} else {
		Errorf(c, "Failed to reflect partition ownership during rebalance")
		return false
	}

	return true
}

func (c *Consumer) initFetchersAndWorkers(assignmentContext *assignmentContext) {
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
	c.initializeWorkerManagers()
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

func (c *Consumer) addPartitionTopicInfo(currenttopicRegistry map[string]map[int32]*partitionTopicInfo,
	topicPartition *TopicAndPartition, offset int64,
	consumerThreadId ConsumerThreadId) {
	Tracef(c, "Adding partitionTopicInfo: %v \n %s", currenttopicRegistry, topicPartition)
	partTopicInfoMap, exists := currenttopicRegistry[topicPartition.Topic]
	if (!exists) {
		partTopicInfoMap = make(map[int32]*partitionTopicInfo)
		currenttopicRegistry[topicPartition.Topic] = partTopicInfoMap
	}

	buffer := c.topicPartitionsAndBuffers[*topicPartition]
	if buffer == nil {
		buffer = newMessageBuffer(*topicPartition, make(chan []*Message, c.config.QueuedMaxMessages), c.config, c.askNextBatch, c.disconnectChannelsForPartition)
		c.topicPartitionsAndBuffers[*topicPartition] = buffer
	}

	partTopicInfo := &partitionTopicInfo{
		Topic: topicPartition.Topic,
		Partition: topicPartition.Partition,
		Buffer: buffer,
		FetchedOffset: offset,
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

func (c *Consumer) releasePartitionOwnership(localtopicRegistry map[string]map[int32]*partitionTopicInfo) {
	Info(c, "Releasing partition ownership")
	for topic, partitionInfos := range localtopicRegistry {
		for partition, _ := range partitionInfos {
			if err := c.config.Coordinator.ReleasePartitionOwnership(c.config.Groupid, topic, partition); err != nil {
				panic(err)
			}
		}
		delete(localtopicRegistry, topic)
	}
}

func isOffsetInvalid(offset int64) bool {
	return offset <= InvalidOffset
}
