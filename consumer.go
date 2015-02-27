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

/* Package go_kafka_client provides a high-level Kafka consumer implementation and introduces different approach than Java/Scala high-level consumer.

Primary differences include workers concept enforcing at least once processing before committing offsets, improved rebalancing algorithm - closing obsolete connections and opening new connections without stopping the whole consumer,
graceful shutdown support notifying client when it is over, batch processing, static partitions configuration support allowing to start a consumer with a predefined set of partitions never caring about rebalancing. */
package go_kafka_client

import (
	"fmt"
	metrics "github.com/rcrowley/go-metrics"
	"strings"
	"sync"
	"time"
)

const (
	// Offset with invalid value
	InvalidOffset int64 = -1

	// Reset the offset to the smallest offset if it is out of range
	SmallestOffset = "smallest"
	// Reset the offset to the largest offset if it is out of range
	LargestOffset = "largest"

	// Zookeeper offset storage configuration string
	ZookeeperOffsetStorage = "zookeeper"
	// Kafka offset storage configuration string
	KafkaOffsetStorage = "kafka"
)

// Consumer is a high-level Kafka consumer designed to work within a consumer group.
// It subscribes to coordinator events and is able to balance load within a consumer group.
type Consumer struct {
	config                         *ConsumerConfig
	fetcher                        *consumerFetcherManager
	shouldUnsubscribe              bool
	unsubscribe                    chan bool
	closeFinished                  chan bool
	rebalanceLock                  sync.Mutex
	isShuttingdown                 bool
	topicPartitionsAndBuffers      map[TopicAndPartition]*messageBuffer
	topicRegistry                  map[string]map[int32]*partitionTopicInfo
	connectChannels                chan bool
	disconnectChannelsForPartition chan TopicAndPartition
	workerManagers                 map[TopicAndPartition]*WorkerManager
	workerManagersLock             sync.Mutex
	stopStreams                    chan bool

	numWorkerManagersGauge            metrics.Gauge
	batchesSentToWorkerManagerCounter metrics.Counter
	activeWorkersCounter              metrics.Counter
	pendingWMsTasksCounter            metrics.Counter
	wmsBatchDurationTimer             metrics.Timer
	wmsIdleTimer                      metrics.Timer

	lastSuccessfulRebalanceHash string
}

/* NewConsumer creates a new Consumer with a given configuration. Creating a Consumer does not start fetching immediately. */
func NewConsumer(config *ConsumerConfig) *Consumer {
	if err := config.Validate(); err != nil {
		panic(err)
	}

	Infof(config.Consumerid, "Creating new consumer with configuration: %s", config)
	c := &Consumer{
		config:                         config,
		unsubscribe:                    make(chan bool),
		closeFinished:                  make(chan bool),
		topicPartitionsAndBuffers:      make(map[TopicAndPartition]*messageBuffer),
		topicRegistry:                  make(map[string]map[int32]*partitionTopicInfo),
		connectChannels:                make(chan bool),
		disconnectChannelsForPartition: make(chan TopicAndPartition),
		workerManagers:                 make(map[TopicAndPartition]*WorkerManager),
		stopStreams:                    make(chan bool),
	}

	if err := c.config.Coordinator.Connect(); err != nil {
		panic(err)
	}
	if err := c.config.LowLevelClient.Initialize(); err != nil {
		panic(err)
	}
	c.fetcher = newConsumerFetcherManager(c.config, c.disconnectChannelsForPartition)

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

	topicCount := &StaticTopicsToNumStreams{
		ConsumerId:            c.config.Consumerid,
		TopicsToNumStreamsMap: topicsToNumStreamsMap,
	}

	c.config.Coordinator.RegisterConsumer(c.config.Consumerid, c.config.Groupid, topicCount)
	allTopics, err := c.config.Coordinator.GetAllTopics()
	if err != nil {
		panic(err)
	}
	brokers, err := c.config.Coordinator.GetAllBrokers()
	if err != nil {
		panic(err)
	}

	time.Sleep(c.config.DeploymentTimeout)

	assignmentContext := newStaticAssignmentContext(c.config.Groupid, c.config.Consumerid, []string{c.config.Consumerid}, allTopics, brokers, topicCount, topicPartitionMap)
	partitionOwnershipDecision := newPartitionAssignor(c.config.PartitionAssignmentStrategy)(assignmentContext)

	topicPartitions := make([]*TopicAndPartition, 0)
	for topicPartition, _ := range partitionOwnershipDecision {
		topicPartitions = append(topicPartitions, &TopicAndPartition{topicPartition.Topic, topicPartition.Partition})
	}

	offsets, err := c.fetchOffsets(topicPartitions)
	if err != nil {
		panic(fmt.Sprintf("Failed to fetch offsets during rebalance: %s", err))
	}
	for _, topicPartition := range topicPartitions {
		offset := offsets[*topicPartition]
		threadId := partitionOwnershipDecision[*topicPartition]
		c.addPartitionTopicInfo(c.topicRegistry, topicPartition, offset, threadId)
	}

	if c.reflectPartitionOwnershipDecision(partitionOwnershipDecision) {
		c.updateFetcher(c.config.NumConsumerFetchers)
		c.initializeWorkerManagers()
	} else {
		panic("Could not reflect partition ownership")
	}

	go func() {
		Infof(c, "Restarted streams")
		c.connectChannels <- true
	}()

	c.startStreams()
}

func (c *Consumer) startStreams() {
	stopRedirects := make(map[TopicAndPartition]chan bool)
	for {
		select {
		case <-c.stopStreams:
			{
				Debug(c, "Stop streams")
				c.disconnectChannels(stopRedirects)
				return
			}
		case tp := <-c.disconnectChannelsForPartition:
			{
				Tracef(c, "Disconnecting %s", tp)
				stopRedirects[tp] <- true
				delete(stopRedirects, tp)

				Debugf(c, "Stopping worker manager for %s", tp)
				select {
				case <-c.workerManagers[tp].Stop():
				case <-time.After(5 * time.Second):
				}
				delete(c.workerManagers, tp)

				Debugf(c, "Stopping buffer: %s", c.topicPartitionsAndBuffers[tp])
				c.topicPartitionsAndBuffers[tp].stop()
				delete(c.topicPartitionsAndBuffers, tp)
			}
		case <-c.connectChannels:
			{
				Debug(c, "Restart streams")
				c.pipeChannels(stopRedirects)
			}
		}
	}
}

func (c *Consumer) pipeChannels(stopRedirects map[TopicAndPartition]chan bool) {
	inLock(&c.workerManagersLock, func() {
		Debugf(c, "connect channels registry: %v", c.topicRegistry)
		for topic, partitions := range c.topicRegistry {
			for partition, info := range partitions {
				topicPartition := TopicAndPartition{topic, partition}
				if _, exists := stopRedirects[topicPartition]; !exists {
					to, exists := c.workerManagers[topicPartition]
					if !exists {
						Infof(c, "WM > Failed to pipe message buffer to workermanager on partition %s", topicPartition)
						continue
					}
					Debugf(c, "Piping %s", topicPartition)
					stopRedirects[topicPartition] = pipe(info.Buffer.OutputChannel, to.inputChannel)
				}
			}
		}
	})
}

func (c *Consumer) disconnectChannels(stopRedirects map[TopicAndPartition]chan bool) {
	for tp, stopRedirect := range stopRedirects {
		Debugf(c, "Disconnecting channel for %s", tp)
		stopRedirect <- true
		delete(stopRedirects, tp)
	}
}

func (c *Consumer) createMessageStreams(topicCountMap map[string]int) {
	topicCount := &StaticTopicsToNumStreams{
		ConsumerId:            c.config.Consumerid,
		TopicsToNumStreamsMap: topicCountMap,
	}

	c.config.Coordinator.RegisterConsumer(c.config.Consumerid, c.config.Groupid, topicCount)

	time.Sleep(c.config.DeploymentTimeout)

	c.reinitializeConsumer()
}

func (c *Consumer) createMessageStreamsByFilterN(topicFilter TopicFilter, numStreams int) {
	topicCount := &WildcardTopicsToNumStreams{
		Coordinator:           c.config.Coordinator,
		ConsumerId:            c.config.Consumerid,
		TopicFilter:           topicFilter,
		NumStreams:            numStreams,
		ExcludeInternalTopics: c.config.ExcludeInternalTopics,
	}

	c.config.Coordinator.RegisterConsumer(c.config.Consumerid, c.config.Groupid, topicCount)

	time.Sleep(c.config.DeploymentTimeout)

	c.reinitializeConsumer()
}

func (c *Consumer) createMessageStreamsByFilter(topicFilter TopicFilter) {
	c.createMessageStreamsByFilterN(topicFilter, c.config.NumConsumerFetchers)
}

func (c *Consumer) reinitializeConsumer() {
	c.subscribeForChanges(c.config.Groupid)
	c.rebalance()
}

func (c *Consumer) initializeWorkerManagers() {
	inLock(&c.workerManagersLock, func() {
		Infof(c, "Initializing worker managers from topic registry: %s", c.topicRegistry)
		for topic, partitions := range c.topicRegistry {
			for partition := range partitions {
				topicPartition := TopicAndPartition{topic, partition}
				workerManager, exists := c.workerManagers[topicPartition]
				if !exists {
					workerManager = NewWorkerManager(fmt.Sprintf("WM-%s-%d", topic, partition), c.config, topicPartition, c.wmsIdleTimer,
						c.wmsBatchDurationTimer, c.activeWorkersCounter, c.pendingWMsTasksCounter)
					c.workerManagers[topicPartition] = workerManager
					go workerManager.Start()
				}
			}
		}
	})
	c.numWorkerManagersGauge.Update(int64(len(c.workerManagers)))

}

// Tells the Consumer to close all existing connections and stop.
// This method is NOT blocking but returns a channel which will get a single value once the closing is finished.
func (c *Consumer) Close() <-chan bool {
	Info(c, "Consumer closing started...")
	c.isShuttingdown = true
	go func() {
		if c.shouldUnsubscribe {
			c.unsubscribeFromChanges()
		}

		Info(c, "Closing fetcher manager...")
		<-c.fetcher.close()
		Info(c, "Stopping worker manager...")
		if !c.stopWorkerManagers() {
			panic("Graceful shutdown failed")
		}

		c.stopStreams <- true

		c.releasePartitionOwnership(c.topicRegistry)

		Info(c, "Deregistering consumer")
		c.config.Coordinator.DeregisterConsumer(c.config.Consumerid, c.config.Groupid)
		Info(c, "Successfully deregistered consumer")

		Info(c, "Disconnecting from consumer coordinator")
		c.config.Coordinator.Disconnect()
		Info(c, "Disconnected from consumer coordinator")

		c.closeFinished <- true
	}()
	return c.closeFinished
}

func (c *Consumer) handleBlueGreenRequest(requestId string, blueGreenRequest *BlueGreenDeployment) {
	var context *assignmentContext
	//Waiting for everybody in group to acknowledge the request, then closing
	inLock(&c.rebalanceLock, func() {
		Infof("Starting blue-green procedure for: %s", blueGreenRequest)
		var err error
		var stateHash string
		barrierPassed := false
		for !barrierPassed {
			context, err = newAssignmentContext(c.config.Groupid, c.config.Consumerid,
				c.config.ExcludeInternalTopics, c.config.Coordinator)
			if err != nil {
				Errorf(c, "Failed to initialize assignment context: %s", err)
				panic(err)
			}
			barrierSize := len(context.Consumers)
			stateHash = context.hash()
			barrierPassed = c.config.Coordinator.AwaitOnStateBarrier(c.config.Consumerid, c.config.Groupid, stateHash,
				barrierSize, fmt.Sprintf("%s/%s", BlueGreenDeploymentAPI, requestId), c.config.BarrierTimeout)
		}

		<-c.Close()

		err = c.config.Coordinator.Connect()
		if err != nil {
			panic(fmt.Sprintf("Failed to connect to Coordinator: %s", err))
		}

		//Waiting for target group to leave the group
		amountOfConsumersInTargetGroup := -1
		for amountOfConsumersInTargetGroup != 0 {
			consumersInTargetGroup, err := c.config.Coordinator.GetConsumersInGroup(blueGreenRequest.Group)
			if err != nil {
				panic(fmt.Sprintf("Failed to perform blue-green procedure due to: %s", err))
			}
			amountOfConsumersInTargetGroup = len(consumersInTargetGroup)
			for _, consumer := range consumersInTargetGroup {
				for _, myGroupConsumer := range context.Consumers {
					if consumer == myGroupConsumer {
						amountOfConsumersInTargetGroup--
						break
					}
				}
			}
			time.Sleep(5 * time.Second)
		}

		//Removing obsolete api calls
		c.config.Coordinator.RemoveOldApiRequests(blueGreenRequest.Group)

		//Generating new topicCount
		var topicCount TopicsToNumStreams
		switch blueGreenRequest.Pattern {
		case blackListPattern:
			topicCount = &WildcardTopicsToNumStreams{
				Coordinator:           c.config.Coordinator,
				ConsumerId:            c.config.Consumerid,
				TopicFilter:           NewBlackList(blueGreenRequest.Topics),
				NumStreams:            c.config.NumConsumerFetchers,
				ExcludeInternalTopics: c.config.ExcludeInternalTopics,
			}
		case whiteListPattern:
			topicCount = &WildcardTopicsToNumStreams{
				Coordinator:           c.config.Coordinator,
				ConsumerId:            c.config.Consumerid,
				TopicFilter:           NewWhiteList(blueGreenRequest.Topics),
				NumStreams:            c.config.NumConsumerFetchers,
				ExcludeInternalTopics: c.config.ExcludeInternalTopics,
			}
		case staticPattern:
			{
				topicMap := make(map[string]int)
				for _, topic := range strings.Split(blueGreenRequest.Topics, ",") {
					topicMap[topic] = c.config.NumConsumerFetchers
				}
				topicCount = &StaticTopicsToNumStreams{
					ConsumerId:            c.config.Consumerid,
					TopicsToNumStreamsMap: topicMap,
				}
			}
		}

		//Getting the partitions for specified topics
		myTopicThreadIds := topicCount.GetConsumerThreadIdsPerTopic()
		topics := make([]string, 0)
		for topic, _ := range myTopicThreadIds {
			topics = append(topics, topic)
		}
		topicPartitionMap, err := c.config.Coordinator.GetPartitionsForTopics(topics)
		if err != nil {
			panic(err)
		}

		//Creating assignment context with new parameters
		newContext := newStaticAssignmentContext(blueGreenRequest.Group, c.config.Consumerid, context.Consumers,
			context.AllTopics, context.Brokers, topicCount, topicPartitionMap)
		c.config.Groupid = blueGreenRequest.Group

		//Resume consuming
		c.resumeAfterClose(newContext)
		Infof(c, "Blue-green procedure has been successfully finished %s", blueGreenRequest)
	})
}

func (c *Consumer) resumeAfterClose(context *assignmentContext) {
	c.isShuttingdown = false
	c.workerManagers = make(map[TopicAndPartition]*WorkerManager)
	c.topicPartitionsAndBuffers = make(map[TopicAndPartition]*messageBuffer)
	c.fetcher = newConsumerFetcherManager(c.config, c.disconnectChannelsForPartition)

	go c.startStreams()

	partitionAssignor := newPartitionAssignor(c.config.PartitionAssignmentStrategy)
	partitionOwnershipDecision := partitionAssignor(context)
	topicPartitions := make([]*TopicAndPartition, 0)
	for topicPartition, _ := range partitionOwnershipDecision {
		topicPartitions = append(topicPartitions, &TopicAndPartition{topicPartition.Topic, topicPartition.Partition})
	}
	offsets, err := c.fetchOffsets(topicPartitions)
	if err != nil {
		panic(fmt.Sprintf("Failed to fetch offsets during rebalance: %s", err))
	}
	currentTopicRegistry := make(map[string]map[int32]*partitionTopicInfo)
	for _, topicPartition := range topicPartitions {
		offset := offsets[*topicPartition]
		threadId := partitionOwnershipDecision[*topicPartition]
		c.addPartitionTopicInfo(currentTopicRegistry, topicPartition, offset, threadId)
	}

	c.config.Coordinator.RegisterConsumer(c.config.Consumerid, c.config.Groupid, context.MyTopicToNumStreams)
	if c.reflectPartitionOwnershipDecision(partitionOwnershipDecision) {
		c.topicRegistry = currentTopicRegistry
		c.lastSuccessfulRebalanceHash = context.hash()
		c.initFetchersAndWorkers(context)
	} else {
		panic("Failed to switch to new deployed topic")
	}

	c.subscribeForChanges(c.config.Groupid)
}

func (c *Consumer) stopWorkerManagers() bool {
	success := false
	inLock(&c.workerManagersLock, func() {
		if len(c.workerManagers) > 0 {
			wmsAreStopped := make(chan bool)
			wmStopChannels := make([]chan bool, 0)
			for _, wm := range c.workerManagers {
				wmStopChannels = append(wmStopChannels, wm.Stop())
			}
			Debugf(c, "Worker channels length: %d", len(wmStopChannels))
			notifyWhenThresholdIsReached(wmStopChannels, wmsAreStopped, len(wmStopChannels))
			select {
			case <-wmsAreStopped:
				{
					Info(c, "All workers have been gracefully stopped")
					c.workerManagers = make(map[TopicAndPartition]*WorkerManager)
					success = true
				}
			case <-time.After(c.config.WorkerManagersStopTimeout):
				{
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
	Infof(c, "Updating fetcher with numStreams = %d", numStreams)
	allPartitionInfos := make([]*partitionTopicInfo, 0)
	Infof(c, "Topic Registry = %s", c.topicRegistry)
	for _, partitionAndInfo := range c.topicRegistry {
		for _, partitionInfo := range partitionAndInfo {
			allPartitionInfos = append(allPartitionInfos, partitionInfo)
		}
	}

	c.fetcher.startConnections(allPartitionInfos, numStreams)
	Infof(c, "Updated fetcher")
}

func (c *Consumer) subscribeForChanges(group string) {
	changes, err := c.config.Coordinator.SubscribeForChanges(group)
	if err != nil {
		panic(err)
	}
	c.shouldUnsubscribe = true

	go func() {
		for {
			select {
			case eventType := <-changes:
				{
					if eventType == BlueGreenRequest {
						blueGreenRequests, err := c.config.Coordinator.GetBlueGreenRequest(group)
						if len(blueGreenRequests) > 0 {
							Info(c, "Blue-green deployment procedure has been requested")
							if err != nil {
								panic(err)
							}
							for requestId, request := range blueGreenRequests {
								go c.handleBlueGreenRequest(requestId, request)
								break
							}
						}
					} else {
						go c.rebalance()
					}
				}
			case <-c.unsubscribe:
				{
					return
				}
			}
		}
	}()
}

func (c *Consumer) unsubscribeFromChanges() {
	c.unsubscribe <- true
	c.config.Coordinator.Unsubscribe()
}

func (c *Consumer) rebalance() {
	if !c.isShuttingdown {
		inLock(&c.rebalanceLock, func() {
			success := false
			var stateHash string
			for i := 0; i <= int(c.config.RebalanceMaxRetries); i++ {
				Infof(c, "rebalance triggered for %s\n", c.config.Consumerid)
				partitionAssignor := newPartitionAssignor(c.config.PartitionAssignmentStrategy)
				var context *assignmentContext
				var err error
				barrierPassed := false
				for !barrierPassed {
					context, err = newAssignmentContext(c.config.Groupid, c.config.Consumerid,
						c.config.ExcludeInternalTopics, c.config.Coordinator)
					if err != nil {
						Errorf(c, "Failed to initialize assignment context: %s", err)
						panic(err)
					}
					barrierSize := len(context.Consumers)
					stateHash = context.hash()

					if c.lastSuccessfulRebalanceHash == stateHash {
						Info(c, "No need in rebalance this time")
						return
					}
					c.releasePartitionOwnership(c.topicRegistry)
					barrierPassed = c.config.Coordinator.AwaitOnStateBarrier(c.config.Consumerid, c.config.Groupid,
						stateHash, barrierSize, string(Rebalance),
						c.config.BarrierTimeout)
				}

				if tryRebalance(c, context, partitionAssignor) {
					success = true
					break
				} else {
					time.Sleep(c.config.RebalanceBackoff)
				}
			}
			if !success && !c.isShuttingdown {
				panic(fmt.Sprintf("Failed to rebalance after %d retries", c.config.RebalanceMaxRetries))
			} else {
				c.lastSuccessfulRebalanceHash = stateHash
				Info(c, "Rebalance has been successfully completed")
			}
		})
	} else {
		Infof(c, "Rebalance was triggered during consumer '%s' shutdown sequence. Ignoring...", c.config.Consumerid)
	}
}

func tryRebalance(c *Consumer, context *assignmentContext, partitionAssignor assignStrategy) bool {
	partitionOwnershipDecision := partitionAssignor(context)
	topicPartitions := make([]*TopicAndPartition, 0)
	for topicPartition, _ := range partitionOwnershipDecision {
		topicPartitions = append(topicPartitions, &TopicAndPartition{topicPartition.Topic, topicPartition.Partition})
	}

	offsets, err := c.fetchOffsets(topicPartitions)
	if err != nil {
		Errorf(c, "Failed to fetch offsets during rebalance: %s", err)
		return false
	}

	currenttopicRegistry := make(map[string]map[int32]*partitionTopicInfo)

	if c.isShuttingdown {
		Warnf(c, "Aborting consumer '%s' rebalancing, since shutdown sequence started.", c.config.Consumerid)
		return true
	} else {
		for _, topicPartition := range topicPartitions {
			offset := offsets[*topicPartition]
			threadId := partitionOwnershipDecision[*topicPartition]
			c.addPartitionTopicInfo(currenttopicRegistry, topicPartition, offset, threadId)
		}
	}

	if c.reflectPartitionOwnershipDecision(partitionOwnershipDecision) {
		Info(c, "Partition ownership has been successfully reflected")
		c.topicRegistry = currenttopicRegistry
		Infof(c, "Trying to reinitialize fetchers and workers")
		c.initFetchersAndWorkers(context)
		Infof(c, "Fetchers and workers have been successfully reinitialized")
	} else {
		Errorf(c, "Failed to reflect partition ownership during rebalance")
		return false
	}

	return true
}

func (c *Consumer) initFetchersAndWorkers(assignmentContext *assignmentContext) {
	switch topicCount := assignmentContext.MyTopicToNumStreams.(type) {
	case *StaticTopicsToNumStreams:
		{
			var numStreams int
			for _, v := range topicCount.GetConsumerThreadIdsPerTopic() {
				numStreams = len(v)
				break
			}
			Infof(c, "Trying to update fetcher")
			c.updateFetcher(numStreams)
		}
	case *WildcardTopicsToNumStreams:
		{
			c.updateFetcher(topicCount.NumStreams)
		}
	}
	Infof(c, "Fetcher has been updated %s", assignmentContext)
	c.initializeWorkerManagers()

	Infof(c, "Restarted streams")
	c.connectChannels <- true
}

func (c *Consumer) fetchOffsets(topicPartitions []*TopicAndPartition) (map[TopicAndPartition]int64, error) {
	offsets := make(map[TopicAndPartition]int64)
	if c.config.OffsetsStorage == "zookeeper" {
		for _, topicPartition := range topicPartitions {
			offset, err := c.config.Coordinator.GetOffsetForTopicPartition(c.config.Groupid, topicPartition)
			if err != nil {
				return nil, err
			} else {
				offsets[*topicPartition] = offset
			}
		}
	} else {
		panic(fmt.Sprintf("Offset storage '%s' is not supported", c.config.OffsetsStorage))
	}

	return offsets, nil
}

func (c *Consumer) addPartitionTopicInfo(currenttopicRegistry map[string]map[int32]*partitionTopicInfo,
	topicPartition *TopicAndPartition, offset int64,
	consumerThreadId ConsumerThreadId) {
	Tracef(c, "Adding partitionTopicInfo: %v \n %s", currenttopicRegistry, topicPartition)
	partTopicInfoMap, exists := currenttopicRegistry[topicPartition.Topic]
	if !exists {
		partTopicInfoMap = make(map[int32]*partitionTopicInfo)
		currenttopicRegistry[topicPartition.Topic] = partTopicInfoMap
	}

	buffer := c.topicPartitionsAndBuffers[*topicPartition]
	if buffer == nil {
		buffer = newMessageBuffer(*topicPartition, make(chan []*Message, c.config.QueuedMaxMessages), c.config)
		c.topicPartitionsAndBuffers[*topicPartition] = buffer
	}

	partTopicInfo := &partitionTopicInfo{
		Topic:         topicPartition.Topic,
		Partition:     topicPartition.Partition,
		Buffer:        buffer,
		FetchedOffset: offset,
	}

	partTopicInfoMap[topicPartition.Partition] = partTopicInfo
}

func (c *Consumer) reflectPartitionOwnershipDecision(partitionOwnershipDecision map[TopicAndPartition]ConsumerThreadId) bool {
	Infof(c, "Consumer is trying to reflect partition ownership decision: %v\n", partitionOwnershipDecision)
	successfullyOwnedPartitions := make([]*TopicAndPartition, 0)
	for topicPartition, consumerThreadId := range partitionOwnershipDecision {
		success, err := c.config.Coordinator.ClaimPartitionOwnership(c.config.Groupid, topicPartition.Topic, topicPartition.Partition, consumerThreadId)
		if err != nil {
			panic(err)
		}
		if success {
			Debugf(c, "Consumer successfully claimed partition %d for topic %s", topicPartition.Partition, topicPartition.Topic)
			successfullyOwnedPartitions = append(successfullyOwnedPartitions, &topicPartition)
		} else {
			Warnf(c, "Consumer failed to claim partition %d for topic %s", topicPartition.Partition, topicPartition.Topic)
		}
	}

	if len(partitionOwnershipDecision) > len(successfullyOwnedPartitions) {
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
	Info(c, "Successfully released partition ownership")
}

// Returns a state snapshot for this consumer. State snapshot contains a set of metrics splitted by topics and partitions.
func (c *Consumer) StateSnapshot() *StateSnapshot {
	metricsMap := make(map[string]map[string]float64)
	metrics.DefaultRegistry.Each(func(name string, metric interface{}) {
		metricsMap[name] = make(map[string]float64)
		switch entry := metric.(type) {
		case metrics.Counter:
			{
				metricsMap[name]["count"] = float64(entry.Count())
			}
		case metrics.Gauge:
			{
				metricsMap[name]["value"] = float64(entry.Value())
			}
		case metrics.Histogram:
			{
				metricsMap[name]["count"] = float64(entry.Count())
				metricsMap[name]["max"] = float64(entry.Max())
				metricsMap[name]["min"] = float64(entry.Min())
				metricsMap[name]["mean"] = entry.Mean()
				metricsMap[name]["stdDev"] = entry.StdDev()
				metricsMap[name]["sum"] = float64(entry.Sum())
				metricsMap[name]["variance"] = entry.Variance()
			}
		case metrics.Meter:
			{
				metricsMap[name]["count"] = float64(entry.Count())
				metricsMap[name]["rate1"] = entry.Rate1()
				metricsMap[name]["rate5"] = entry.Rate5()
				metricsMap[name]["rate15"] = entry.Rate15()
				metricsMap[name]["rateMean"] = entry.RateMean()
			}
		case metrics.Timer:
			{
				metricsMap[name]["count"] = float64(entry.Count())
				metricsMap[name]["max"] = float64(entry.Max())
				metricsMap[name]["min"] = float64(entry.Min())
				metricsMap[name]["mean"] = entry.Mean()
				metricsMap[name]["rate1"] = entry.Rate1()
				metricsMap[name]["rate5"] = entry.Rate5()
				metricsMap[name]["rate15"] = entry.Rate15()
				metricsMap[name]["rateMean"] = entry.RateMean()
				metricsMap[name]["stdDev"] = entry.StdDev()
				metricsMap[name]["sum"] = float64(entry.Sum())
				metricsMap[name]["variance"] = entry.Variance()
			}
		}
	})

	offsetsMap := make(map[string]map[int32]int64)
	for topicAndPartition, workerManager := range c.workerManagers {
		if _, exists := offsetsMap[topicAndPartition.Topic]; !exists {
			offsetsMap[topicAndPartition.Topic] = make(map[int32]int64)
		}

		offsetsMap[topicAndPartition.Topic][topicAndPartition.Partition] = workerManager.GetLargestOffset()
	}

	return &StateSnapshot{
		Metrics: metricsMap,
		Offsets: offsetsMap,
	}
}

func isOffsetInvalid(offset int64) bool {
	return offset <= InvalidOffset
}
