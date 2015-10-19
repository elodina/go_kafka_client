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
	"math"
	"sync"
	"time"
)

type consumerFetcherManager struct {
	config                         *ConsumerConfig
	numStreams                     int
	closeFinished                  chan bool
	updateLock                     sync.RWMutex
	partitionMap                   map[TopicAndPartition]*partitionTopicInfo
	fetcherRoutineMap              map[int]*consumerFetcherRoutine
	shuttingDown                   bool
	updateInProgress               bool
	updatedCond                    *sync.Cond
	disconnectChannelsForPartition chan TopicAndPartition

	metrics *ConsumerMetrics
	client  LowLevelClient
}

func (m *consumerFetcherManager) String() string {
	return fmt.Sprintf("%s-manager", m.config.Consumerid)
}

func newConsumerFetcherManager(config *ConsumerConfig, disconnectChannelsForPartition chan TopicAndPartition, metrics *ConsumerMetrics) *consumerFetcherManager {
	manager := &consumerFetcherManager{
		config:                         config,
		closeFinished:                  make(chan bool),
		partitionMap:                   make(map[TopicAndPartition]*partitionTopicInfo),
		fetcherRoutineMap:              make(map[int]*consumerFetcherRoutine),
		disconnectChannelsForPartition: disconnectChannelsForPartition,
		client:  config.LowLevelClient,
		metrics: metrics,
	}
	manager.updatedCond = sync.NewCond(manager.updateLock.RLocker())

	return manager
}

func (m *consumerFetcherManager) startConnections(topicInfos []*partitionTopicInfo, numStreams int) {
	if Logger.IsAllowed(DebugLevel) {
		Debug(m, "Fetcher Manager started")
		Debugf(m, "TopicInfos = %s", topicInfos)
	}
	m.numStreams = numStreams

	partitionInfos := make(map[TopicAndPartition]*partitionTopicInfo)

	m.updateInProgress = true
	inWriteLock(&m.updateLock, func() {
		if Logger.IsAllowed(DebugLevel) {
			Debug(m, "Updating fetcher configuration")
		}
		newPartitionMap := make(map[TopicAndPartition]*partitionTopicInfo)
		for _, providedInfo := range topicInfos {
			topicAndPartition := TopicAndPartition{providedInfo.Topic, providedInfo.Partition}
			if currentInfo, alreadyFetching := m.partitionMap[topicAndPartition]; alreadyFetching {
				newPartitionMap[topicAndPartition] = currentInfo
				continue
			} else {
				newPartitionMap[topicAndPartition] = providedInfo
				partitionInfos[topicAndPartition] = providedInfo
			}
		}

		if Logger.IsAllowed(DebugLevel) {
			Debugf(m, "Got new list of partitions to process %v", newPartitionMap)
			Debugf(m, "All partitions map: %v", m.partitionMap)
		}
		//receive obsolete partitions map
		for k := range newPartitionMap {
			delete(m.partitionMap, k)
		}

		//removing obsolete partitions and tearing down associated jobs
		topicPartitionsToRemove := make([]TopicAndPartition, 0)
		for tp := range m.partitionMap {
			topicPartitionsToRemove = append(topicPartitionsToRemove, tp)
		}
		if Logger.IsAllowed(DebugLevel) {
			Debugf(m, "There are obsolete partitions %v", topicPartitionsToRemove)
		}

		//removing unnecessary partition-fetchRoutine bindings
		for _, fetcher := range m.fetcherRoutineMap {
			if Logger.IsAllowed(DebugLevel) {
				Debugf(m, "Fetcher %s parition map before obsolete partitions removal", fetcher, fetcher.partitionMap)
			}
			fetcher.removePartitions(topicPartitionsToRemove)
			if Logger.IsAllowed(DebugLevel) {
				Debugf(m, "Fetcher %s parition map after obsolete partitions removal", fetcher, fetcher.partitionMap)
			}
		}
		for tp := range m.partitionMap {
			m.disconnectChannelsForPartition <- tp
			delete(m.partitionMap, tp)
		}
		m.shutdownIdleFetchers()

		//updating partitions map with requested partitions
		for k, v := range newPartitionMap {
			m.partitionMap[k] = v
		}
		m.addFetcherForPartitions(partitionInfos)

		m.updateInProgress = false
		if Logger.IsAllowed(DebugLevel) {
			Debugf(m, "Applied new partition map %v", m.partitionMap)
		}
	})

	if Logger.IsAllowed(DebugLevel) {
		Debug(m, "Notifying all waiters about completed update")
	}
	m.updatedCond.Broadcast()
}

func (m *consumerFetcherManager) addFetcherForPartitions(partitionInfos map[TopicAndPartition]*partitionTopicInfo) {
	if Logger.IsAllowed(InfoLevel) {
		Infof(m, "Adding fetcher for partitions %v", partitionInfos)
	}
	partitionsPerFetcher := make(map[int]map[TopicAndPartition]*partitionTopicInfo)
	for topicAndPartition, info := range partitionInfos {
		fetcherId := m.getFetcherId(topicAndPartition.Topic, topicAndPartition.Partition)
		if partitionsPerFetcher[fetcherId] == nil {
			partitionsPerFetcher[fetcherId] = make(map[TopicAndPartition]*partitionTopicInfo)
		}
		partitionsPerFetcher[fetcherId][topicAndPartition] = info
	}

	if Logger.IsAllowed(DebugLevel) {
		Debugf(m, "partitionsPerFetcher: %v", partitionsPerFetcher)
	}
	for fetcherId, partitionInfos := range partitionsPerFetcher {
		if m.fetcherRoutineMap[fetcherId] == nil {
			if Logger.IsAllowed(DebugLevel) {
				Debugf(m, "Starting new fetcher")
			}
			fetcherRoutine := newConsumerFetcher(m,
				fmt.Sprintf("ConsumerFetcherRoutine-%s-%d", m.config.Consumerid, fetcherId))
			m.fetcherRoutineMap[fetcherId] = fetcherRoutine
			go fetcherRoutine.start()
		}

		m.fetcherRoutineMap[fetcherId].addPartitions(partitionInfos)
	}
}

func (m *consumerFetcherManager) getFetcherId(topic string, partitionId int32) int {
	return int(math.Abs(float64(31*hash(topic)+partitionId))) % int(m.numStreams)
}

func (m *consumerFetcherManager) shutdownIdleFetchers() {
	if Logger.IsAllowed(DebugLevel) {
		Debug(m, "Shutting down idle fetchers")
	}
	for key, fetcher := range m.fetcherRoutineMap {
		if len(fetcher.partitionMap) <= 0 {
			Debugf(m, "There is idle fetcher: %s", fetcher)
			<-fetcher.close()
			delete(m.fetcherRoutineMap, key)
		}
	}
	if Logger.IsAllowed(DebugLevel) {
		Debug(m, "Closed idle fetchers")
	}
}

func (m *consumerFetcherManager) closeAllFetchers() {
	Info(m, "Closing fetchers")
	inWriteLock(&m.updateLock, func() {
		Debug(m, "Stopping all message buffers and removing paritions")
		for k, v := range m.partitionMap {
			v.Buffer.stop()
			delete(m.partitionMap, k)
		}

		Debugf(m, "Trying to close %d fetchers", len(m.fetcherRoutineMap))
		for key, fetcher := range m.fetcherRoutineMap {
			Debugf(m, "Closing %s", fetcher)
			<-fetcher.close()
			Debugf(m, "Closed %s", fetcher)
			delete(m.fetcherRoutineMap, key)
		}
	})
}

func (m *consumerFetcherManager) close() <-chan bool {
	Info(m, "Closing manager")
	go func() {
		Info(m, "Setting shutdown flag")
		m.shuttingDown = true
		m.closeAllFetchers()
		m.updatedCond.Broadcast()
		m.partitionMap = nil
		m.closeFinished <- true
		Info(m, "Successfully closed all fetcher manager routines")
	}()

	return m.closeFinished
}

type consumerFetcherRoutine struct {
	manager       *consumerFetcherManager
	name          string
	partitionMap  map[TopicAndPartition]*partitionTopicInfo
	lock          sync.RWMutex
	closeFinished chan bool
	fetchStopper  chan bool
	askNext       chan TopicAndPartition
}

func (f *consumerFetcherRoutine) String() string {
	return f.name
}

func newConsumerFetcher(m *consumerFetcherManager, name string) *consumerFetcherRoutine {
	return &consumerFetcherRoutine{
		manager:       m,
		name:          name,
		partitionMap:  make(map[TopicAndPartition]*partitionTopicInfo),
		closeFinished: make(chan bool),
		fetchStopper:  make(chan bool),
		askNext:       make(chan TopicAndPartition, m.config.AskNextChannelSize),
	}
}

func (f *consumerFetcherRoutine) start() {
	if Logger.IsAllowed(InfoLevel) {
		Info(f, "Fetcher started")
	}
	for {
		if Logger.IsAllowed(TraceLevel) {
			Trace(f, "Waiting for asknext or die")
		}
		ts := time.Now()
		select {
		case nextTopicPartition := <-f.askNext:
			{
				timestamp := time.Now().UnixNano() / int64(time.Millisecond)
				f.manager.metrics.fetchersIdle().Update(time.Since(ts))
				if Logger.IsAllowed(DebugLevel) {
					Debugf(f, "Received asknext for %s", &nextTopicPartition)
				}
				inReadLock(&f.lock, func() {
					if !f.manager.shuttingDown {
						if Logger.IsAllowed(DebugLevel) {
							Debugf(f, "Partition map: %v", f.partitionMap)
						}
						if _, exists := f.partitionMap[nextTopicPartition]; !exists {
							if Logger.IsAllowed(WarnLevel) {
								Warnf(f, "Message buffer for partition %s has been terminated. Aborting processing task...", nextTopicPartition)
							}
							return
						}
						offset := f.partitionMap[nextTopicPartition].FetchedOffset

						var messages []*Message
						var err error
						f.manager.metrics.fetchDuration().Time(func() {
							messages, err = f.manager.client.Fetch(nextTopicPartition.Topic, nextTopicPartition.Partition, offset)
						})

						if err != nil {
							if offset > -1 { // Negative offsets are obviously out of range but don't spam the logs...
								if f.manager.client.IsOffsetOutOfRange(err) {
									Warnf(f, "Current offset %d for topic %s and partition %s is out of range.", offset, nextTopicPartition.Topic, nextTopicPartition.Partition)
									f.handleOffsetOutOfRange(&nextTopicPartition)
								} else {
									Warnf(f, "Got a fetch error for topic %s, partition %d: %s", nextTopicPartition.Topic, nextTopicPartition.Partition, err)
									//TODO new backoff type?
									time.Sleep(1 * time.Second)
								}
							}
						}

						if f.manager.config.Debug {
							for _, message := range messages {
								message.DecodedKey = append([]int64{timestamp}, message.DecodedKey.([]int64)...)
							}
						}

						f.processPartitionData(nextTopicPartition, messages)
					}
				})
			}
		case <-f.fetchStopper:
			{
				if Logger.IsAllowed(InfoLevel) {
					Info(f, "Stopped fetcher")
				}
				return
			}
		}
	}
}

func (f *consumerFetcherRoutine) addPartitions(partitionTopicInfos map[TopicAndPartition]*partitionTopicInfo) {
	if Logger.IsAllowed(DebugLevel) {
		Debugf(f, "Adding partitions: %v", partitionTopicInfos)
	}
	newPartitions := make(map[TopicAndPartition]chan TopicAndPartition)
	inWriteLock(&f.lock, func() {
		for topicAndPartition, info := range partitionTopicInfos {
			if _, contains := f.partitionMap[topicAndPartition]; !contains {
				f.partitionMap[topicAndPartition] = info
				validOffset := info.FetchedOffset + 1
				if isOffsetInvalid(info.FetchedOffset) {
					f.handleOffsetOutOfRange(&topicAndPartition)
				} else {
					f.partitionMap[topicAndPartition].FetchedOffset = validOffset
				}
				f.partitionMap[topicAndPartition].Buffer.start(f.askNext)
				newPartitions[topicAndPartition] = f.askNext
				if Logger.IsAllowed(DebugLevel) {
					Debugf(f, "Owner of %s", topicAndPartition)
				}
			}
		}
	})

	for topicAndPartition, askNext := range newPartitions {
		if Logger.IsAllowed(DebugLevel) {
			Debugf(f, "Sending ask next to %s for %s", f, topicAndPartition)
		}
	Loop:
		for {
			timeout := time.NewTimer(1 * time.Second)
			select {
			case askNext <- topicAndPartition:
				timeout.Stop()
				break Loop
			case <-timeout.C:
				{
					if f.manager.shuttingDown {
						return
					}
				}
			}
		}
		if Logger.IsAllowed(DebugLevel) {
			Debugf(f, "Sent ask next to %s for %s", f, topicAndPartition)
		}
	}
}

func (f *consumerFetcherRoutine) processPartitionData(topicAndPartition TopicAndPartition, messages []*Message) {
	if Logger.IsAllowed(TraceLevel) {
		Trace(f, "Trying to acquire lock for partition processing")
		Tracef(f, "Processing partition data for %s", topicAndPartition)
	}
	if len(messages) > 0 {
		f.partitionMap[topicAndPartition].FetchedOffset = messages[len(messages)-1].Offset + 1
	}
	go f.partitionMap[topicAndPartition].Buffer.addBatch(messages)
	if Logger.IsAllowed(TraceLevel) {
		Tracef(f, "Sent partition data to %s", topicAndPartition)
	}
}

func (f *consumerFetcherRoutine) handleOffsetOutOfRange(topicAndPartition *TopicAndPartition) {
	newOffset, err := f.manager.client.GetAvailableOffset(topicAndPartition.Topic, topicAndPartition.Partition, f.manager.config.AutoOffsetReset)
	if err != nil {
		Errorf(f, "Cannot get available offset for %s. Reason: %s", topicAndPartition, err)
		return
	}

	// Do not use a lock here just because it's faster and it will be checked afterwards if we should still fetch that TopicPartition
	// This just guarantees we dont get a nil pointer dereference here
	if topicInfo, exists := f.partitionMap[*topicAndPartition]; exists {
		topicInfo.FetchedOffset = newOffset
		f.partitionMap[*topicAndPartition].FetchedOffset = newOffset
	}
}

func (f *consumerFetcherRoutine) removeAllPartitions() {
	partitions := make([]TopicAndPartition, 0)
	for topicPartition, _ := range f.partitionMap {
		partitions = append(partitions, topicPartition)
	}
	f.removePartitions(partitions)
}

func (f *consumerFetcherRoutine) removePartitions(partitions []TopicAndPartition) {
	if Logger.IsAllowed(DebugLevel) {
		Debug(f, "Remove partitions")
	}
	inWriteLock(&f.lock, func() {
		for _, topicAndPartition := range partitions {
			delete(f.partitionMap, topicAndPartition)
		}
	})
}

func (f *consumerFetcherRoutine) close() <-chan bool {
	Info(f, "Closing fetcher")
	go func() {
		f.fetchStopper <- true
		f.removeAllPartitions()
		Debug(f, "Sending close finished")
		f.closeFinished <- true
		Debug(f, "Sent close finished")
	}()
	return f.closeFinished
}
