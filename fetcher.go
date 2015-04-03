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

	metrics *consumerMetrics
	client  LowLevelClient
}

func (m *consumerFetcherManager) String() string {
	return fmt.Sprintf("%s-manager", m.config.Consumerid)
}

func newConsumerFetcherManager(config *ConsumerConfig, disconnectChannelsForPartition chan TopicAndPartition, metrics *consumerMetrics) *consumerFetcherManager {
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
	Debug(m, "Fetcher Manager started")
	Debugf(m, "TopicInfos = %s", topicInfos)
	m.numStreams = numStreams

	partitionAndOffsets := make(map[TopicAndPartition]int64)

	m.updateInProgress = true
	inWriteLock(&m.updateLock, func() {
		Debug(m, "Updating fetcher configuration")
		newPartitionMap := make(map[TopicAndPartition]*partitionTopicInfo)
		for _, providedInfo := range topicInfos {
			topicAndPartition := TopicAndPartition{providedInfo.Topic, providedInfo.Partition}
			if currentInfo, alreadyFetching := m.partitionMap[topicAndPartition]; alreadyFetching {
				newPartitionMap[topicAndPartition] = currentInfo
				continue
			} else {
				newPartitionMap[topicAndPartition] = providedInfo
				partitionAndOffsets[topicAndPartition] = providedInfo.FetchedOffset
			}
		}

		Debugf(m, "Got new list of partitions to process %v", newPartitionMap)
		Debugf(m, "All partitions map: %v", m.partitionMap)
		//receive obsolete partitions map
		for k := range newPartitionMap {
			delete(m.partitionMap, k)
		}

		//removing obsolete partitions and tearing down associated jobs
		topicPartitionsToRemove := make([]TopicAndPartition, 0)
		for tp := range m.partitionMap {
			topicPartitionsToRemove = append(topicPartitionsToRemove, tp)
			m.disconnectChannelsForPartition <- tp
			delete(m.partitionMap, tp)
		}
		Debugf(m, "There are obsolete partitions %v", topicPartitionsToRemove)

		//removing unnecessary partition-fetchRoutine bindings
		for _, fetcher := range m.fetcherRoutineMap {
			Debugf(m, "Fetcher %s parition map before obsolete partitions removal", fetcher, fetcher.partitionMap)
			fetcher.removePartitions(topicPartitionsToRemove)
			Debugf(m, "Fetcher %s parition map after obsolete partitions removal", fetcher, fetcher.partitionMap)
		}
		m.shutdownIdleFetchers()

		//updating partitions map with requested partitions
		for k, v := range newPartitionMap {
			m.partitionMap[k] = v
		}
		m.addFetcherForPartitions(partitionAndOffsets)

		m.updateInProgress = false
		Debugf(m, "Applied new partition map %v", m.partitionMap)
	})

	Debug(m, "Notifying all waiters about completed update")
	m.updatedCond.Broadcast()
}

func (m *consumerFetcherManager) addFetcherForPartitions(partitionAndOffsets map[TopicAndPartition]int64) {
	Infof(m, "Adding fetcher for partitions %v", partitionAndOffsets)
	partitionsPerFetcher := make(map[int]map[TopicAndPartition]int64)
	for topicAndPartition, offset := range partitionAndOffsets {
		fetcherId := m.getFetcherId(topicAndPartition.Topic, topicAndPartition.Partition)
		if partitionsPerFetcher[fetcherId] == nil {
			partitionsPerFetcher[fetcherId] = make(map[TopicAndPartition]int64)
		}
		partitionsPerFetcher[fetcherId][topicAndPartition] = offset
	}

	Debugf(m, "partitionsPerFetcher: %v", partitionsPerFetcher)
	for fetcherId, partitionOffsets := range partitionsPerFetcher {
		if m.fetcherRoutineMap[fetcherId] == nil {
			Debugf(m, "Starting new fetcher")
			fetcherRoutine := newConsumerFetcher(m,
				fmt.Sprintf("ConsumerFetcherRoutine-%s-%d", m.config.Consumerid, fetcherId),
				m.partitionMap)
			m.fetcherRoutineMap[fetcherId] = fetcherRoutine
			go fetcherRoutine.start()
		}

		partitionToOffsetMap := make(map[TopicAndPartition]int64)
		for tp, offset := range partitionOffsets {
			partitionToOffsetMap[tp] = offset
		}
		m.fetcherRoutineMap[fetcherId].addPartitions(partitionToOffsetMap)
	}
}

func (m *consumerFetcherManager) getFetcherId(topic string, partitionId int32) int {
	return int(math.Abs(float64(31*hash(topic)+partitionId))) % int(m.numStreams)
}

func (m *consumerFetcherManager) shutdownIdleFetchers() {
	Debug(m, "Shutting down idle fetchers")
	for key, fetcher := range m.fetcherRoutineMap {
		if len(fetcher.partitionMap) <= 0 {
			Debugf(m, "There is idle fetcher: %s", fetcher)
			<-fetcher.close()
			delete(m.fetcherRoutineMap, key)
		}
	}
	Debug(m, "Closed idle fetchers")
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
	manager         *consumerFetcherManager
	name            string
	allPartitionMap map[TopicAndPartition]*partitionTopicInfo
	partitionMap    map[TopicAndPartition]int64
	lock            sync.Mutex
	closeFinished   chan bool
	fetchStopper    chan bool
	askNext         map[TopicAndPartition]chan bool
}

func (f *consumerFetcherRoutine) String() string {
	return f.name
}

func newConsumerFetcher(m *consumerFetcherManager, name string, allPartitionMap map[TopicAndPartition]*partitionTopicInfo) *consumerFetcherRoutine {
	return &consumerFetcherRoutine{
		manager:         m,
		name:            name,
		allPartitionMap: allPartitionMap,
		partitionMap:    make(map[TopicAndPartition]int64),
		closeFinished:   make(chan bool),
		fetchStopper:    make(chan bool),
		askNext:         make(map[TopicAndPartition]chan bool),
	}
}

func (f *consumerFetcherRoutine) start() {
	Info(f, "Fetcher started")
	for {
		if len(f.askNext) == 0 {
			time.Sleep(f.manager.config.FetchRequestBackoff)
		}

		Trace(f, "Waiting for asknext or die")
		ts := time.Now()
		for nextTopicPartition, askNext := range f.askNext {
			if askNext == nil {
				continue
			}

			select {
			case <-askNext:
				{
					f.manager.metrics.FetchersIdleTimer().Update(time.Since(ts))
					Debugf(f, "Received asknext for %s", &nextTopicPartition)
					inLock(&f.lock, func() {
						if !f.manager.shuttingDown {
							Trace(f, "Next asked")
							offset := InvalidOffset
							Debugf(f, "Partition map: %v", f.partitionMap)
							if existingOffset, exists := f.partitionMap[nextTopicPartition]; exists {
								offset = existingOffset
							}

							if f.allPartitionMap[nextTopicPartition] == nil {
								Warnf(f, "Message buffer for partition %s has been terminated. Aborting processing task...", nextTopicPartition)
								return
							}

							var messages []*Message
							var err error
							f.manager.metrics.FetchDurationTimer().Time(func() {
								messages, err = f.manager.client.Fetch(nextTopicPartition.Topic, nextTopicPartition.Partition, offset)
							})

							if err != nil {
								if f.manager.client.IsOffsetOutOfRange(err) {
									Warnf(f, "Current offset %d for topic %s and partition %s is out of range.", offset, nextTopicPartition.Topic, nextTopicPartition.Partition)
									f.handleOffsetOutOfRange(&nextTopicPartition)
								} else {
									Warnf(f, "Got a fetch error: %s", err)
									//TODO new backoff type?
									time.Sleep(1 * time.Second)
								}
							}

							go f.processPartitionData(nextTopicPartition, messages)

							if len(messages) == 0 {
								go f.requeue(nextTopicPartition)
							}
						}
					})
				}
			case <-time.After(f.manager.config.FetchRequestBackoff): continue
			case <-f.fetchStopper:
				{
					Info(f, "Stopped fetcher")
					return
				}
			}
		}
	}
}

func (f *consumerFetcherRoutine) requeue(topicPartition TopicAndPartition) {
	Debug(f, "Asknext received no messages, requeue request")
	f.askNext[topicPartition] <- true
	Debugf(f, "Requeued request %s", topicPartition)
}

func (f *consumerFetcherRoutine) addPartitions(partitionAndOffsets map[TopicAndPartition]int64) {
	Debugf(f, "Adding partitions: %v", partitionAndOffsets)
	newPartitions := make(map[TopicAndPartition]chan bool)
	inLock(&f.lock, func() {
		for topicAndPartition, offset := range partitionAndOffsets {
			if _, contains := f.partitionMap[topicAndPartition]; !contains {
				validOffset := offset + 1
				if isOffsetInvalid(offset) {
					f.handleOffsetOutOfRange(&topicAndPartition)
				} else {
					f.partitionMap[topicAndPartition] = validOffset
				}
				f.askNext[topicAndPartition] = make(chan bool)
				f.manager.partitionMap[topicAndPartition].Buffer.start(f.askNext[topicAndPartition])
				newPartitions[topicAndPartition] = f.askNext[topicAndPartition]
				Debugf(f, "Owner of %s", topicAndPartition)
			}
		}
	})

	for topicAndPartition, askNext := range newPartitions {
		Debugf(f, "Sending ask next to %s for %s", f, topicAndPartition)
	Loop:
		for {
			select {
			case askNext <- true:
				break Loop
			case <-time.After(1 * time.Second):
				{
					if f.manager.shuttingDown {
						return
					}
				}
			}
		}
		Debugf(f, "Sent ask next to %s for %s", f, topicAndPartition)
	}
}

func (f *consumerFetcherRoutine) processPartitionData(topicAndPartition TopicAndPartition, messages []*Message) {
	Trace(f, "Trying to acquire lock for partition processing")
	inReadLock(&f.manager.updateLock, func() {
		for f.manager.updateInProgress {
			f.manager.updatedCond.Wait()
		}
		Tracef(f, "Processing partition data for %s", topicAndPartition)
		if len(messages) > 0 {
			f.partitionMap[topicAndPartition] = messages[len(messages)-1].Offset + 1
			f.allPartitionMap[topicAndPartition].Buffer.addBatch(messages)
			Debugf(f, "Sent partition data to %s", topicAndPartition)
		} else {
			Trace(f, "Got empty message. Ignoring...")
		}
	})
}

func (f *consumerFetcherRoutine) handleOffsetOutOfRange(topicAndPartition *TopicAndPartition) {
	newOffset, err := f.manager.client.GetAvailableOffset(topicAndPartition.Topic, topicAndPartition.Partition, f.manager.config.AutoOffsetReset)
	if err != nil {
		Errorf(f, "Cannot get available offset for %s. Reason: %s", topicAndPartition, err)
		return
	}

	// Do not use a lock here just because it's faster and it will be checked afterwards if we should still fetch that TopicPartition
	// This just guarantees we dont get a nil pointer dereference here
	if topicInfo, exists := f.allPartitionMap[*topicAndPartition]; exists {
		topicInfo.FetchedOffset = newOffset
		f.partitionMap[*topicAndPartition] = newOffset
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
	Debug(f, "Remove partitions")
	inLock(&f.lock, func() {
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
