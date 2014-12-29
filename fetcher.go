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
	"github.com/Shopify/sarama"
	metrics "github.com/rcrowley/go-metrics"
	"math"
	"sync"
	"time"
)

type consumerFetcherManager struct {
	config                *ConsumerConfig
	numStreams            int
	closeFinished         chan bool
	partitionMapLock      sync.Mutex
	fetcherRoutineMapLock sync.Mutex
	partitionMap          map[TopicAndPartition]*partitionTopicInfo
	fetcherRoutineMap     map[brokerAndFetcherId]*consumerFetcherRoutine
	noLeaderPartitions    []TopicAndPartition
	shuttingDown          bool
	leaderCond            *sync.Cond
	askNext               chan TopicAndPartition
	askNextStopper        chan bool
	askNextFetchers       map[TopicAndPartition]chan TopicAndPartition
	askNextFetchersLock   sync.RWMutex
	isReady               bool
	isReadyLock           sync.RWMutex

	numFetchRoutinesCounter metrics.Counter
	idleTimer               metrics.Timer
	fetchDurationTimer      metrics.Timer

	switchTopic    chan bool
	fetcherBarrier *barrier
}

func (m *consumerFetcherManager) String() string {
	return fmt.Sprintf("%s-manager", m.config.Consumerid)
}

func newConsumerFetcherManager(config *ConsumerConfig, askNext chan TopicAndPartition, fetcherBarrier *barrier) *consumerFetcherManager {
	manager := &consumerFetcherManager{
		config:             config,
		closeFinished:      make(chan bool),
		partitionMap:       make(map[TopicAndPartition]*partitionTopicInfo),
		fetcherRoutineMap:  make(map[brokerAndFetcherId]*consumerFetcherRoutine),
		noLeaderPartitions: make([]TopicAndPartition, 0),
		askNext:            askNext,
		askNextStopper:     make(chan bool),
		askNextFetchers:    make(map[TopicAndPartition]chan TopicAndPartition),
		switchTopic:        make(chan bool),
		fetcherBarrier:     fetcherBarrier,
	}
	manager.leaderCond = sync.NewCond(&manager.partitionMapLock)
	manager.numFetchRoutinesCounter = metrics.NewRegisteredCounter(fmt.Sprintf("NumFetchRoutines-%s", manager.String()), metrics.DefaultRegistry)
	manager.idleTimer = metrics.NewRegisteredTimer(fmt.Sprintf("FetchersIdleTime-%s", manager.String()), metrics.DefaultRegistry)
	manager.fetchDurationTimer = metrics.NewRegisteredTimer(fmt.Sprintf("FetchDuration-%s", manager.String()), metrics.DefaultRegistry)

	go manager.findLeaders()
	go manager.waitForNextRequests()

	return manager
}

func (m *consumerFetcherManager) notReady() {
	inWriteLock(&m.isReadyLock, func() {
		m.isReady = false
	})
}

func (m *consumerFetcherManager) startConnections(topicInfos []*partitionTopicInfo, numStreams int) {
	Trace(m, "Fetcher Manager started")
	Debugf(m, "TopicInfos = %s", topicInfos)
	m.numStreams = numStreams

	inLock(&m.partitionMapLock, func() {
		inWriteLock(&m.isReadyLock, func() {
			newPartitionMap := make(map[TopicAndPartition]*partitionTopicInfo)
			for _, info := range topicInfos {
				topicAndPartition := TopicAndPartition{info.Topic, info.Partition}
				newPartitionMap[topicAndPartition] = info

				isAlreadyUp := false
				inReadLock(&m.askNextFetchersLock, func() {
					_, isAlreadyUp = m.askNextFetchers[topicAndPartition]
				})
				if isAlreadyUp {
					continue
				}
				//				if _, isAlreadyUp := m.askNextFetchers[topicAndPartition]; isAlreadyUp { continue }
				exists := false
				for _, noLeader := range m.noLeaderPartitions {
					if topicAndPartition == noLeader {
						exists = true
						break
					}
				}
				if !exists {
					Tracef(m, "Adding no leader partition = %s", topicAndPartition)
					m.noLeaderPartitions = append(m.noLeaderPartitions, topicAndPartition)
				}
			}

			Tracef(m, "Got new list of partitions to process %v", newPartitionMap)
			Tracef(m, "All partitions map: %v", m.partitionMap)
			//			//receive obsolete partitions map
			for k := range newPartitionMap {
				delete(m.partitionMap, k)
			}
			//receive unnecessary partitions list for fetcher cleanup, stopping obsolete message buffers
			topicPartitionsToRemove := make([]TopicAndPartition, 0)
			for tp := range m.partitionMap {
				topicPartitionsToRemove = append(topicPartitionsToRemove, tp)
				Tracef(m, "Stopping buffer: %s", m.partitionMap[tp].Buffer)
				m.partitionMap[tp].Buffer.stop()
				delete(m.partitionMap, tp)
			}
			Tracef(m, "There are obsolete partitions %v", topicPartitionsToRemove)

			//removing unnecessary partition-fetchRoutine bindings
			inLock(&m.fetcherRoutineMapLock, func() {
				for _, fetcher := range m.fetcherRoutineMap {
					Tracef(m, "Fetcher %s parition map before obsolete partitions removal", fetcher, fetcher.partitionMap)
					fetcher.removePartitions(topicPartitionsToRemove)
					Tracef(m, "Fetcher %s parition map after obsolete partitions removal", fetcher, fetcher.partitionMap)
				}
			})
			//updating partitions map with requested partitions
			for k, v := range newPartitionMap {
				m.partitionMap[k] = v
			}
			Tracef(m, "Applied new partition map %v", m.partitionMap)
			m.isReady = true
		})
	})

	Trace(m, "Broadcasting")
	m.leaderCond.Broadcast()
}

func (m *consumerFetcherManager) waitForNextRequests() {
	for {
		select {
		case <-m.switchTopic:
			{
				for _, routine := range m.fetcherRoutineMap {
					routine.switchRequested = true
				}
			}
		case topicPartition := <-m.askNext:
			{
				Tracef(m, "WaitForNextRequests: got asknext for partition=%d", topicPartition.Partition)
				inReadLock(&m.isReadyLock, func() {
					if m.isReady {
						Tracef(m, "Manager ready, asking next for %s", topicPartition)
						inReadLock(&m.askNextFetchersLock, func() {
							if nextChannel, exists := m.askNextFetchers[topicPartition]; exists {
								nextChannel <- topicPartition
								Tracef(m, "Manager ready, asked next for %s", topicPartition)
							} else {
								Warnf(m, "Received askNext for wrong partition %s", topicPartition)
							}
						})
					}
				})
			}
		case <-m.askNextStopper:
			return
		}
	}
}

func (m *consumerFetcherManager) findLeaders() {
	for {
		Trace(m, "Find leaders")
		leaderForPartitions := make(map[TopicAndPartition]*BrokerInfo)
		inLock(&m.partitionMapLock, func() {
			for len(m.noLeaderPartitions) == 0 {
				if m.shuttingDown {
					return
				}
				Trace(m, "No partition for leader election")
				m.leaderCond.Wait()
				Trace(m, "Got broadcast event")
			}

			Infof(m, "Partitions without leader %v\n", m.noLeaderPartitions)
			brokers, err := m.config.Coordinator.GetAllBrokers()
			if err != nil {
				panic(err)
			}
			topicsMetadata := m.fetchTopicMetadata(m.distinctTopics(), brokers, m.config.Clientid).Topics
			for _, meta := range topicsMetadata {
				topic := meta.Name
				for _, partition := range meta.Partitions {
					topicAndPartition := TopicAndPartition{topic, partition.ID}

					var leaderBroker *BrokerInfo = nil
					for _, broker := range brokers {
						if broker.Id == partition.Leader {
							leaderBroker = broker
							break
						}
					}

					for i, tp := range m.noLeaderPartitions {
						if tp == topicAndPartition && leaderBroker != nil {
							leaderForPartitions[topicAndPartition] = leaderBroker
							m.noLeaderPartitions = append(m.noLeaderPartitions[:i], m.noLeaderPartitions[i+1:]...)
							break
						}
					}
				}
			}
		})

		if m.shuttingDown {
			Info(m, "Stopping find leaders routine")
			return
		}

		partitionAndOffsets := make(map[TopicAndPartition]*brokerAndInitialOffset)
		for topicAndPartition, broker := range leaderForPartitions {
			partitionAndOffsets[topicAndPartition] = &brokerAndInitialOffset{broker, m.partitionMap[topicAndPartition].FetchedOffset}
		}
		m.addFetcherForPartitions(partitionAndOffsets)

		m.shutdownIdleFetchers()
		time.Sleep(m.config.RefreshLeaderBackoff)
	}
}

func (m *consumerFetcherManager) fetchTopicMetadata(topics []string, brokers []*BrokerInfo, clientId string) *sarama.MetadataResponse {
	shuffledBrokers := make([]*BrokerInfo, len(brokers))
	shuffleArray(&brokers, &shuffledBrokers)
	for i := 0; i < len(shuffledBrokers); i++ {
		for j := 0; j <= m.config.FetchTopicMetadataRetries; j++ {
			brokerAddr := fmt.Sprintf("%s:%d", shuffledBrokers[i].Host, shuffledBrokers[i].Port)
			broker := sarama.NewBroker(brokerAddr)
			err := broker.Open(newSaramaBrokerConfig(m.config))
			if err != nil {
				Warnf(m, "Could not fetch topic metadata from broker %s\n", brokerAddr)
				time.Sleep(m.config.FetchTopicMetadataBackoff)
				continue
			}
			defer broker.Close()

			request := sarama.MetadataRequest{Topics: topics}
			response, err := broker.GetMetadata(clientId, &request)
			if err != nil {
				Warnf(m, "Could not fetch topic metadata from broker %s\n", brokerAddr)
				time.Sleep(m.config.FetchTopicMetadataBackoff)
				continue
			}
			return response
		}
	}

	panic(fmt.Sprintf("fetching topic metadata for topics [%s] from broker [%s] failed", topics, shuffledBrokers))
}

func (m *consumerFetcherManager) distinctTopics() []string {
	topics := make([]string, len(m.noLeaderPartitions))

	i := 0
	for j := 0; j < len(m.noLeaderPartitions); j++ {
		current := m.noLeaderPartitions[j]
		exists := false
		for k := 0; k < len(topics); k++ {
			if current.Topic == topics[k] {
				exists = true
				break
			}
		}
		if !exists {
			topics[i] = current.Topic
			i++
		}
	}

	return topics[:i]
}

func (m *consumerFetcherManager) addFetcherForPartitions(partitionAndOffsets map[TopicAndPartition]*brokerAndInitialOffset) {
	Infof(m, "Adding fetcher for partitions %v", partitionAndOffsets)
	inLock(&m.fetcherRoutineMapLock, func() {

		//TODO map key contains a pointer, inspect this!
		partitionsPerFetcher := make(map[brokerAndFetcherId]map[TopicAndPartition]*brokerAndInitialOffset)
		for topicAndPartition, brokerAndOffset := range partitionAndOffsets {
			brokerAndFetcher := brokerAndFetcherId{brokerAndOffset.Broker, m.getFetcherId(topicAndPartition.Topic, topicAndPartition.Partition)}
			if partitionsPerFetcher[brokerAndFetcher] == nil {
				partitionsPerFetcher[brokerAndFetcher] = make(map[TopicAndPartition]*brokerAndInitialOffset)
			}
			partitionsPerFetcher[brokerAndFetcher][topicAndPartition] = brokerAndOffset
		}
		Debugf(m, "partitionsPerFetcher: %v", partitionsPerFetcher)
		for brokerAndFetcherId, partitionOffsets := range partitionsPerFetcher {
			if m.fetcherRoutineMap[brokerAndFetcherId] == nil {
				Tracef(m, "Starting new fetcher")
				fetcherRoutine := newConsumerFetcher(m,
					fmt.Sprintf("ConsumerFetcherRoutine-%s-%d-%d", m.config.Consumerid, brokerAndFetcherId.FetcherId, brokerAndFetcherId.Broker.Id),
					brokerAndFetcherId.Broker,
					m.partitionMap, m.fetcherBarrier)
				m.fetcherRoutineMap[brokerAndFetcherId] = fetcherRoutine
				go fetcherRoutine.start()
			}

			partitionToOffsetMap := make(map[TopicAndPartition]int64)
			for tp, b := range partitionOffsets {
				partitionToOffsetMap[tp] = b.InitOffset
			}
			m.fetcherRoutineMap[brokerAndFetcherId].addPartitions(partitionToOffsetMap)
		}

	})
}

func (m *consumerFetcherManager) addPartitionsWithError(partitions []TopicAndPartition) {
	Info(m, "Adding partitions with error")
	inLock(&m.partitionMapLock, func() {
		if m.partitionMap != nil {
			for _, topicAndPartition := range partitions {
				exists := false
				for _, noLeaderPartition := range m.noLeaderPartitions {
					if topicAndPartition == noLeaderPartition {
						exists = true
						break
					}
				}
				if !exists {
					m.noLeaderPartitions = append(m.noLeaderPartitions, topicAndPartition)
				}
			}
			m.leaderCond.Broadcast()
		}
	})
}

func (m *consumerFetcherManager) getFetcherId(topic string, partitionId int32) int {
	return int(math.Abs(float64(31*hash(topic)+partitionId))) % int(m.numStreams)
}

func (m *consumerFetcherManager) shutdownIdleFetchers() {
	Trace(m, "Shutting down idle fetchers")
	inLock(&m.fetcherRoutineMapLock, func() {
		for key, fetcher := range m.fetcherRoutineMap {
			if len(fetcher.partitionMap) <= 0 {
				<-fetcher.close()
				delete(m.fetcherRoutineMap, key)
				m.fetcherBarrier.reset(m.fetcherBarrier.size - 1)
			}
		}
	})
	Trace(m, "Closed idle fetchers")
}

func (m *consumerFetcherManager) closeAllFetchers() {
	Info(m, "Closing fetchers")
	m.notReady()
	inLock(&m.partitionMapLock, func() {
		Debugf(m, "Trying to close %d fetchers", len(m.fetcherRoutineMap))
		for _, fetcher := range m.fetcherRoutineMap {
			Tracef(m, "Closing %s", fetcher)
			<-fetcher.close()
			Tracef(m, "Closed %s", fetcher)
		}

		for key := range m.fetcherRoutineMap {
			delete(m.fetcherRoutineMap, key)
		}

		for k := range m.partitionMap {
			delete(m.partitionMap, k)
		}
	})
}

func (m *consumerFetcherManager) close() <-chan bool {
	Info(m, "Closing manager")
	go func() {
		Info(m, "Stopping find leader")
		m.notReady()
		m.shuttingDown = true
		m.askNextStopper <- true
		m.leaderCond.Broadcast()
		m.closeAllFetchers()
		m.partitionMap = nil
		m.noLeaderPartitions = nil
		m.closeFinished <- true
	}()

	return m.closeFinished
}

type consumerFetcherRoutine struct {
	manager           *consumerFetcherManager
	name              string
	broker            *BrokerInfo
	brokerAddr        string //just not to calculate each time
	brokerConn        *sarama.Broker
	allPartitionMap   map[TopicAndPartition]*partitionTopicInfo
	partitionMap      map[TopicAndPartition]int64
	partitionMapLock  sync.Mutex
	closeFinished     chan bool
	fetchStopper      chan bool
	askNext           chan TopicAndPartition
	fetcherBarrier    *barrier
	switchRequested   bool
	switchFetchesUsed int
}

func (f *consumerFetcherRoutine) String() string {
	return f.name
}

func newConsumerFetcher(m *consumerFetcherManager, name string, broker *BrokerInfo, allPartitionMap map[TopicAndPartition]*partitionTopicInfo, fetcherBarrier *barrier) *consumerFetcherRoutine {
	return &consumerFetcherRoutine{
		manager:         m,
		name:            name,
		broker:          broker,
		brokerAddr:      fmt.Sprintf("%s:%d", broker.Host, broker.Port),
		allPartitionMap: allPartitionMap,
		partitionMap:    make(map[TopicAndPartition]int64),
		closeFinished:   make(chan bool),
		fetchStopper:    make(chan bool),
		askNext:         make(chan TopicAndPartition),
		fetcherBarrier:  fetcherBarrier,
	}
}

func (f *consumerFetcherRoutine) start() {
	Info(f, "Fetcher started")
	for {
		Debug(f, "Waiting for asknext or die")
		ts := time.Now()
		select {
		case nextTopicPartition := <-f.askNext:
			{
				f.manager.idleTimer.Update(time.Since(ts))
				Debugf(f, "Received asknext for %s", &nextTopicPartition)
				config := f.manager.config
				inReadLock(&f.manager.isReadyLock, func() {
					if f.manager.isReady {
						Debug(f, "Next asked")
						offset := InvalidOffset
						inLock(&f.partitionMapLock, func() {
							Debugf(f, "Partition map: %v", f.partitionMap)
							if existingOffset, exists := f.partitionMap[nextTopicPartition]; exists {
								offset = existingOffset
							}
						})
						if isOffsetInvalid(offset) {
							return
						}

						fetchRequest := new(sarama.FetchRequest)
						fetchRequest.MinBytes = config.FetchMinBytes
						fetchRequest.MaxWaitTime = config.FetchWaitMaxMs
						Infof(f, "Adding block: topic=%s, partition=%d, offset=%d, fetchsize=%d", nextTopicPartition.Topic, int32(nextTopicPartition.Partition), offset, f.manager.config.FetchMessageMaxBytes)
						fetchRequest.AddBlock(nextTopicPartition.Topic, int32(nextTopicPartition.Partition), offset, f.manager.config.FetchMessageMaxBytes)

						var hasMessages bool
						f.manager.fetchDurationTimer.Time(func() { hasMessages = f.processFetchRequest(fetchRequest, offset) })

						if !hasMessages {
							if f.switchRequested {
								f.switchFetchesUsed++
								if config.FetchMaxRetries > f.switchFetchesUsed {
									f.removePartitions([]TopicAndPartition{nextTopicPartition})
								} else {
									go f.requeue(nextTopicPartition)
								}
							} else {
								go f.requeue(nextTopicPartition)
							}
						}
					}
				})
				if f.switchRequested && config.FetchMaxRetries > f.switchFetchesUsed {
					if len(f.partitionMap) == 0 {
						Debug(f, "fetcher started awaiting for barrier break")
						f.fetcherBarrier.await()
					}
				}
				time.Sleep(f.manager.config.FetchRequestBackoff)
			}
		case <-f.fetchStopper:
			{
				Debug(f, "Stopped fetcher")
				return
			}
		}
	}
}

func (f *consumerFetcherRoutine) requeue(topicPartition TopicAndPartition) {
	Debug(f, "Asknext received no messages, requeue request")
	time.Sleep(f.manager.config.RequeueAskNextBackoff)
	f.askNext <- topicPartition
	Debug(f, "Requeued request")
}

func (f *consumerFetcherRoutine) addPartitions(partitionAndOffsets map[TopicAndPartition]int64) {
	Infof(f, "Adding partitions: %v", partitionAndOffsets)
	newPartitions := make(map[TopicAndPartition]chan TopicAndPartition)
	inLock(&f.partitionMapLock, func() {
		for topicAndPartition, offset := range partitionAndOffsets {
			if _, contains := f.partitionMap[topicAndPartition]; !contains {
				validOffset := offset + 1
				if isOffsetInvalid(offset) {
					validOffset = f.handleOffsetOutOfRange(&topicAndPartition)
				}
				f.partitionMap[topicAndPartition] = validOffset
				inWriteLock(&f.manager.askNextFetchersLock, func() {
					f.manager.askNextFetchers[topicAndPartition] = f.askNext
				})
				newPartitions[topicAndPartition] = f.askNext
				Debugf(f, "Owner of %s", topicAndPartition)
			}
		}
	})
	for topicAndPartition, askNext := range newPartitions {
		Debugf(f, "Sending ask next to %s for %s", f, topicAndPartition)
		askNext <- topicAndPartition
	}
}

func (f *consumerFetcherRoutine) processFetchRequest(request *sarama.FetchRequest, requestedOffset int64) bool {
	Info(f, "Started processing fetch request")
	hasMessages := false
	partitionsWithError := make(map[TopicAndPartition]bool)

	if f.brokerConn == nil {
		f.brokerConn = sarama.NewBroker(f.brokerAddr)
		err := f.brokerConn.Open(newSaramaBrokerConfig(f.manager.config))
		if err != nil {
			f.handleFetchError(request, err, partitionsWithError)
		}
	}

	response, err := f.brokerConn.Fetch(f.manager.config.Clientid, request)
	if err != nil {
		f.handleFetchError(request, err, partitionsWithError)
	}

	if response != nil {
		Trace(f, "Processing fetch request")
		inLock(&f.partitionMapLock, func() {
			for topic, partitionAndData := range response.Blocks {
				for partition, data := range partitionAndData {
					topicAndPartition := TopicAndPartition{topic, partition}
					if currentOffset, exists := f.partitionMap[topicAndPartition]; exists {
						switch data.Err {
						case sarama.NoError:
							{
								messages := data.MsgSet.Messages
								newOffset := currentOffset
								if len(messages) > 0 {
									hasMessages = true
									newOffset = messages[len(messages)-1].Offset + 1
								}
								f.partitionMap[topicAndPartition] = newOffset
								filterPartitionData(data, requestedOffset)
								go f.processPartitionData(topicAndPartition, currentOffset, data)
							}
						case sarama.OffsetOutOfRange:
							{
								newOffset := f.handleOffsetOutOfRange(&topicAndPartition)
								f.partitionMap[topicAndPartition] = newOffset
								Warnf(f, "Current offset %d for partition %s is out of range. Reset offset to %d\n", currentOffset, topicAndPartition, newOffset)
							}
						default:
							{
								Errorf(f, "Error for partition %s. Removing. Cause: %s", topicAndPartition, data.Err)
								partitionsWithError[topicAndPartition] = true
							}
						}
					}
				}
			}
		})
	}

	if len(partitionsWithError) > 0 {
		Warn(f, "Handling partitions with error")
		partitionsWithErrorSet := make([]TopicAndPartition, 0, len(partitionsWithError))
		for k := range partitionsWithError {
			partitionsWithErrorSet = append(partitionsWithErrorSet, k)
		}
		f.handlePartitionsWithErrors(partitionsWithErrorSet)
	}

	return hasMessages
}

func filterPartitionData(partitionData *sarama.FetchResponseBlock, requestedOffset int64) {
	lowestCorrectIndex := 0
	for i, v := range partitionData.MsgSet.Messages {
		if v.Offset < requestedOffset {
			lowestCorrectIndex = i + 1
		} else {
			break
		}
	}
	partitionData.MsgSet.Messages = partitionData.MsgSet.Messages[lowestCorrectIndex:]
}

func (f *consumerFetcherRoutine) processPartitionData(topicAndPartition TopicAndPartition, fetchOffset int64, partitionData *sarama.FetchResponseBlock) {
	Tracef(f, "Processing partition data for %s", topicAndPartition)

	partitionTopicInfo := f.allPartitionMap[topicAndPartition] //TODO this is potentially unsafe, maybe use allPartitionMapLock here?
	if len(partitionData.MsgSet.Messages) > 0 {
		partitionTopicInfo.Buffer.addBatch(&TopicPartitionData{topicAndPartition, partitionData})
		Info(f, "Sent partition data")
	} else {
		Debug(f, "Got empty message. Ignoring...")
	}
}

func (f *consumerFetcherRoutine) handleFetchError(request *sarama.FetchRequest, err error, partitionsWithError map[TopicAndPartition]bool) {
	Infof(f, "Error in fetch %v. Possible cause: %s\n", request, err)
	inLock(&f.partitionMapLock, func() {
		for k, _ := range f.partitionMap {
			partitionsWithError[k] = true
		}
	})
}

func (f *consumerFetcherRoutine) handleOffsetOutOfRange(topicAndPartition *TopicAndPartition) int64 {
	Tracef(f, "Handling offset out of range for %s", topicAndPartition)
	offsetTime := sarama.LatestOffsets
	if f.manager.config.AutoOffsetReset == SmallestOffset {
		offsetTime = sarama.EarliestOffset
	}

	newOffset := f.earliestOrLatestOffset(topicAndPartition, offsetTime)
	partitionTopicInfo := f.allPartitionMap[*topicAndPartition]
	partitionTopicInfo.FetchedOffset = newOffset

	return newOffset
}

func (f *consumerFetcherRoutine) handlePartitionsWithErrors(partitions []TopicAndPartition) {
	f.removePartitions(partitions)
	f.manager.addPartitionsWithError(partitions)
}

func (f *consumerFetcherRoutine) earliestOrLatestOffset(topicAndPartition *TopicAndPartition, offsetTime sarama.OffsetTime) int64 {
	client, err := sarama.NewClient(f.manager.config.Clientid, []string{f.brokerAddr}, nil)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	offset, err := client.GetOffset(topicAndPartition.Topic, int32(topicAndPartition.Partition), offsetTime)
	if err != nil {
		panic(err)
	}

	return offset
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
	inLock(&f.partitionMapLock, func() {
		for _, topicAndPartition := range partitions {
			delete(f.partitionMap, topicAndPartition)
			inWriteLock(&f.manager.askNextFetchersLock, func() {
				delete(f.manager.askNextFetchers, topicAndPartition)
			})
		}
	})
}

func (f *consumerFetcherRoutine) close() <-chan bool {
	Info(f, "Closing fetcher")
	go func() {
		f.fetchStopper <- true
		for tp, pti := range f.allPartitionMap {
			if _, exists := f.partitionMap[tp]; exists {
				Debugf(f, "Stopping %s", pti.Buffer)
				pti.Buffer.stop()
				Debugf(f, "Stopped %s", pti.Buffer)
			}
		}
		f.removeAllPartitions()
		Debug(f, "Sending close finished")
		f.brokerConn.Close()
		f.closeFinished <- true
		Debug(f, "Sent close finished")
	}()
	return f.closeFinished
}
