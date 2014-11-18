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
	"github.com/samuel/go-zookeeper/zk"
	"fmt"
	"time"
	"sync"
	"github.com/Shopify/sarama"
	"math"
)

type consumerFetcherManager struct {
	config *ConsumerConfig
	zkConn *zk.Conn
	numStreams              int
	stopWaitingNextRequests chan bool
	closeFinished           chan bool
	partitionMapLock        sync.Mutex
	fetcherRoutineMapLock   sync.Mutex
	partitionMap map[TopicAndPartition]*PartitionTopicInfo
	fetcherRoutineMap map[BrokerAndFetcherId]*consumerFetcherRoutine
	noLeaderPartitions      []TopicAndPartition
	shuttingDown            bool
	leaderCond *sync.Cond
	askNext                 chan TopicAndPartition
	askNextFetchers map[TopicAndPartition]chan TopicAndPartition
	stopLock                sync.Mutex
	isReady                 bool
}

func (m *consumerFetcherManager) String() string {
	return fmt.Sprintf("%s-manager", m.config.ConsumerId)
}

func newConsumerFetcherManager(config *ConsumerConfig, zkConn *zk.Conn, askNext chan TopicAndPartition) *consumerFetcherManager {
	manager := &consumerFetcherManager{
		config : config,
		zkConn : zkConn,
		stopWaitingNextRequests : make(chan bool),
		closeFinished : make(chan bool),
		partitionMap : make(map[TopicAndPartition]*PartitionTopicInfo),
		fetcherRoutineMap : make(map[BrokerAndFetcherId]*consumerFetcherRoutine),
		noLeaderPartitions : make([]TopicAndPartition, 0),
		askNext : askNext,
		askNextFetchers : make(map[TopicAndPartition]chan TopicAndPartition),
	}
	manager.leaderCond = sync.NewCond(&manager.partitionMapLock)
	go manager.FindLeaders()
	go manager.WaitForNextRequests()

	return manager
}

func (m *consumerFetcherManager) startConnections(topicInfos []*PartitionTopicInfo, numStreams int) {
	Info(m, "Fetcher Manager started")
	Debugf(m, "TopicInfos = %s", topicInfos)
	m.numStreams = numStreams
	m.isReady = true

	InLock(&m.partitionMapLock, func() {
		newPartitionMap := make(map[TopicAndPartition]*PartitionTopicInfo)
		for _, info := range topicInfos {
			topicAndPartition := TopicAndPartition{info.Topic, info.Partition}
			newPartitionMap[topicAndPartition] = info

			exists := false
			for _, noLeader := range m.noLeaderPartitions {
				if topicAndPartition == noLeader {
					exists = true
					break
				}
			}
			if !exists {
				Debugf(m, "Adding no leader partition = %s", topicAndPartition)
				m.noLeaderPartitions = append(m.noLeaderPartitions, topicAndPartition)
			}
		}
		for k, _ := range m.partitionMap {
			delete(m.partitionMap, k)
		}
		for k, v := range newPartitionMap {
			m.partitionMap[k] = v
		}
	})

	Debug(m, "Broadcasting")
	m.leaderCond.Broadcast()
}

func (m *consumerFetcherManager) WaitForNextRequests() {
	for {
		select {
		case <-m.stopWaitingNextRequests: return
		case topicPartition := <-m.askNext: {
			m.askNextFetchers[topicPartition] <- topicPartition
		}
		}
	}
}

func (m *consumerFetcherManager) FindLeaders() {
	for {
		Info(m, "Find leaders")
		leaderForPartitions := make(map[TopicAndPartition]*BrokerInfo)
		InLock(&m.partitionMapLock, func() {
			for len(m.noLeaderPartitions) == 0 {
				if m.shuttingDown {
					return
				}
				Info(m, "No partition for leader election")
				m.leaderCond.Wait()
				Debug(m, "Got broadcast event")
			}

			Infof(m, "Partitions without leader %v\n", m.noLeaderPartitions)
			brokers, err := GetAllBrokersInCluster(m.zkConn)
			if err != nil {
				panic(err)
			}
			topicsMetadata := m.fetchTopicMetadata(m.distinctTopics(), brokers, m.config.ClientId).Topics
			for _, meta := range topicsMetadata {
				topic := meta.Name
				for _, partition := range meta.Partitions {
					topicAndPartition := TopicAndPartition{topic, partition.ID }

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

		partitionAndOffsets := make(map[TopicAndPartition]*BrokerAndInitialOffset)
		for topicAndPartition, broker := range leaderForPartitions {
			partitionAndOffsets[topicAndPartition] = &BrokerAndInitialOffset{broker, m.partitionMap[topicAndPartition].FetchedOffset}
		}
		m.addFetcherForPartitions(partitionAndOffsets)

//		m.ShutdownIdleFetchers()
		time.Sleep(m.config.RefreshLeaderBackoff)
	}
}

func (m *consumerFetcherManager) fetchTopicMetadata(topics []string, brokers []*BrokerInfo, clientId string) *sarama.MetadataResponse {
	shuffledBrokers := make([]*BrokerInfo, len(brokers))
	ShuffleArray(&brokers, &shuffledBrokers)
	for i := 0; i < len(shuffledBrokers); i++ {
		brokerAddr := fmt.Sprintf("%s:%d", shuffledBrokers[i].Host, shuffledBrokers[i].Port)
		broker := sarama.NewBroker(brokerAddr)
		err := broker.Open(nil)
		if err != nil {
			Infof(m.config.ConsumerId, "Could not fetch topic metadata from broker %s\n", brokerAddr)
			continue
		}
		defer broker.Close()

		request := sarama.MetadataRequest{Topics: topics}
		response, err := broker.GetMetadata(clientId, &request)
		if err != nil {
			Infof(m.config.ConsumerId, "Could not fetch topic metadata from broker %s\n", brokerAddr)
			continue
		}
		return response
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

func (m *consumerFetcherManager) addFetcherForPartitions(partitionAndOffsets map[TopicAndPartition]*BrokerAndInitialOffset) {
	Infof(m, "Adding fetcher for partitions %v", partitionAndOffsets)
	InLock(&m.fetcherRoutineMapLock, func() {

		partitionsPerFetcher := make(map[BrokerAndFetcherId]map[TopicAndPartition]*BrokerAndInitialOffset)
		for topicAndPartition, brokerAndInitialOffset := range partitionAndOffsets {
			brokerAndFetcher := BrokerAndFetcherId{brokerAndInitialOffset.Broker, m.getFetcherId(topicAndPartition.Topic, topicAndPartition.Partition)}
			if partitionsPerFetcher[brokerAndFetcher] == nil {
				partitionsPerFetcher[brokerAndFetcher] = make(map[TopicAndPartition]*BrokerAndInitialOffset)
			}
			partitionsPerFetcher[brokerAndFetcher][topicAndPartition] = brokerAndInitialOffset

			Debugf(m, "partitionsPerFetcher: %v", partitionsPerFetcher)
			for brokerAndFetcherId, partitionOffsets := range partitionsPerFetcher {
				if m.fetcherRoutineMap[brokerAndFetcherId] == nil {
					Debugf(m, "Starting new fetcher")
					fetcherRoutine := newConsumerFetcher(m,
						fmt.Sprintf("ConsumerFetcherRoutine-%s-%d-%d", m.config.ConsumerId, brokerAndFetcherId.FetcherId, brokerAndFetcherId.Broker.Id),
						brokerAndFetcherId.Broker,
						m.partitionMap)
					m.fetcherRoutineMap[brokerAndFetcherId] = fetcherRoutine
					go fetcherRoutine.Start()
				}

				partitionToOffsetMap := make(map[TopicAndPartition]int64)
				for tp, b := range partitionOffsets {
					partitionToOffsetMap[tp] = b.InitOffset
				}
				m.fetcherRoutineMap[brokerAndFetcherId].AddPartitions(partitionToOffsetMap)
			}
		}
	})
}

func (m *consumerFetcherManager) addPartitionsWithError(partitions []TopicAndPartition) {
	Info(m.config.ConsumerId, "Adding partitions with error")
	InLock(&m.partitionMapLock, func() {
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
	return int(math.Abs(float64(31 * Hash(topic) + partitionId))) % int(m.numStreams)
}

func (m *consumerFetcherManager) ShutdownIdleFetchers() {
	Debug(m, "Shutting down idle fetchers")
	InLock(&m.fetcherRoutineMapLock, func() {
		for key, fetcher := range m.fetcherRoutineMap {
			if fetcher.PartitionCount() <= 0 {
				<-fetcher.Close()
				delete(m.fetcherRoutineMap, key)
			}
		}
	})
	Debug(m, "Closed idle fetchers")
}

func (m *consumerFetcherManager) CloseAllFetchers() {
	Info(m, "Closing fetchers")
	m.isReady = false
	//	InLock(&m.mapLock, func() {
	Debugf(m, "Trying to close %d fetchers", len(m.fetcherRoutineMap))
	for _, fetcher := range m.fetcherRoutineMap {
		Debugf(m, "Closing %s", fetcher)
		<-fetcher.Close()
		Debugf(m, "Closed %s", fetcher)
	}

	for key := range m.fetcherRoutineMap {
		delete(m.fetcherRoutineMap, key)
	}
	//		})
}

func (m *consumerFetcherManager) SwitchTopic(newTopic string) {
	Infof(m.config.ConsumerId, "Stop all current fetchers and switch topic to %s\n", newTopic)
}

func (m *consumerFetcherManager) Close() <-chan bool {
	Info(m, "Closing manager")
	go func() {
		InLock(&m.stopLock, func() {
			Info(m, "Stopping find leader")
			m.shuttingDown = true
			m.stopWaitingNextRequests <- true
			m.leaderCond.Broadcast()
			m.CloseAllFetchers()
			m.partitionMap = nil
			m.noLeaderPartitions = nil
			m.closeFinished <- true
		})
	}()

	return m.closeFinished
}

type consumerFetcherRoutine struct {
	manager *consumerFetcherManager
	name             string
	broker *BrokerInfo
	brokerAddr       string //just not to calculate each time
	allPartitionMap map[TopicAndPartition]*PartitionTopicInfo
	partitionMap map[TopicAndPartition]int64
	partitionMapLock sync.Mutex
	partitionCount   int
	id               string
	topic            string
	closeFinished    chan bool
	fetchRequestBlockMap map[TopicAndPartition]*PartitionFetchInfo
	fetchStopper     chan bool
	askNext          chan TopicAndPartition
}

func (f *consumerFetcherRoutine) String() string {
	return f.name
}

func newConsumerFetcher(m *consumerFetcherManager, name string, broker *BrokerInfo, allPartitionMap map[TopicAndPartition]*PartitionTopicInfo) *consumerFetcherRoutine {
	return &consumerFetcherRoutine{
		manager : m,
		name : name,
		broker : broker,
		brokerAddr : fmt.Sprintf("%s:%d", broker.Host, broker.Port),
		allPartitionMap : allPartitionMap,
		partitionMap : make(map[TopicAndPartition]int64),
		closeFinished : make(chan bool),
		fetchRequestBlockMap : make(map[TopicAndPartition]*PartitionFetchInfo),
		fetchStopper : make(chan bool),
		askNext : make(chan TopicAndPartition),
	}
}

func (f *consumerFetcherRoutine) Start() {
	Info(f, "Fetcher started")
	for !f.manager.shuttingDown && f.manager.isReady {
		nextTopicPartition := <-f.askNext
		InLock(&f.manager.stopLock, func() {
			if f.manager.shuttingDown { return }
			Debug(f, "Next asked")
			InLock(&f.partitionMapLock, func() {
				Debugf(f, "Partition map: %v", f.partitionMap)
				offset := f.partitionMap[nextTopicPartition]
				f.fetchRequestBlockMap[nextTopicPartition] = &PartitionFetchInfo{offset, f.manager.config.FetchMessageMaxBytes}
			})

			config := f.manager.config
			fetchRequest := new(sarama.FetchRequest)
			fetchRequest.MinBytes = config.FetchMinBytes
			fetchRequest.MaxWaitTime = config.FetchWaitMaxMs
			partitionFetchInfo := f.fetchRequestBlockMap[nextTopicPartition]
			Infof(f, "Adding block: topic=%s, partition=%d, offset=%d, fetchsize=%d", nextTopicPartition.Topic, int32(nextTopicPartition.Partition), partitionFetchInfo.Offset, partitionFetchInfo.FetchSize)
			fetchRequest.AddBlock(nextTopicPartition.Topic, int32(nextTopicPartition.Partition), partitionFetchInfo.Offset, partitionFetchInfo.FetchSize)

			Debugf(f, "Request Block Map length: %d", len(f.fetchRequestBlockMap))
			if len(f.fetchRequestBlockMap) > 0 {
				f.processFetchRequest(fetchRequest)
			}
		})
	}
	f.fetchStopper <- true
	Debug(f, "Stopped fetcher")
}

func (f *consumerFetcherRoutine) AddPartitions(partitionAndOffsets map[TopicAndPartition]int64) {
	Infof(f, "Adding partitions: %v", partitionAndOffsets)
	newPartitions := make(map[TopicAndPartition]chan TopicAndPartition)
	InLock(&f.partitionMapLock, func() {
		for topicAndPartition, offset := range partitionAndOffsets {
			if _, contains := f.partitionMap[topicAndPartition]; !contains {
				validOffset := offset
				if IsOffsetInvalid(validOffset) {
					validOffset = f.handleOffsetOutOfRange(&topicAndPartition)
				}
				f.partitionMap[topicAndPartition] = validOffset
				f.manager.askNextFetchers[topicAndPartition] = f.askNext
				newPartitions[topicAndPartition] = f.askNext
			}
		}
	})
	for topicAndPartition, askNext := range newPartitions {
		Debugf(f, "Sending ask next to %s", f)
		askNext <- topicAndPartition
	}
}

func (f *consumerFetcherRoutine) PartitionCount() int {
	count := 0
	InLock(&f.manager.fetcherRoutineMapLock, func() {
		count = len(f.partitionMap)
	})
	return count
}

func (f *consumerFetcherRoutine) processFetchRequest(request *sarama.FetchRequest) {
	Info(f, "Started processing fetch request")
	partitionsWithError := make(map[TopicAndPartition]bool)

	saramaBroker := sarama.NewBroker(f.brokerAddr)
	err := saramaBroker.Open(nil)
	if err != nil {
		f.handleFetchError(request, err, partitionsWithError)
	}

	response, err := saramaBroker.Fetch(f.manager.config.ClientId, request)
	if err != nil {
		f.handleFetchError(request, err, partitionsWithError)
	}
	defer saramaBroker.Close()

	if response != nil {
		Debug(f, "Processing fetch request")
		InLock(&f.partitionMapLock, func() {
			for topic, partitionAndData := range response.Blocks {
				for partition, data := range partitionAndData {
					topicAndPartition := TopicAndPartition{topic, partition}
					currentOffset, exists := f.partitionMap[topicAndPartition]
					if exists && f.fetchRequestBlockMap[topicAndPartition].Offset == currentOffset {
						switch data.Err {
						case sarama.NoError: {
							messages := data.MsgSet.Messages
							newOffset := currentOffset
							if len(messages) > 0 {
								newOffset = messages[len(messages)-1].Offset+1
							}
							f.partitionMap[topicAndPartition] = newOffset
							f.processPartitionData(topicAndPartition, currentOffset, data)
						}
						case sarama.OffsetOutOfRange: {
							newOffset := f.handleOffsetOutOfRange(&topicAndPartition)
							f.partitionMap[topicAndPartition] = newOffset
							Infof(f.manager.config.ConsumerId, "Current offset %d for partition %s is out of range. Reset offset to %d\n", currentOffset, topicAndPartition, newOffset)
						}
						default: {
							Infof(f.manager.config.ConsumerId, "Error for partition %s. Removing", topicAndPartition)
							partitionsWithError[topicAndPartition] = true
						}
						}
					}
				}
			}
		})
	}

	if len(partitionsWithError) > 0 {
		Info(f.manager.config.ConsumerId, "Handling partitions with error")
		partitionsWithErrorSet := make([]TopicAndPartition, 0, len(partitionsWithError))
		for k := range partitionsWithError {
			partitionsWithErrorSet = append(partitionsWithErrorSet, k)
		}
		f.handlePartitionsWithErrors(partitionsWithErrorSet)
	}
}

func (f *consumerFetcherRoutine) processPartitionData(topicAndPartition TopicAndPartition, fetchOffset int64, partitionData *sarama.FetchResponseBlock) {
	Info(f, "Processing partition data")

	partitionTopicInfo := f.allPartitionMap[topicAndPartition]
	partitionTopicInfo.Accumulator.InputChannel.chunks <- &TopicPartitionData{ topicAndPartition, partitionData }
	Info(f, "Sent partition data")
	//	partitionTopicInfo.BlockChannel.chunks <- &TopicPartitionData{ topicAndPartition, partitionData }
}

func (f *consumerFetcherRoutine) handleFetchError(request *sarama.FetchRequest, err error, partitionsWithError map[TopicAndPartition]bool) {
	Infof(f, "Error in fetch %v. Possible cause: %s\n", request, err)
	InLock(&f.partitionMapLock, func() {
		//		for k, _ := range f.partitionMap {
		//			partitionsWithError[k] = true
		//		}
		//	})
		//	Infof(f, "Error in fetch out of lock", request, err)
		for k, _ := range f.partitionMap {
			partitionsWithError[k] = true
		}
	})
}

func (f *consumerFetcherRoutine) handleOffsetOutOfRange(topicAndPartition *TopicAndPartition) int64 {
	offsetTime := sarama.LatestOffsets
	if f.manager.config.AutoOffsetReset == SmallestOffset {
		offsetTime = sarama.EarliestOffset
	}

	newOffset := f.earliestOrLatestOffset(topicAndPartition, offsetTime)
	partitionTopicInfo := f.allPartitionMap[*topicAndPartition]
	partitionTopicInfo.FetchedOffset = newOffset
	partitionTopicInfo.ConsumedOffset = newOffset

	return newOffset
}

func (f *consumerFetcherRoutine) handlePartitionsWithErrors(partitions []TopicAndPartition) {
	f.removePartitions(partitions)
	f.manager.addPartitionsWithError(partitions)
}

func (f *consumerFetcherRoutine) earliestOrLatestOffset(topicAndPartition *TopicAndPartition, offsetTime sarama.OffsetTime) int64 {
	client, err := sarama.NewClient(f.manager.config.ClientId, []string{f.brokerAddr}, nil)
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
	InLock(&f.partitionMapLock, func() {
		for _, topicAndPartition := range partitions {
			delete(f.partitionMap, topicAndPartition)
			delete(f.manager.askNextFetchers, topicAndPartition)
		}
	})
}

func (f *consumerFetcherRoutine) Close() <-chan bool {
	Info(f, "Closing fetcher")
	go func() {
		<-f.fetchStopper
		for _, pti := range f.allPartitionMap {
			Debugf(f, "Stopping %s", pti.Accumulator)
			if !pti.Accumulator.closed {
				<-pti.Accumulator.Stop()
			}
			Debugf(f, "Stopped %s", pti.Accumulator)
		}
		f.removeAllPartitions()
		Debug(f, "Sending close finished")
		f.closeFinished <- true
		Debug(f, "Sent close finished")
	}()
	return f.closeFinished
}
