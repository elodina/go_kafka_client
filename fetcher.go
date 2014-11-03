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
	"math/rand"
	"time"
	"sync"
	"github.com/Shopify/sarama"
	"math"
)

type consumerFetcherManager struct {
	config        *ConsumerConfig
	zkConn        *zk.Conn
	fetchers      map[string]*consumerFetcherRoutine
	messages              chan *Message
	closeFinished         chan bool
	lock                  sync.Mutex
	mapLock               sync.Mutex
	partitionMap map[*TopicAndPartition]*PartitionTopicInfo
	fetcherRoutineMap map[*BrokerAndFetcherId]*consumerFetcherRoutine
	noLeaderPartitions    []*TopicAndPartition
	leaderCond            *sync.Cond
}

func newConsumerFetcherManager(config *ConsumerConfig, zkConn *zk.Conn, fetchInto chan *Message) *consumerFetcherManager {
	manager := &consumerFetcherManager{
		config : config,
		zkConn : zkConn,
		fetchers : make(map[string]*consumerFetcherRoutine),
		messages : fetchInto,
		closeFinished : make(chan bool),
		partitionMap : make(map[*TopicAndPartition]*PartitionTopicInfo),
		fetcherRoutineMap : make(map[*BrokerAndFetcherId]*consumerFetcherRoutine),
	}
	manager.leaderCond = sync.NewCond(&manager.lock)

	return manager
}

func (m *consumerFetcherManager) startConnections(topicInfos []*PartitionTopicInfo) {
	go m.FindLeaders()

	InLock(&m.lock, func() {
		newPartitionMap := make(map[*TopicAndPartition]*PartitionTopicInfo)
		noLeaderPartitions := make([]*TopicAndPartition, len(topicInfos))
		index := 0
		for _, info := range topicInfos {
			topicAndPartition := &TopicAndPartition{info.Topic, info.Partition}
			newPartitionMap[topicAndPartition] = info

			exists := false
			for _, noLeader := range m.noLeaderPartitions {
				if *topicAndPartition == *noLeader {
					exists = true
					break
				}
			}
			if !exists {
				noLeaderPartitions[index] = topicAndPartition
				index++
			}
		}
		m.partitionMap = newPartitionMap
		m.noLeaderPartitions = append(m.noLeaderPartitions, noLeaderPartitions[:index]...)
		m.leaderCond.Broadcast()
	})
}

func (m *consumerFetcherManager) FindLeaders() {
	for {
		leaderForPartitions := make(map[*TopicAndPartition]*BrokerInfo)
		_ = leaderForPartitions
		InLock(&m.lock, func() {
			for len(m.noLeaderPartitions) == 0 {
				Logger.Println("No partition for leader election")
				m.leaderCond.Wait()
			}

			Logger.Printf("Partitions without leader %v\n", m.noLeaderPartitions)
			brokers, err := GetAllBrokersInCluster(m.zkConn)
			if err != nil {
				panic(err)
			}
			topicsMetadata := fetchTopicMetadata(m.distinctTopics(), brokers, m.config.ClientId).Topics
			for _, meta := range topicsMetadata {
				topic := meta.Name
				for _, partition := range meta.Partitions {
					topicAndPartition := &TopicAndPartition{topic, int(partition.ID) }

					var leaderBroker *BrokerInfo = nil
					for _, broker := range brokers {
						if broker.Id == partition.Leader {
							leaderBroker = broker
							break
						}
					}

					for i, tp := range m.noLeaderPartitions {
						if *tp == *topicAndPartition && leaderBroker != nil {
							leaderForPartitions[topicAndPartition] = leaderBroker
							m.noLeaderPartitions[i] = nil
							break
						}
					}
				}
			}
		})

		partitionAndOffsets := make(map[*TopicAndPartition]*BrokerAndInitialOffset)
		for topicAndPartition, broker := range leaderForPartitions {
			partitionAndOffsets[topicAndPartition] = &BrokerAndInitialOffset{broker, m.partitionMap[topicAndPartition].FetchedOffset}
		}
		m.addFetcherForPartitions(partitionAndOffsets)

		m.ShutdownIdleFetchers()
		time.Sleep(m.config.RefreshLeaderBackoff)
	}
}

func fetchTopicMetadata(topics []string, brokers []*BrokerInfo, clientId string) *sarama.MetadataResponse {
	shuffledBrokers := make([]*BrokerInfo, len(brokers))
	ShuffleArray(brokers, shuffledBrokers)
	for i := 0; i < len(shuffledBrokers); i++ {
		brokerAddr := fmt.Sprintf("%s:%d", shuffledBrokers[i].Host, shuffledBrokers[i].Port)
		broker := sarama.NewBroker(brokerAddr)
		err := broker.Open(nil)
		if err != nil {
			Logger.Printf("Could not fetch topic metadata from broker %s\n", brokerAddr)
			continue
		}
		defer broker.Close()

		request := sarama.MetadataRequest{Topics: topics}
		response, err := broker.GetMetadata(clientId, &request)
		if err != nil {
			Logger.Printf("Could not fetch topic metadata from broker %s\n", brokerAddr)
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

func (m *consumerFetcherManager) addFetcherForPartitions(partitionAndOffsets map[*TopicAndPartition]*BrokerAndInitialOffset) {
	InLock(&m.mapLock, func() {
		partitionsPerFetcher := make(map[*BrokerAndFetcherId]map[*TopicAndPartition]*BrokerAndInitialOffset)
		for topicAndPartition, brokerAndInitialOffset := range partitionAndOffsets {
			brokerAndFetcher := &BrokerAndFetcherId{brokerAndInitialOffset.Broker, m.getFetcherId(topicAndPartition.Topic, topicAndPartition.Partition)}
			if partitionsPerFetcher[brokerAndFetcher] == nil {
				partitionsPerFetcher[brokerAndFetcher] = make(map[*TopicAndPartition]*BrokerAndInitialOffset)
			}
			partitionsPerFetcher[brokerAndFetcher][topicAndPartition] = brokerAndInitialOffset

			for brokerAndFetcherId, partitionOffsets := range partitionsPerFetcher {
				if m.fetcherRoutineMap[brokerAndFetcherId] == nil {
					fetcherRoutine := newConsumerFetcher(m,
						fmt.Sprintf("ConsumerFetcherRoutine-%s-%d-%d", m.config.ConsumerId, brokerAndFetcherId.FetcherId, brokerAndFetcherId.Broker.Id),
						brokerAndFetcherId.Broker,
						m.partitionMap)
					m.fetcherRoutineMap[brokerAndFetcherId] = fetcherRoutine
					fetcherRoutine.Start()
				}

				partitionToOffsetMap := make(map[*TopicAndPartition]int64)
				for tp, b := range partitionOffsets {
					partitionToOffsetMap[tp] = b.InitOffset
				}
				m.fetcherRoutineMap[brokerAndFetcherId].AddPartitions(partitionToOffsetMap)
			}
		}
	})
}

func (m *consumerFetcherManager) getFetcherId(topic string, partitionId int) int {
	return int(math.Abs(float64(31 * Hash(topic) + partitionId))) % int(m.config.NumConsumerFetchers)
}

func (m *consumerFetcherManager) ShutdownIdleFetchers() {
	InLock(&m.mapLock, func() {
		keysToBeRemoved := make([]*BrokerAndFetcherId, len(m.fetcherRoutineMap))
		index := 0
		for key, fetcher := range m.fetcherRoutineMap {
			if fetcher.PartitionCount() <= 0 {
				fetcher.Close()
				keysToBeRemoved[index] = key
				index++
			}
		}
		for i := 0; i < index; i++ {
			delete(m.fetcherRoutineMap, keysToBeRemoved[i])
		}
	})
}

func (m *consumerFetcherManager) SwitchTopic(newTopic string) {
	Logger.Printf("Stop all current fetchers and switch topic to %s\n", newTopic)
}

func (m *consumerFetcherManager) Close() <-chan bool {
	go func() {
		for _, fetcher := range m.fetchers {
			<-fetcher.Close()
		}
		m.closeFinished <- true
	}()

	return m.closeFinished
}

type consumerFetcherRoutine struct {
	manager *consumerFetcherManager
	name           string
	broker *BrokerInfo
	partitionMap map[*TopicAndPartition]*PartitionTopicInfo
	partitionCount int
	id             string
	topic          string
	close          chan bool
	closeFinished  chan bool
}

func newConsumerFetcher(m *consumerFetcherManager, name string, broker *BrokerInfo, partitionMap map[*TopicAndPartition]*PartitionTopicInfo) *consumerFetcherRoutine {
	return &consumerFetcherRoutine{
		manager : m,
		name : name,
		broker : broker,
		partitionMap : partitionMap,
		close : make(chan bool),
		closeFinished : make(chan bool),
	}
}

func (f *consumerFetcherRoutine) Start() {
	//TODO start fetcher
}

func (f *consumerFetcherRoutine) AddPartitions(partitionAndOffsets map[*TopicAndPartition]int64) {
	//TODO implement
}

func (f *consumerFetcherRoutine) PartitionCount() int {
	count := 0
	InLock(&f.manager.mapLock, func() {
		count = len(f.partitionMap)
	})
	return count
}

func (f *consumerFetcherRoutine) fetchLoop() {
	messageChannel := f.nextBlock()
	for {
		select {
		case messages := <-messageChannel: {
			for _, message := range messages {
				f.manager.messages <- message
			}
		}
			//		case topic := <-f.topicSwitch: {
			//			Logger.Printf("switch topic to %s\n", topic)
			//			f.topic = topic
			//		}
		case <-f.close: {
			Logger.Printf("Closing fetcher thread %s", f.id)
			close(messageChannel)
			time.Sleep(3 * time.Second)
			f.closeFinished <- true
			return
		}
		}
	}
}

func (f *consumerFetcherRoutine) Close() <-chan bool {
	//TODO fix this
	f.close <- true
	return f.closeFinished
}

//simulate next batch from broker
func (f *consumerFetcherRoutine) nextBlock() chan []*Message {
	messages := make(chan []*Message)

	messageSlice := make([]*Message, 10)
	id := rand.Int()
	for i := 0; i < 10; i++ {
		message := &Message{
			Offset : int64(i),
			Topic : f.topic,
			Key : []byte(fmt.Sprintf("key-%d-%d", id, i)),
			Value : []byte(fmt.Sprintf("message-%d-%d", id, i)),
		}
		messageSlice[i] = message
	}
	go func() {
		messages <- messageSlice
	}()

	return messages
}
