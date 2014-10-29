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
	"github.com/littleinc/go-zookeeper"
	"fmt"
	"math/rand"
	"time"
)

type consumerFetcherManager struct {
	topic         string
	group         string
	config        *ConsumerConfig
	zkConn        *zk.Conn
	fetchers      map[string]*consumerFetcherRoutine
	messages      chan *Message
	closeFinished chan bool
}

func newConsumerFetcherManager(topic, group string, config *ConsumerConfig, zkConn *zk.Conn, fetchInto chan *Message) *consumerFetcherManager {
	manager := &consumerFetcherManager{
		topic : topic,
		group : group,
		config : config,
		zkConn : zkConn,
		fetchers : make(map[string]*consumerFetcherRoutine),
		messages : fetchInto,
		closeFinished : make(chan bool),
	}

	go manager.startFetchers()

	return manager
}

func (m *consumerFetcherManager) startFetchers() {
	Logger.Println("starting fetchers")
	numPartitions := 1
	for i := 0; i < numPartitions; i++ {
		id := fmt.Sprintf("fetcher-%s-%d", m.topic, i)
		fetcher := newConsumerFetcher(m, id, m.topic, newConsumerFetcherRoutineConfig(1, "192.168.86.10", 9092))
		m.fetchers[fmt.Sprintf("%d", i)] = fetcher
		fetcher.fetchLoop()
	}
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
	id            string
	topic         string
	config *fetcherRoutineConfig
	close         chan bool
	closeFinished chan bool
}

type fetcherRoutineConfig struct {
	name             string
	clientId         string
	sourceBroker *BrokerInfo
	socketTimeout    int
	socketBufferSize int
	fetchSize        int
	fetcherBrokerId  int
	maxWait          int
	minBytes         int
	isinterruptible  bool
}

func newConsumerFetcher(m *consumerFetcherManager, id string, topic string, config *fetcherRoutineConfig) *consumerFetcherRoutine {
	return &consumerFetcherRoutine{
		manager : m,
		id : id,
		topic : topic,
		config : config,
		close : make(chan bool),
		closeFinished : make(chan bool),
	}
}

func newConsumerFetcherRoutineConfig(brokerId int32, brokerHost string, brokerPort uint32) *fetcherRoutineConfig {
	broker := &BrokerInfo{
		Version : int16(1),
		Id : brokerId,
		Host : brokerHost,
		Port : brokerPort,
	}

	return &fetcherRoutineConfig {
		sourceBroker : broker,
	}
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
