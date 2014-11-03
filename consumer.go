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
)

type Consumer struct {
	config        *ConsumerConfig
	topic         string
	group         string
	zookeeper     []string
	fetcher         *consumerFetcherManager
	messages      chan *Message
	unsubscribe   chan bool
	closeFinished chan bool
	zkConn          *zk.Conn
	rebalanceLock sync.Mutex
	isSuttingdown bool
}

type Message struct {
	Key       []byte
	Value     []byte
	Topic     string
	Partition int32
	Offset    int64
}

func NewConsumer(config *ConsumerConfig) *Consumer {
	c := &Consumer{
		config : config,
		//		topic : topic,
		//		group : group,
		//		zookeeper : zookeeper,
		messages : make(chan *Message),
		unsubscribe : make(chan bool),
		closeFinished : make(chan bool),
	}

	c.addShutdownHook()

	c.connectToZookeeper()
	//	c.registerInZookeeper()
	c.fetcher = newConsumerFetcherManager(config, c.zkConn, c.messages)

	return c
}

func (c *Consumer) Messages() <-chan *Message {
	return c.messages
}

func (c *Consumer) SwitchTopic(newTopic string) {
	c.fetcher.SwitchTopic(newTopic)
}

func (c *Consumer) Close() <-chan bool {
	Logger.Println("Closing consumer")
	go func() {
		<-c.fetcher.Close()
		c.unsubscribe <- true
		err := DeregisterConsumer(c.zkConn, c.group, c.config.ConsumerId)
		if err != nil {
			panic(err)
		}
		c.closeFinished <- true
	}()
	return c.closeFinished
}

func (c *Consumer) Ack(offset int64, topic string, partition int32) error {
	Logger.Printf("Acking offset %d for topic %s and partition %d", offset, topic, partition)
	return nil
}

func (c *Consumer) addShutdownHook() {
	s := make(chan os.Signal, 1)
	signal.Notify(s, os.Interrupt)
	go func() {
		<-s
		c.Close()
	}()
}

func (c *Consumer) connectToZookeeper() {
	Logger.Println("Connecting to zk")
	if conn, _, err := zk.Connect(c.config.ZookeeperConnect, c.config.ZookeeperTimeout); err != nil {
		panic(err)
	} else {
		c.zkConn = conn
	}
}

func (c *Consumer) registerInZookeeper() {
	Logger.Println("Registering in zk")
	subscription := make(map[string]int)
	subscription[c.topic] = 1

	consumerInfo := &ConsumerInfo{
		Version : int16(1),
		Subscription : subscription,
		Pattern : WhiteListPattern,
		Timestamp : time.Now().Unix(),
	}

	if err := RegisterConsumer(c.zkConn, c.group, c.config.ConsumerId, consumerInfo); err != nil {
		panic(err)
	}

	c.subscribeForChanges(c.group)
}

func (c *Consumer) subscribeForChanges(group string) {
	Logger.Println("Subscribing for changes for", NewZKGroupDirs(group).ConsumerRegistryDir)

	consumersWatcher, err := GetConsumersInGroupWatcher(c.zkConn, group)
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
				triggerRebalanceIfNeeded(e, c)
			}
			case e := <-consumersWatcher: {
				triggerRebalanceIfNeeded(e, c)
			}
			case e := <-brokersWatcher: {
				triggerRebalanceIfNeeded(e, c)
			}
			case <-c.unsubscribe: {
				Logger.Println("Unsubscribing from changes")
				break
			}
			}
		}
	}()
}

func triggerRebalanceIfNeeded(e zk.Event, c *Consumer) {
	emptyEvent := zk.Event{}
	if e != emptyEvent {
		c.rebalance(e)
	} else {
		time.Sleep(2 * time.Second)
	}
}

func (c *Consumer) rebalance(_ zk.Event) {
	Logger.Printf("rebalance triggered for %s\n", c.config.ConsumerId)
	c.rebalanceLock.Lock()
	defer c.rebalanceLock.Unlock()
	if (c.isSuttingdown) {
		var success = false
		var err error
		for i := 0; i < int(c.config.RebalanceMaxRetries); i++ {
			topicPerThreadIdsMap, err := NewTopicsToNumStreams(c.group, c.config.ConsumerId, c.zkConn, c.config.ExcludeInternalTopics)
			if (err != nil) {
				Logger.Println(err)
				continue
			}
			brokers, err := GetAllBrokersInCluster(c.zkConn)
			if (err != nil) {
				Logger.Println(err)
				continue
			}

			//TODO: close fetchers
			//TODO: release parition ownership

			Logger.Println(topicPerThreadIdsMap)
			Logger.Println(brokers)

			success = true
		}

		if (!success) {
			panic(err)
		}
	}
}
