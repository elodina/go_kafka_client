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
	"fmt"
	"sync"
	"testing"
	"time"
)

var numMessages = 1000
var consumeTimeout = 1 * time.Minute
var localZk = "localhost:2181"
var localBroker = "localhost:9092"

func TestConsumerWithInconsistentProducing(t *testing.T) {
	consumeStatus := make(chan int)
	produceMessages := 1
	consumeMessages := 2
	sleepTime := 10 * time.Second
	timeout := 30 * time.Second
	topic := fmt.Sprintf("inconsistent-producing-%d", time.Now().Unix())

	//create topic
	CreateMultiplePartitionsTopic(localZk, topic, 1)
	EnsureHasLeader(localZk, topic)

	Infof("test", "Produce %d message", produceMessages)
	go produceN(t, produceMessages, topic, localBroker)

	config := testConsumerConfig()
	config.Strategy = newCountingStrategy(t, consumeMessages, timeout, consumeStatus)
	consumer := NewConsumer(config)
	Info("test", "Starting consumer")
	go consumer.StartStatic(map[string]int{topic: 1})
	//produce one more message after 10 seconds
	Infof("test", "Waiting for %s before producing another message", sleepTime)
	time.Sleep(sleepTime)
	Infof("test", "Produce %d message", produceMessages)
	go produceN(t, produceMessages, topic, localBroker)

	//make sure we get 2 messages
	if actual := <-consumeStatus; actual != consumeMessages {
		t.Errorf("Failed to consume %d messages within %s. Actual messages = %d", consumeMessages, timeout, actual)
	}

	closeWithin(t, 10*time.Second, consumer)
}

func TestStaticConsumingSinglePartition(t *testing.T) {
	consumeStatus := make(chan int)
	topic := fmt.Sprintf("test-static-%d", time.Now().Unix())

	CreateMultiplePartitionsTopic(localZk, topic, 1)
	EnsureHasLeader(localZk, topic)
	go produceN(t, numMessages, topic, localBroker)

	config := testConsumerConfig()
	config.Strategy = newCountingStrategy(t, numMessages, consumeTimeout, consumeStatus)
	consumer := NewConsumer(config)
	go consumer.StartStatic(map[string]int{topic: 1})
	if actual := <-consumeStatus; actual != numMessages {
		t.Errorf("Failed to consume %d messages within %s. Actual messages = %d", numMessages, consumeTimeout, actual)
	}
	closeWithin(t, 10*time.Second, consumer)
}

func TestStaticConsumingMultiplePartitions(t *testing.T) {
	consumeStatus := make(chan int)
	topic := fmt.Sprintf("test-static-%d", time.Now().Unix())

	CreateMultiplePartitionsTopic(localZk, topic, 5)
	EnsureHasLeader(localZk, topic)
	go produceN(t, numMessages, topic, localBroker)

	config := testConsumerConfig()
	config.Strategy = newCountingStrategy(t, numMessages, consumeTimeout, consumeStatus)
	consumer := NewConsumer(config)
	go consumer.StartStatic(map[string]int{topic: 3})
	if actual := <-consumeStatus; actual != numMessages {
		t.Errorf("Failed to consume %d messages within %s. Actual messages = %d", numMessages, consumeTimeout, actual)
	}
	closeWithin(t, 10*time.Second, consumer)
}

func TestWhitelistConsumingSinglePartition(t *testing.T) {
	consumeStatus := make(chan int)
	topic1 := fmt.Sprintf("test-whitelist-%d", time.Now().Unix())
	topic2 := fmt.Sprintf("test-whitelist-%d", time.Now().Unix()+1)

	CreateMultiplePartitionsTopic(localZk, topic1, 1)
	EnsureHasLeader(localZk, topic1)
	CreateMultiplePartitionsTopic(localZk, topic2, 1)
	EnsureHasLeader(localZk, topic2)
	go produceN(t, numMessages, topic1, localBroker)
	go produceN(t, numMessages, topic2, localBroker)

	expectedMessages := numMessages * 2

	config := testConsumerConfig()
	config.Strategy = newCountingStrategy(t, expectedMessages, consumeTimeout, consumeStatus)
	consumer := NewConsumer(config)
	go consumer.StartWildcard(NewWhiteList("test-whitelist-.+"), 1)
	if actual := <-consumeStatus; actual != expectedMessages {
		t.Errorf("Failed to consume %d messages within %s. Actual messages = %d", expectedMessages, consumeTimeout, actual)
	}
	closeWithin(t, 10*time.Second, consumer)
}

func TestMessagesProcessedOnce(t *testing.T) {
	closeTimeout := 15 * time.Second
	consumeFinished := make(chan bool)
	messages := 100
	topic := fmt.Sprintf("test-processing-%d", time.Now().Unix())
	CreateMultiplePartitionsTopic(localZk, topic, 1)
	EnsureHasLeader(localZk, topic)
	go produceN(t, messages, topic, localBroker)

	config := testConsumerConfig()
	messagesMap := make(map[string]bool)
	var messagesMapLock sync.Mutex
	config.Strategy = func(_ *Worker, msg *Message, id TaskId) WorkerResult {
		value := string(msg.Value)
		inLock(&messagesMapLock, func() {
				if _, exists := messagesMap[value]; exists {
					t.Errorf("Duplicate message: %s", value)
				}
				messagesMap[value] = true
				if len(messagesMap) == messages {
					consumeFinished <- true
				}
			})
		return NewSuccessfulResult(id)
	}
	consumer := NewConsumer(config)

	go consumer.StartStatic(map[string]int{topic:1})

	select {
	case <-consumeFinished:
	case <-time.After(consumeTimeout): t.Errorf("Failed to consume %d messages within %s. Actual messages = %d", messages, consumeTimeout, len(messagesMap))
	}
	closeWithin(t, closeTimeout, consumer)

	//restart consumer
	zkConfig := NewZookeeperConfig()
	zkConfig.ZookeeperConnect = []string{localZk}
	config.Coordinator = NewZookeeperCoordinator(zkConfig)
	consumer = NewConsumer(config)
	go consumer.StartStatic(map[string]int{topic:1})

	select {
	//this happens if we get a duplicate
	case <-consumeFinished:
	//and this happens normally
	case <-time.After(closeTimeout):
	}
	closeWithin(t, closeTimeout, consumer)
}

func testConsumerConfig() *ConsumerConfig {
	config := DefaultConsumerConfig()
	config.AutoOffsetReset = SmallestOffset
	config.WorkerFailureCallback = func(_ *WorkerManager) FailedDecision {
		return CommitOffsetAndContinue
	}
	config.WorkerFailedAttemptCallback = func(_ *Task, _ WorkerResult) FailedDecision {
		return CommitOffsetAndContinue
	}
	config.Strategy = goodStrategy

	zkConfig := NewZookeeperConfig()
	zkConfig.ZookeeperConnect = []string{localZk}
	config.Coordinator = NewZookeeperCoordinator(zkConfig)

	return config
}

func newCountingStrategy(t *testing.T, expectedMessages int, timeout time.Duration, notify chan int) WorkerStrategy {
	consumedMessages := 0
	var consumedMessagesLock sync.Mutex
	consumeFinished := make(chan bool)
	go func() {
		select {
		case <-consumeFinished:
		case <-time.After(timeout):
		}
		inLock(&consumedMessagesLock, func() {
			notify <- consumedMessages
		})
	}()
	return func(_ *Worker, _ *Message, id TaskId) WorkerResult {
		inLock(&consumedMessagesLock, func() {
			consumedMessages++
			if consumedMessages == expectedMessages {
				consumeFinished <- true
			}
		})
		return NewSuccessfulResult(id)
	}
}
