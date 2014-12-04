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
	"github.com/samuel/go-zookeeper/zk"
	"sync"
	"testing"
	"time"
)

var numMessages = 1000
var consumeTimeout = 20 * time.Second


func TestStaticConsumingSinglePartition(t *testing.T) {
	withKafka(t, func(zkServer *zk.TestServer, kafkaServer *TestKafkaServer) {
		consumeStatus := make(chan int)
		topic := fmt.Sprintf("test-static-%d", time.Now().Unix())

		CreateMultiplePartitionsTopic(fmt.Sprintf("localhost:%d", zkServer.Port), topic, 1)
		go produceN(t, numMessages, topic, kafkaServer.Addr())

		config := testConsumerConfig(zkServer)
		config.Strategy = newCountingStrategy(t, numMessages, consumeTimeout, consumeStatus)
		consumer := NewConsumer(config)
		go consumer.StartStatic(map[string]int{topic: 1})
		if actual := <-consumeStatus; actual != numMessages {
			t.Errorf("Failed to consume %d messages within %s. Actual messages = %d", numMessages, consumeTimeout, actual)
		}
		closeWithin(t, 10 * time.Second, consumer)
	})
}

//func TestBlueGreenDeployment(t *testing.T) {
//	config := DefaultConsumerConfig()
//	config.BlueGreenDeploymentEnabled = true
//	config.PartitionAssignmentStrategy = RoundRobinStrategy
//	assertNot(t, config.Validate(), nil)
//
//	withKafka(t, func(zkServer *zk.TestServer, kafkaServer *TestKafkaServer) {
//		topic := fmt.Sprintf("test-%d", time.Now().Unix())
//		CreateMultiplePartitionsTopic(fmt.Sprintf("localhost:%d", zkServer.Port), topic, 3)
//
//		config = testConsumerConfig(zkServer)
//		consumer := NewConsumer(config)
//		go consumer.StartStatic(map[string]int{topic: 3})
//		time.Sleep(5 * time.Second)
//		//			consumer.S
//		//			config.Coordinator.(*ZookeeperCoordinator).
//	})
//}

func testConsumerConfig(zkServer *zk.TestServer) *ConsumerConfig {
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
	zkConfig.ZookeeperConnect = []string{fmt.Sprintf("localhost:%d", zkServer.Port)}
	config.Coordinator = NewZookeeperCoordinator(zkConfig)

	return config
}

func newCountingStrategy(t *testing.T, expectedMessages int, consumeTimeout time.Duration, notify chan int) WorkerStrategy {
	consumedMessages := 0
	var consumedMessagesLock sync.Mutex
	consumeFinished := make(chan bool)
	go func() {
		select {
		case <-consumeFinished:
		case <-time.After(consumeTimeout):
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
