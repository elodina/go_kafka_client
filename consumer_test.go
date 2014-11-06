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
	"testing"
	"github.com/stealthly/go-kafka/producer"
	"fmt"
	"time"
)

var TEST_KAFKA_HOST = "192.168.86.10:9092"
var TEST_ZOOKEEPER_HOST = "192.168.86.5:2181"

func TestConsumerSingleMessage(t *testing.T) {
	consumer := testConsumer(t)

	topic := fmt.Sprintf("test-topic-single-%d", time.Now().Unix())

	kafkaProducer := producer.NewKafkaProducer(topic, []string{TEST_KAFKA_HOST}, nil)
	ProduceN(t, 1, kafkaProducer)

	topics := map[string]int {topic: 1}
	streams := consumer.CreateMessageStreams(topics)
	ReceiveN(t, 1, 5 * time.Second, streams[topic][0])

	CloseWithin(t, 5 * time.Second, consumer)
	kafkaProducer.Close()
}

func TestConsumerMultipleMessages(t *testing.T) {
	consumer := testConsumer(t)

	numMessages := 100
	topic := fmt.Sprintf("test-topic-multiple-%d", time.Now().Unix())

	kafkaProducer := producer.NewKafkaProducer(topic, []string{TEST_KAFKA_HOST}, nil)
	ProduceN(t, numMessages, kafkaProducer)

	topics := map[string]int {topic: 1}
	streams := consumer.CreateMessageStreams(topics)
	ReceiveN(t, numMessages, 5 * time.Second, streams[topic][0])

	CloseWithin(t, 5 * time.Second, consumer)
	kafkaProducer.Close()
}

func TestProduceIntoMultipleAndConsumeFromOne(t *testing.T) {
	consumer := testConsumer(t)

	numMessages := 100
	topic1 := fmt.Sprintf("test-producemultiple-noread-%d", time.Now().Unix())
	topic2 := fmt.Sprintf("test-producemultiple-read-%d", time.Now().Unix())

	kafkaProducer1 := producer.NewKafkaProducer(topic1, []string{TEST_KAFKA_HOST}, nil)
	ProduceN(t, numMessages, kafkaProducer1)
	kafkaProducer2 := producer.NewKafkaProducer(topic2, []string{TEST_KAFKA_HOST}, nil)
	ProduceN(t, numMessages, kafkaProducer2)

	topics := map[string]int {topic2: 1}
	streams := consumer.CreateMessageStreams(topics)
	ReceiveN(t, numMessages, 5 * time.Second, streams[topic2][0])

	CloseWithin(t, 5 * time.Second, consumer)
	kafkaProducer1.Close()
	kafkaProducer2.Close()
}

func testConsumer(t *testing.T) *Consumer {
	config := DefaultConsumerConfig()
	config.ZookeeperConnect = []string{TEST_ZOOKEEPER_HOST}
	config.AutoOffsetReset = SmallestOffset
	consumer := NewConsumer(config)
	AssertNot(t, consumer.zkConn, nil)
	return consumer
}
