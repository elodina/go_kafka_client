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
	"io/ioutil"
	"os"
	"sync"
	"testing"
	"time"
)

func TestMirrorMakerWorks(t *testing.T) {
	topic := fmt.Sprintf("mirror-maker-works-%d", time.Now().Unix())
	prefix := "mirror_"
	destTopic := fmt.Sprintf("%s%s", prefix, topic)

	consumeMessages := 100
	timeout := 1 * time.Minute
	consumeStatus := make(chan int)

	CreateMultiplePartitionsTopic(localZk, topic, 1)
	EnsureHasLeader(localZk, topic)
	CreateMultiplePartitionsTopic(localZk, destTopic, 1)
	EnsureHasLeader(localZk, destTopic)

	config := NewMirrorMakerConfig()
	consumerConfigLocation := createConsumerConfig(t, 1)
	defer os.Remove(consumerConfigLocation)
	config.ConsumerConfigs = []string{consumerConfigLocation}
	config.NumProducers = 1
	config.NumStreams = 1
	config.PreservePartitions = false
	producerConfigLocation := createProducerConfig(t, 1)
	defer os.Remove(producerConfigLocation)
	config.ProducerConfig = producerConfigLocation
	config.TopicPrefix = "mirror_"
	config.Whitelist = fmt.Sprintf("^%s$", topic)
	mirrorMaker := NewMirrorMaker(config)
	go mirrorMaker.Start()

	messages := make([]string, 0)
	for i := 0; i < 100; i++ {
		messages = append(messages, fmt.Sprintf("mirror-%d", i))
	}
	produce(t, messages, topic, localBroker, sarama.CompressionNone)

	consumerConfig := testConsumerConfig()
	consumerConfig.Strategy = newCountingStrategy(t, consumeMessages, timeout, consumeStatus)
	consumer := NewConsumer(consumerConfig)
	go consumer.StartStatic(map[string]int{topic: 1})
	if actual := <-consumeStatus; actual != consumeMessages {
		t.Errorf("Failed to consume %d messages within %s. Actual messages = %d", consumeMessages, timeout, actual)
	}
	closeWithin(t, 10*time.Second, consumer)
	mirrorMaker.Stop()
}

func TestMirrorMakerPreservesPartitions(t *testing.T) {
	topic := fmt.Sprintf("mirror-maker-partitions-%d", time.Now().Unix())
	prefix := "mirror_"
	destTopic := fmt.Sprintf("%s%s", prefix, topic)

	consumeMessages := 100
	timeout := 1 * time.Minute
	sourceConsumeStatus := make(chan int)
	destConsumeStatus := make(chan int)
	sourceConsumedMessages := 0
	destConsumedMessages := 0

	CreateMultiplePartitionsTopic(localZk, topic, 5)
	EnsureHasLeader(localZk, topic)
	CreateMultiplePartitionsTopic(localZk, destTopic, 5)
	EnsureHasLeader(localZk, destTopic)

	config := NewMirrorMakerConfig()
	consumerConfigLocation := createConsumerConfig(t, 1)
	defer os.Remove(consumerConfigLocation)
	config.ConsumerConfigs = []string{consumerConfigLocation}
	config.NumProducers = 1
	config.NumStreams = 1
	config.PreservePartitions = true
	producerConfigLocation := createProducerConfig(t, 1)
	defer os.Remove(producerConfigLocation)
	config.ProducerConfig = producerConfigLocation
	config.TopicPrefix = prefix
	config.Whitelist = fmt.Sprintf("^%s$", topic)
	mirrorMaker := NewMirrorMaker(config)
	go mirrorMaker.Start()

	produceN(t, consumeMessages, topic, localBroker)

	//consume all messages with a regular consumer and make a map of message partitions
	messagePartitions := make(map[string]int32)
	sourceConsumerConfig := testConsumerConfig()
	sourceConsumerConfig.Strategy = func(_ *Worker, msg *Message, id TaskId) WorkerResult {
		messagePartitions[string(msg.Value)] = msg.Partition
		sourceConsumedMessages++
		if sourceConsumedMessages == consumeMessages {
			sourceConsumeStatus <- sourceConsumedMessages
		}
		return NewSuccessfulResult(id)
	}
	sourceConsumer := NewConsumer(sourceConsumerConfig)
	go sourceConsumer.StartStatic(map[string]int{topic: 1})
	select {
	case <-sourceConsumeStatus:
	case <-time.After(timeout):
		t.Errorf("Failed to consume %d messages within %s", consumeMessages, timeout)
	}
	closeWithin(t, 10*time.Second, sourceConsumer)

	//now consume the mirrored topic and make sure all messages are in correct partitions
	destConsumerConfig := testConsumerConfig()
	destConsumerConfig.Strategy = func(_ *Worker, msg *Message, id TaskId) WorkerResult {
		expectedPartition := messagePartitions[string(msg.Value)]
		assert(t, msg.Partition, expectedPartition)
		destConsumedMessages++
		if destConsumedMessages == consumeMessages {
			destConsumeStatus <- destConsumedMessages
		}
		return NewSuccessfulResult(id)
	}
	destConsumer := NewConsumer(destConsumerConfig)
	go destConsumer.StartStatic(map[string]int{destTopic: 1})
	select {
	case <-destConsumeStatus:
	case <-time.After(timeout):
		t.Errorf("Failed to consume %d messages within %s", consumeMessages, timeout)
	}
	closeWithin(t, 10*time.Second, destConsumer)
	mirrorMaker.Stop()
}

func TestMirrorMakerPreservesOrder(t *testing.T) {
	topic := fmt.Sprintf("mirror-maker-order-%d", time.Now().Unix())
	prefix := "mirror_"
	destTopic := fmt.Sprintf("%s%s", prefix, topic)

	consumeMessages := 100
	timeout := 1 * time.Minute
	sourceConsumeStatus := make(chan int)
	destConsumeStatus := make(chan int)
	sourceConsumedMessages := 0
	destConsumedMessages := 0

	CreateMultiplePartitionsTopic(localZk, topic, 5)
	EnsureHasLeader(localZk, topic)
	CreateMultiplePartitionsTopic(localZk, destTopic, 5)
	EnsureHasLeader(localZk, destTopic)

	config := NewMirrorMakerConfig()
	consumerConfigLocation := createConsumerConfig(t, 1)
	defer os.Remove(consumerConfigLocation)
	config.ConsumerConfigs = []string{consumerConfigLocation}
	config.NumProducers = 3
	config.NumStreams = 1
	config.PreservePartitions = true
	config.PreserveOrder = true
	producerConfigLocation := createProducerConfig(t, 1)
	defer os.Remove(producerConfigLocation)
	config.ProducerConfig = producerConfigLocation
	config.TopicPrefix = prefix
	config.Whitelist = fmt.Sprintf("^%s$", topic)
	mirrorMaker := NewMirrorMaker(config)
	go mirrorMaker.Start()

	produceN(t, consumeMessages, topic, localBroker)

	//consume all messages with a regular consumer and make a map of message partitions
	messageOrder := make(map[int32][]string)
	var messageOrderLock sync.Mutex
	sourceConsumerConfig := testConsumerConfig()
	sourceConsumerConfig.Strategy = func(_ *Worker, msg *Message, id TaskId) WorkerResult {
		messageOrderLock.Lock()
		defer messageOrderLock.Unlock()
		if _, exists := messageOrder[msg.Partition]; !exists {
			messageOrder[msg.Partition] = make([]string, 0)
		}
		messageOrder[msg.Partition] = append(messageOrder[msg.Partition], string(msg.Value))
		sourceConsumedMessages++
		if sourceConsumedMessages == consumeMessages {
			sourceConsumeStatus <- sourceConsumedMessages
		}
		return NewSuccessfulResult(id)
	}
	sourceConsumer := NewConsumer(sourceConsumerConfig)
	go sourceConsumer.StartStatic(map[string]int{topic: 1})
	select {
	case <-sourceConsumeStatus:
	case <-time.After(timeout):
		t.Errorf("Failed to consume %d messages within %s", consumeMessages, timeout)
	}
	closeWithin(t, 10*time.Second, sourceConsumer)

	//now consume the mirrored topic and make sure all messages are in correct partitions
	destConsumerConfig := testConsumerConfig()
	destConsumerConfig.Strategy = func(_ *Worker, msg *Message, id TaskId) WorkerResult {
		messageOrderLock.Lock()
		defer messageOrderLock.Unlock()
		expectedMessage := messageOrder[msg.Partition][0]
		assert(t, string(msg.Value), expectedMessage)
		messageOrder[msg.Partition] = messageOrder[msg.Partition][1:]
		destConsumedMessages++
		if destConsumedMessages == consumeMessages {
			destConsumeStatus <- destConsumedMessages
		}
		return NewSuccessfulResult(id)
	}
	destConsumer := NewConsumer(destConsumerConfig)
	go destConsumer.StartStatic(map[string]int{destTopic: 1})
	select {
	case <-destConsumeStatus:
	case <-time.After(timeout):
		t.Errorf("Failed to consume %d messages within %s", consumeMessages, timeout)
	}
	closeWithin(t, 10*time.Second, destConsumer)
	mirrorMaker.Stop()
}

func createConsumerConfig(t *testing.T, id int) string {
	tmpPath, err := ioutil.TempDir("", "go_kafka_client")
	if err != nil {
		t.Fatal(err)
	}
	os.Mkdir(tmpPath, 0700)

	fileName := fmt.Sprintf("consumer-%d.properties", id)

	contents := fmt.Sprintf(`zookeeper.connect=%s
zookeeper.connection.timeout.ms=6000
group.id=%s
`, localZk, fmt.Sprintf("test-mirrormaker-%d", time.Now().Unix()))

	configPath := fmt.Sprintf("%s/%s", tmpPath, fileName)
	err = ioutil.WriteFile(configPath, []byte(contents), 0700)
	if err != nil {
		panic(err)
	}

	return configPath
}

func createProducerConfig(t *testing.T, id int) string {
	tmpPath, err := ioutil.TempDir("", "go_kafka_client")
	if err != nil {
		t.Fatal(err)
	}
	os.Mkdir(tmpPath, 0700)

	fileName := fmt.Sprintf("producer-%d.properties", id)

	contents := fmt.Sprintf(`metadata.broker.list=%s
`, localBroker)

	configPath := fmt.Sprintf("%s/%s", tmpPath, fileName)
	err = ioutil.WriteFile(configPath, []byte(contents), 0700)
	if err != nil {
		panic(err)
	}

	return configPath
}
