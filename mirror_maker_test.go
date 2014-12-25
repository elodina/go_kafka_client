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
	"testing"
	"fmt"
	"time"
	"io/ioutil"
	"os"
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
	produce(t, messages, topic, localBroker)

	consumerConfig := testConsumerConfig()
	consumerConfig.Strategy = newCountingStrategy(t, consumeMessages, timeout, consumeStatus)
	consumer := NewConsumer(consumerConfig)
	go consumer.StartStatic(map[string]int {topic: 1})
	if actual := <-consumeStatus; actual != consumeMessages {
		t.Errorf("Failed to consume %d messages within %s. Actual messages = %d", consumeMessages, timeout, actual)
	}
	closeWithin(t, 10*time.Second, consumer)
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
