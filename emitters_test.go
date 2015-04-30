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
	"testing"
	"time"
)

func TestLogEmitter(t *testing.T) {
	partitions := 1
	topic := fmt.Sprintf("testLogEmitter-%d", time.Now().Unix())

	CreateMultiplePartitionsTopic(localZk, topic, partitions)
	EnsureHasLeader(localZk, topic)

	loggerConfig := NewKafkaLogEmitterConfig()
	loggerConfig.SchemaRegistryUrl = schemaRepositoryUrl
	loggerConfig.Topic = topic
	loggerConfig.Source = "go_kafka_client.log.emitter"
	loggerConfig.Tags = map[string]string{"origin": topic}
	loggerConfig.ProducerConfig = DefaultProducerConfig()
	loggerConfig.ProducerConfig.BrokerList = []string{localBroker}

	logger := NewKafkaLogEmitter(loggerConfig)
	logger.Info("Message sent at %d", time.Now().Unix())

	consumeMessages := 1
	consumeStatus := make(chan int)
	delayTimeout := 10 * time.Second

	config := testConsumerConfig()
	config.Strategy = newCountingStrategy(t, consumeMessages, consumeTimeout, consumeStatus)
	consumer := NewConsumer(config)
	go consumer.StartStatic(map[string]int{topic: 1})

	if actual := <-consumeStatus; actual != consumeMessages {
		t.Errorf("Failed to consume %d messages within %s. Actual messages = %d", consumeMessages, consumeTimeout, actual)
	}

	closeWithin(t, delayTimeout, consumer)
}

func TestMetricsEmitter(t *testing.T) {
	partitions := 1
	topic := fmt.Sprintf("testMetricsEmitter-%d", time.Now().Unix())

	CreateMultiplePartitionsTopic(localZk, topic, partitions)
	EnsureHasLeader(localZk, topic)

	consumeMessages := 1
	consumeStatus := make(chan int)
	delayTimeout := 10 * time.Second

	metricsProducerConfig := DefaultProducerConfig()
	metricsProducerConfig.BrokerList = []string{localBroker}
	reporter := NewCodahaleKafkaReporter(topic, schemaRepositoryUrl, metricsProducerConfig)

	config := testConsumerConfig()
	config.Strategy = newCountingStrategy(t, consumeMessages, consumeTimeout, consumeStatus)
	consumer := NewConsumer(config)
	go consumer.Metrics().WriteJSON(10*time.Second, reporter)
	go consumer.StartStatic(map[string]int{topic: 1})

	if actual := <-consumeStatus; actual != consumeMessages {
		t.Errorf("Failed to consume %d messages within %s. Actual messages = %d", consumeMessages, consumeTimeout, actual)
	}

	closeWithin(t, delayTimeout, consumer)
}
