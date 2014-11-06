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

package main

import (
	"github.com/stealthly/go_kafka_client"
	"time"
	"math/rand"
	"fmt"
	"github.com/stealthly/go-kafka/producer"
)

type Worker struct {}

func (w *Worker) doWork(msg *go_kafka_client.Message, consumer *go_kafka_client.Consumer) {
	time.Sleep(time.Duration(rand.Intn(10)) * time.Second)
	consumer.Ack(msg.Offset, msg.Topic, msg.Partition)
}

func main() {
	topic := fmt.Sprintf("test_topic-%d", time.Now().Unix())
	testMessage := fmt.Sprintf("test-message-%d", time.Now().Unix())

	kafkaProducer := producer.NewKafkaProducer(topic, []string{"192.168.86.10:9092"}, nil)
	err := kafkaProducer.Send(testMessage)
	if err != nil {
		panic(err)
	}

	config := go_kafka_client.DefaultConsumerConfig()
	config.ZookeeperConnect = []string{"192.168.86.5:2181"}
	config.AutoOffsetReset = go_kafka_client.SmallestOffset
	consumer := go_kafka_client.NewConsumer(config)

	topics := map[string]int {topic: 1}
	streams := consumer.CreateMessageStreams(topics)
	select {
	case event := <-streams[topic][0]: {
		for _, message := range event {
			fmt.Println(string(message.Value))
		}
	}
	case <-time.After(5 * time.Second): {
		panic("Failed to receive a message within 5 seconds")
	}
	}

	select {
	case <-consumer.Close(): {
		fmt.Println("Successfully closed consumer")
	}
	case <-time.After(5 * time.Second): {
		panic("Failed to close a consumer within 5 seconds")
	}
	}
}
