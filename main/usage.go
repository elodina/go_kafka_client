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
)

type Worker struct {}

func (w *Worker) doWork(msg *go_kafka_client.Message, consumer *go_kafka_client.Consumer) {
	time.Sleep(time.Duration(rand.Intn(10)) * time.Second)
	consumer.Ack(msg.Offset, msg.Topic, msg.Partition)
}

func main() {
	config := go_kafka_client.DefaultConsumerConfig()
	config.ConsumerId = fmt.Sprintf("consumer-%d", time.Now().Unix())
	consumer := go_kafka_client.NewConsumer("my_topic", "group1", []string{"192.168.86.5"}, config)

	time.Sleep(1 * time.Second)
	config2 := go_kafka_client.DefaultConsumerConfig()
	config2.ConsumerId = fmt.Sprintf("consumer-%d", time.Now().Unix())
	consumer2 := go_kafka_client.NewConsumer("my_topic", "group1", []string{"192.168.86.5"}, config2)
	fmt.Println(consumer2)

	time.Sleep(1 * time.Second)
	config3 := go_kafka_client.DefaultConsumerConfig()
	config3.ConsumerId = fmt.Sprintf("consumer-%d", time.Now().Unix())
	consumer3 := go_kafka_client.NewConsumer("my_topic", "group1", []string{"192.168.86.5"}, config3)
	fmt.Println(consumer3)

	go func() {
		for message := range consumer.Messages() {
			go_kafka_client.Logger.Printf("Consumed message '%v' from topic %s\n", string(message.Value), message.Topic)
			worker := &Worker{}
			go worker.doWork(message, consumer)
		}
	}()

	time.Sleep(10 * time.Second)
	go func() {
		consumer.SwitchTopic("another_topic")
	}()

	time.Sleep(10 * time.Second)
	futureClose := consumer.Close()
	<-futureClose
	go_kafka_client.Logger.Println("Gracefully shutdown")
}
