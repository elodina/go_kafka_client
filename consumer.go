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
	"fmt"
)

type Consumer struct {
	config *ConsumerConfig
	topic       string
	messages    chan *Message
	topicSwitch chan string
	close       chan bool
}

type Message struct {
	Message []byte
	Topic   string
}

func NewConsumer(topic string, config *ConsumerConfig) *Consumer {
	c := &Consumer{ config : config, topic : topic }
	c.messages = make(chan *Message)
	c.topicSwitch = make(chan string)
	c.close = make(chan bool)

	go c.fetchLoop()

	return c
}

func (c *Consumer) Messages() <-chan *Message {
	return c.messages
}

func (c *Consumer) SwitchTopic(newTopic string) {
	c.topicSwitch <- newTopic
}

func (c *Consumer) Close() {
	c.close <- true
}

func (c *Consumer) fetchLoop() {
	messageChannel := c.messageChannel()
	for {
		select {
		case message := <- messageChannel:
		c.messages <- message
		case topic := <- c.topicSwitch:
			Logger.Printf("switch topic to %s\n", topic)
			c.topic = topic
		case <-c.close:
			Logger.Println("Closing consumer")
			close(c.messages)
			close(c.topicSwitch)
			return
		}
	}
}

func (c *Consumer) messageChannel() <-chan *Message {
	messages := make(chan *Message)

	go func() {
		for i := 0; i < 5; i++ {
		time.Sleep(1 * time.Second)
		message := &Message{ Topic : c.topic, Message : []byte(fmt.Sprintf("message %d", i)) }
		messages <- message
	}
	}()

	return messages
}
