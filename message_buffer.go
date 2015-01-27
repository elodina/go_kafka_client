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
	"sync"
	"time"
)

type messageBuffer struct {
	OutputChannel                  chan []*Message
	Messages                       []*Message
	Config                         *ConsumerConfig
	Timer                          *time.Timer
	MessageLock                    sync.Mutex
	Close                          chan bool
	stopSending                    bool
	TopicPartition                 TopicAndPartition
	askNextBatch                   chan TopicAndPartition
	disconnectChannelsForPartition chan TopicAndPartition
}

func newMessageBuffer(topicPartition TopicAndPartition, outputChannel chan []*Message, config *ConsumerConfig, askNextBatch chan TopicAndPartition, disconnectChannelsForPartition chan TopicAndPartition) *messageBuffer {
	buffer := &messageBuffer{
		OutputChannel:                  outputChannel,
		Messages:                       make([]*Message, 0),
		Config:                         config,
		Timer:                          time.NewTimer(config.FetchBatchTimeout),
		Close:                          make(chan bool),
		TopicPartition:                 topicPartition,
		askNextBatch:                   askNextBatch,
		disconnectChannelsForPartition: disconnectChannelsForPartition,
	}

	go buffer.autoFlush()

	return buffer
}

func (mb *messageBuffer) String() string {
	return fmt.Sprintf("%s-MessageBuffer", &mb.TopicPartition)
}

func (mb *messageBuffer) autoFlush() {
	for {
		select {
		case <-mb.Close:
			return
		case <-mb.Timer.C:
			{
				go inLock(&mb.MessageLock, func() {
					if !mb.stopSending {
						Debug(mb, "Batch accumulation timed out. Flushing...")
						mb.Timer.Reset(mb.Config.FetchBatchTimeout)
						mb.flush()
					}
				})
			}
		}
	}
}

func (mb *messageBuffer) flush() {
	if len(mb.Messages) > 0 {
		Debug(mb, "Flushing")
		mb.Timer.Reset(mb.Config.FetchBatchTimeout)
	flushLoop:
		for {
			select {
			case mb.OutputChannel <- mb.Messages:
				break flushLoop
			case <-time.After(200 * time.Millisecond):
				if mb.stopSending {
					return
				}
			}
		}
		Debug(mb, "Flushed")
		mb.Messages = make([]*Message, 0)
	}
}

func (mb *messageBuffer) stop() {
	if !mb.stopSending {
		mb.stopSending = true
		Info(mb, "Trying to stop message buffer")
		inLock(&mb.MessageLock, func() {
			Info(mb, "Stopping message buffer")
			mb.Close <- true
			Info(mb, "Disconnecting channels for partitions")
			mb.disconnectChannelsForPartition <- mb.TopicPartition
			Info(mb, "Stopped message buffer")
		})
	}
}

func (mb *messageBuffer) addBatch(data *TopicPartitionData) {
	inLock(&mb.MessageLock, func() {
		if mb.stopSending {
			return
		}
		fetchResponseBlock := data.Data
		topicPartition := data.TopicPartition
		if topicPartition != mb.TopicPartition {
			panic(fmt.Sprintf("%s got batch for wrong topic and partition: %s", mb, topicPartition))
		}
		if fetchResponseBlock != nil {
			for _, message := range fetchResponseBlock.MsgSet.Messages {
				if message.Msg.Set != nil {
					for _, wrapped := range message.Msg.Set.Messages {
						mb.add(&Message{
							Key:       wrapped.Msg.Key,
							Value:     wrapped.Msg.Value,
							Topic:     topicPartition.Topic,
							Partition: topicPartition.Partition,
							Offset:    wrapped.Offset,
						})
					}
				} else {
					mb.add(&Message{
						Key:       message.Msg.Key,
						Value:     message.Msg.Value,
						Topic:     topicPartition.Topic,
						Partition: topicPartition.Partition,
						Offset:    message.Offset,
					})
				}
			}
		}

	askNextLoop:
		for !mb.stopSending {
			select {
			case mb.askNextBatch <- mb.TopicPartition:
				break askNextLoop
			case <-time.After(200 * time.Millisecond):
			}
		}
	})
}

func (mb *messageBuffer) add(msg *Message) {
	Debugf(mb, "Added message: %s", msg)
	mb.Messages = append(mb.Messages, msg)
	if len(mb.Messages) == mb.Config.FetchBatchSize {
		Debug(mb, "Batch is ready. Flushing")
		mb.flush()
	}
}
