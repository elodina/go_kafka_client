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
	"sync"
)

type BatchAccumulator struct {
	Config *ConsumerConfig
	InputChannel *SharedBlockChannel
	OutputChannel chan []*Message
	CloseChannel  chan bool
	MessageBuffers map[TopicAndPartition]*MessageBuffer
}

func NewBatchAccumulator(config *ConsumerConfig, closeChannel chan bool) *BatchAccumulator {
	blockChannel := &SharedBlockChannel{make(chan *TopicPartitionData, config.QueuedMaxMessages), false}
	ba := &BatchAccumulator {
		Config : config,
		InputChannel : blockChannel,
		OutputChannel : make(chan []*Message),
		CloseChannel : closeChannel,
		MessageBuffers : make(map[TopicAndPartition]*MessageBuffer),
	}

	go ba.processIncomingBlocks()
	return ba
}

func (ba *BatchAccumulator) String() string {
	return fmt.Sprintf("%s-batchAccumulator", ba.Config.ConsumerId)
}

func (ba *BatchAccumulator) processIncomingBlocks() {
	Debug(ba, "Started processing blocks")
	for {
		select {
		case <-ba.CloseChannel: {
			for _, buffer := range ba.MessageBuffers {
				buffer.Stop()
			}
			return
		}
		case b := <-ba.InputChannel.chunks: {
			fetchResponseBlock := b.Data
			topicPartition := b.TopicPartition
			if fetchResponseBlock != nil {
				for _, message := range fetchResponseBlock.MsgSet.Messages {
					msg := &Message {
						Key : message.Msg.Key,
						Value : message.Msg.Value,
						Topic : topicPartition.Topic,
						Partition : topicPartition.Partition,
						Offset : message.Offset,
					}
					buffer, exists := ba.MessageBuffers[topicPartition]
					if !exists {
						ba.MessageBuffers[topicPartition] = NewMessageBuffer(&topicPartition, ba.OutputChannel, ba.Config)
						buffer = ba.MessageBuffers[topicPartition]
					}
					buffer.Add(msg)
				}
			}
		}
		}
	}
}

type MessageBuffer struct {
	OutputChannel chan []*Message
	Messages      []*Message
	Config *ConsumerConfig
	Timer *time.Timer
	MessageLock sync.Mutex
	Close       chan bool
	TopicPartition *TopicAndPartition
}

func NewMessageBuffer(topicPartition *TopicAndPartition, outputChannel chan []*Message, config *ConsumerConfig) *MessageBuffer {
	buffer := &MessageBuffer{
		OutputChannel : outputChannel,
		Messages : make([]*Message, 0),
		Config : config,
		Timer : time.NewTimer(config.FetchBatchTimeout),
		Close : make(chan bool),
		TopicPartition : topicPartition,
	}

	go buffer.Start()

	return buffer
}

func (mb *MessageBuffer) String() string {
	return fmt.Sprintf("%s-messageBuffer", mb.TopicPartition)
}

func (mb *MessageBuffer) Start() {
	for {
		select {
		case <-mb.Close: return
		case <-mb.Timer.C: {
			Debug(mb, "Batch accumulation timed out. Flushing...")
			mb.Flush()
		}
		}
	}
}

func (mb *MessageBuffer) Stop() {
	mb.Close <- true
	mb.Flush()
}

func (mb *MessageBuffer) Add(msg *Message) {
	InLock(&mb.MessageLock, func() {
		mb.Messages = append(mb.Messages, msg)
	})
	if len(mb.Messages) == mb.Config.FetchBatchSize {
		Debug(mb, "Batch is ready. Flushing")
		mb.Flush()
	}
}

func (mb *MessageBuffer) Flush() {
	if len(mb.Messages) > 0 {
		mb.OutputChannel <- mb.Messages
		InLock(&mb.MessageLock, func() {
			mb.Messages = make([]*Message, 0)
		})
	}
	mb.Timer.Reset(mb.Config.FetchBatchTimeout)
}
