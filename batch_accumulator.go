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
	MessageBuffers map[TopicAndPartition]*MessageBuffer
	closeFinished chan bool
	closed        bool
	closeLock     sync.Mutex
}

func NewBatchAccumulator(config *ConsumerConfig) *BatchAccumulator {
	blockChannel := &SharedBlockChannel{make(chan *TopicPartitionData, config.QueuedMaxMessages), false}
	ba := &BatchAccumulator {
		Config : config,
		InputChannel : blockChannel,
		OutputChannel : make(chan []*Message),
		MessageBuffers : make(map[TopicAndPartition]*MessageBuffer),
		closeFinished : make(chan bool),
	}

	go ba.processIncomingBlocks()
	return ba
}

func (ba *BatchAccumulator) String() string {
	return fmt.Sprintf("%s-batchAccumulator", ba.Config.ConsumerId)
}

func (ba *BatchAccumulator) processIncomingBlocks() {
	Debug(ba, "Started processing blocks")
	for !ba.closed {
		var b *TopicPartitionData
		select {
		case b = <-ba.InputChannel.chunks:
		case <-time.After(1 * time.Second): {
			Debugf(ba, "Timed out")
		}
		}
		InLock(&ba.closeLock, func() {
			Debugf(ba, "Acquired lock for BA close")
			if ba.closed || b == nil {
				Debugf(ba, "BA closed: %s", ba.closed)
				return
			}
			Debugf(ba, "BA is not closed")
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
			Debugf(ba, "Released lock for BA close")
		})
		time.Sleep(1 * time.Second)
	}

	Debug(ba, "Exited BA processing loop")

	for _, buffer := range ba.MessageBuffers {
		buffer.Stop()
	}

	Debug(ba, "Closed batch accumulator")
	ba.closeFinished <- true
}

func (ba *BatchAccumulator) Stop() chan bool {
	Debug(ba, "Trying to stop BA")
	Debugf(ba, "BA is closed = %b", ba.closed)
	InLock(&ba.closeLock, func() {
		ba.closed = true
	})
	Debug(ba, "BA close flag set")

	return ba.closeFinished
}

type MessageBuffer struct {
	OutputChannel chan []*Message
	Messages      []*Message
	Config *ConsumerConfig
	Timer *time.Timer
	MessageLock   sync.Mutex
	Close         chan bool
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
	Debug(mb, "Stopping message buffer")
	mb.Close <- true
	Debug(mb, "Stopped message buffer")
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
		Debug(mb, "Flushing")
		mb.OutputChannel <- mb.Messages
		Debug(mb, "Flushed")
		InLock(&mb.MessageLock, func() {
			mb.Messages = make([]*Message, 0)
		})
	}
	mb.Timer.Reset(mb.Config.FetchBatchTimeout)
}
