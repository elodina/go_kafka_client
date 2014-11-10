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

type BatchAccumulator struct {
	Config *ConsumerConfig
	InputChannel *SharedBlockChannel
	OutputChannel chan []*Message
	CloseChannel  chan bool
	MessagesBuffer []*Message
}

func NewBatchAccumulator(config *ConsumerConfig, closeChannel chan bool) *BatchAccumulator {
	blockChannel := &SharedBlockChannel{make(chan *TopicPartitionData, config.QueuedMaxMessages), false}
	ba := &BatchAccumulator {
		Config : config,
		InputChannel : blockChannel,
		OutputChannel : make(chan []*Message),
		CloseChannel : closeChannel,
		MessagesBuffer : make([]*Message, 0),
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
		case <-time.After(ba.Config.FetchBatchTimeout): {
			Debug(ba, "Batch accumulation timed out. Flushing...")
			ba.Flush()
		}
		case <-ba.CloseChannel: {
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
						//TODO fix casting int to int32 everywhere
						Partition : int32(topicPartition.Partition),
						Offset : message.Offset,
					}
					ba.MessagesBuffer = append(ba.MessagesBuffer, msg)

					if len(ba.MessagesBuffer) == ba.Config.FetchBatchSize {
						ba.Flush()
					}
				}

			}
		}
		}
	}
}

func (ba *BatchAccumulator) Flush() {
	ba.OutputChannel <- ba.MessagesBuffer
	ba.MessagesBuffer = make([]*Message, 0)
}
