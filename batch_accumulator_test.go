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
	"testing"
	"time"
	"github.com/Shopify/sarama"
)

func TestMessageBuffer(t *testing.T) {
	config := DefaultConsumerConfig()
	config.FetchBatchSize = 5
	config.FetchBatchTimeout = 3*time.Second

	out := make(chan []*Message)
	topicPartition := &TopicAndPartition{"fakeTopic", 0}
	buffer := NewMessageBuffer(topicPartition, out, config)

	ReceiveNoMessages(t, 4*time.Second, out)

	buffer.Add(&Message{})

	ReceiveN(t, 1, 4*time.Second, out)

	go func() {
		for i := 0; i < config.FetchBatchSize; i++ {
			buffer.Add(&Message{})
		}
	}()

	ReceiveN(t, config.FetchBatchSize, 4*time.Second, out)

	buffer.Add(&Message{})
	buffer.Stop()
	ReceiveNoMessages(t, 4*time.Second, out)
}

func TestBatchAccumulator(t *testing.T) {
	config := DefaultConsumerConfig()
	config.FetchBatchSize = 5
	askNextBatch := make(chan TopicAndPartition)
	reconnectChannels := make(chan bool, 100) //we never read this, so just swallow these messages

	topicPartition1 := TopicAndPartition{"fakeTopic", int32(0)}
	topicPartition2 := TopicAndPartition{"anotherFakeTopic", int32(1)}

	acc := NewBatchAccumulator(config, askNextBatch, reconnectChannels)
	tpd1 := &TopicPartitionData{
		TopicPartition : topicPartition1,
		Data : &sarama.FetchResponseBlock{
			MsgSet: sarama.MessageSet{
				Messages: []*sarama.MessageBlock {
					&sarama.MessageBlock{int64(1), &sarama.Message{}},
					&sarama.MessageBlock{int64(2), &sarama.Message{}},
					&sarama.MessageBlock{int64(3), &sarama.Message{}},
					&sarama.MessageBlock{int64(4), &sarama.Message{}},
					&sarama.MessageBlock{int64(5), &sarama.Message{}},
				},
			},
		},
	}
	tpd2 := &TopicPartitionData{
		TopicPartition : topicPartition2,
		Data : &sarama.FetchResponseBlock{
			MsgSet: sarama.MessageSet{
				Messages: []*sarama.MessageBlock {
					&sarama.MessageBlock{int64(1), &sarama.Message{}},
					&sarama.MessageBlock{int64(2), &sarama.Message{}},
					&sarama.MessageBlock{int64(3), &sarama.Message{}},
					&sarama.MessageBlock{int64(4), &sarama.Message{}},
					&sarama.MessageBlock{int64(5), &sarama.Message{}},
				},
			},
		},
	}
	go func() {
		acc.InputChannel.chunks <- tpd1
		acc.InputChannel.chunks <- tpd2
	}()

	timeout := 1 * time.Second
	select {
	case <-askNextBatch:
	case <-time.After(timeout): {
		t.Errorf("Failed to receive an 'ask next' request from Batch Accumulator within %s", timeout)
	}
	}

	if len(acc.MessageBuffers) != 2 {
		t.Errorf("Batch Accumulator should contain 2 MessageBuffers, actual %d", len(acc.MessageBuffers))
	}

	acc.RemoveBuffer(topicPartition1)
	time.Sleep(1 * time.Second)
	if len(acc.MessageBuffers) != 1 {
		t.Errorf("Batch Accumulator's MessageBuffers should be empty after buffer removal, actual %d", len(acc.MessageBuffers))
	}

	select {
	case <-askNextBatch:
	case <-time.After(timeout): {
		t.Errorf("Failed to receive an 'ask next' request from Batch Accumulator within %s", timeout)
	}
	}

	acc.Stop()
	acc.Stop() //ensure BA does not hang
}
