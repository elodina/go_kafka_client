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
	"github.com/Shopify/sarama"
	"math/rand"
	"testing"
	"time"
)

func TestFilterPartitionData(t *testing.T) {
	rand.Seed(time.Now().Unix())
	startOffset := rand.Int63n(1000) + 10
	numMessages := rand.Intn(1000) + 10

	requestedOffset := startOffset
	data := getFetchResponseBlock(startOffset, numMessages)
	filterPartitionData(data, requestedOffset)
	//no messages should be filtered when startOffset == requestedOffset
	assert(t, len(data.MsgSet.Messages), numMessages)

	requestedOffset = startOffset + rand.Int63n(100)
	filterPartitionData(data, requestedOffset)
	expectedMessages := numMessages - int(requestedOffset-startOffset)
	if expectedMessages < 0 {
		expectedMessages = 0
	}
	//check the filtering itself
	assert(t, len(data.MsgSet.Messages), expectedMessages)

	requestedOffset = startOffset + int64(numMessages)
	data = getFetchResponseBlock(startOffset, numMessages)
	filterPartitionData(data, requestedOffset)
	//should get empty slice if the response does not contain values from requested offset
	assert(t, len(data.MsgSet.Messages), 0)
}

func getFetchResponseBlock(startOffset int64, numMessages int) *sarama.FetchResponseBlock {
	return &sarama.FetchResponseBlock{
		HighWaterMarkOffset: startOffset + int64(numMessages),
		MsgSet: sarama.MessageSet{
			PartialTrailingMessage: false,
			Messages:               generateMessages(startOffset, numMessages),
		},
	}
}

func generateMessages(startOffset int64, numMessages int) []*sarama.MessageBlock {
	messages := make([]*sarama.MessageBlock, 0)
	for i := 0; i < numMessages; i++ {
		messages = append(messages, &sarama.MessageBlock{Offset: startOffset + int64(i)})
	}

	return messages
}
