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
)

func TestMessageBuffer(t *testing.T) {
	config := DefaultConsumerConfig()
	config.FetchBatchSize = 5
	config.FetchBatchTimeout = 3 * time.Second
	config.FetchBatchFlushBackoff = 1 * time.Second

	out := make(chan []*Message)
	topicPartition := &TopicAndPartition{"fakeTopic", 0}
	buffer := NewMessageBuffer(topicPartition, out, config)

	ReceiveNoMessages(t, 4 * time.Second, out)

	buffer.Add(&Message{})

	ReceiveN(t, 1, 4 * time.Second, out)

	go func() {
		for i := 0; i < config.FetchBatchSize; i++ {
			buffer.Add(&Message{})
		}
	}()

	ReceiveN(t, config.FetchBatchSize, 4 * time.Second, out)
}
