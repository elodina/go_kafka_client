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
	"github.com/samuel/go-zookeeper/zk"
	"fmt"
)

type OffsetsCommitter struct {
	Config *ConsumerConfig
	WorkerAcks []chan map[TopicAndPartition]int64
	AskNext chan TopicAndPartition
	zkConn *zk.Conn
}

func (oc *OffsetsCommitter) String() string {
	return fmt.Sprintf("%s-offsetsCommitter", oc.Config.ConsumerId)
}

func NewOffsetsCommiter(config *ConsumerConfig, workerAcks []chan map[TopicAndPartition]int64, askNext chan TopicAndPartition, zkConn *zk.Conn) *OffsetsCommitter {
	return &OffsetsCommitter{
		Config: config,
		WorkerAcks: workerAcks,
		AskNext: askNext,
		zkConn: zkConn,
	}
}

func (oc *OffsetsCommitter) Start() {
	acksChannel := make(chan map[TopicAndPartition]int64)
	RedirectChannelsTo(oc.WorkerAcks, acksChannel)
	for {
		ack := <-acksChannel
		Debugf(oc, "Committing offsets: %+v", ack)
		oc.Commit(ack)
		for topicPartition, _ := range ack {
			oc.AskNext <- topicPartition
		}
	}
}

func (oc *OffsetsCommitter) Commit(ack map[TopicAndPartition]int64) {
	for topicPartition, offset := range ack {
		success := false
		for i := 0; i < int(oc.Config.OffsetsCommitMaxRetries); i++ {
			err := CommitOffset(oc.zkConn, oc.Config.Groupid, &topicPartition, offset)
			if err == nil {
				success = true
				Debugf(oc, "Successfully committed offset %d for %s", offset, &topicPartition)
				break
			} else {
				Warnf(oc, "Failed to commit offset %d for %s. Retying...", offset, &topicPartition)
			}
		}

		if !success {
			Errorf(oc, "Failed to commit offset %d for %s after %d retries", offset, &topicPartition, oc.Config.OffsetsCommitMaxRetries)
			//TODO: what to do next?
		}
	}
}
