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

package siesta

import (
	"fmt"
	"math/rand"
	"net"
	"os"
	"testing"
	"time"
)

var ci = os.Getenv("TRAVIS_CI") != ""
var brokerUp = true
var brokerAddr = "localhost:9092"

func init() {
	conn, err := net.Dial("tcp", brokerAddr)
	if err == nil {
		brokerUp = true
		conn.Close()
	}
}

func TestDefaultConnectorFunctional(t *testing.T) {
	if !brokerUp && !ci {
		t.Skip("Broker is not running. Please spin up the broker at localhost:9092 for this test to work.")
	}

	numMessages := 1000
	topicName := fmt.Sprintf("siesta-%d", time.Now().Unix())

	connector := testConnector(t)
	testTopicMetadata(t, topicName, connector)
	testOffsetStorage(t, topicName, connector)
	testProduce(t, topicName, numMessages, connector)
	testConsume(t, topicName, numMessages, connector)
	closeWithin(t, time.Second, connector)
	//check whether closing multiple times hangs
	closeWithin(t, time.Second, connector)

	anotherConnector := testConnector(t)
	//should also work fine - must get topic metadata before consuming
	testConsume(t, topicName, numMessages, anotherConnector)
	closeWithin(t, time.Second, anotherConnector)
}

func testTopicMetadata(t *testing.T, topicName string, connector *DefaultConnector) {
	metadata, err := connector.GetTopicMetadata([]string{topicName})
	assertFatal(t, err, nil)

	assertNot(t, len(metadata.Brokers), 0)
	assertNot(t, len(metadata.TopicsMetadata), 0)
	if len(metadata.Brokers) > 1 {
		t.Skip("Cluster should consist only of one broker for this test to run.")
	}

	broker := metadata.Brokers[0]
	assert(t, broker.ID, int32(0))
	if ci {
		// this can be asserted on Travis only as we are guaranteed to advertise the broker as localhost
		assert(t, broker.Host, "localhost")
	}
	assert(t, broker.Port, int32(9092))

	topicMetadata := findTopicMetadata(t, metadata.TopicsMetadata, topicName)
	assert(t, topicMetadata.Error, ErrNoError)
	assert(t, topicMetadata.Topic, topicName)
	assertFatal(t, len(topicMetadata.PartitionsMetadata), 1)

	partitionMetadata := topicMetadata.PartitionsMetadata[0]
	assert(t, partitionMetadata.Error, ErrNoError)
	assert(t, partitionMetadata.ISR, []int32{0})
	assert(t, partitionMetadata.Leader, int32(0))
	assert(t, partitionMetadata.PartitionID, int32(0))
	assert(t, partitionMetadata.Replicas, []int32{0})
}

func testOffsetStorage(t *testing.T, topicName string, connector *DefaultConnector) {
	group := fmt.Sprintf("test-%d", time.Now().Unix())
	targetOffset := rand.Int63()

	offset, err := connector.GetOffset(group, topicName, 0)
	assertFatal(t, err, ErrUnknownTopicOrPartition)
	assert(t, offset, int64(-1))

	err = connector.CommitOffset(group, topicName, 0, targetOffset)
	assertFatal(t, err, nil)

	offset, err = connector.GetOffset(group, topicName, 0)
	assertFatal(t, err, nil)
	assert(t, offset, targetOffset)
}

func testProduce(t *testing.T, topicName string, numMessages int, connector *DefaultConnector) {
	produceRequest := new(ProduceRequest)
	produceRequest.AckTimeoutMs = 1000
	produceRequest.RequiredAcks = 1
	for i := 0; i < numMessages; i++ {
		produceRequest.AddMessage(topicName, 0, &Message{
			Key:   []byte(fmt.Sprintf("%d", numMessages-i)),
			Value: []byte(fmt.Sprintf("%d", i)),
		})
	}

	leader, err := connector.tryGetLeader(topicName, 0, 3)
	assert(t, err, nil)
	assertNot(t, leader, (*brokerLink)(nil))
	bytes, err := connector.syncSendAndReceive(leader, produceRequest)
	assertFatal(t, err, nil)

	produceResponse := new(ProduceResponse)
	decodingErr := connector.decode(bytes, produceResponse)
	assertFatal(t, decodingErr, (*DecodingError)(nil))

	topicBlock, exists := produceResponse.Status[topicName]
	assertFatal(t, exists, true)
	partitionBlock, exists := topicBlock[int32(0)]
	assertFatal(t, exists, true)

	assert(t, partitionBlock.Error, ErrNoError)
	assert(t, partitionBlock.Offset, int64(0))
}

func testConsume(t *testing.T, topicName string, numMessages int, connector *DefaultConnector) {
	response, err := connector.Fetch(topicName, 0, 0)
	assertFatal(t, response.Error(topicName, 0), ErrNoError)
	assertFatal(t, err, nil)
	messages, err := response.GetMessages()
	assertFatal(t, err, nil)
	assertFatal(t, len(messages), numMessages)
	for i := 0; i < numMessages; i++ {
		message := messages[i]
		assert(t, message.Topic, topicName)
		assert(t, message.Partition, int32(0))
		assert(t, message.Offset, int64(i))
		assert(t, message.Key, []byte(fmt.Sprintf("%d", numMessages-i)))
		assert(t, message.Value, []byte(fmt.Sprintf("%d", i)))
	}
}

func findTopicMetadata(t *testing.T, metadata []*TopicMetadata, topic string) *TopicMetadata {
	for _, topicMetadata := range metadata {
		if topicMetadata.Topic == topic {
			return topicMetadata
		}
	}

	t.Fatalf("TopicMetadata for topic %s not found", topic)
	return nil
}
