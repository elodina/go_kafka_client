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
	"testing"
)

var (
	consumers         = []string{"consumerid1", "consumerid2"}
	consumerThreadIds = []ConsumerThreadId{
		ConsumerThreadId{"consumerid1", 0},
		ConsumerThreadId{"consumerid1", 1},
		ConsumerThreadId{"consumerid2", 0},
		ConsumerThreadId{"consumerid2", 1},
	}
	partitionsForTopic = map[string][]int32{
		"topic1": []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
	}
	consumersForTopic = map[string][]ConsumerThreadId{
		"topic1": consumerThreadIds,
	}
	totalPartitions = 10
)

func TestRoundRobinAssignor(t *testing.T) {
	//basic scenario
	assignor := newPartitionAssignor("roundrobin")
	context := &assignmentContext{
		Group:              "group",
		PartitionsForTopic: partitionsForTopic,
		ConsumersForTopic:  consumersForTopic,
		Consumers:          consumers,
	}

	assignments := make(map[TopicAndPartition]string)
	var totalDecisions = 0
	for _, consumer := range consumers {
		context.ConsumerId = consumer
		context.MyTopicThreadIds = map[string][]ConsumerThreadId{
			"topic1": []ConsumerThreadId{
				ConsumerThreadId{consumer, 0},
				ConsumerThreadId{consumer, 1}},
		}
		ownershipDecision := assignor(context)
		decisionsNum := len(ownershipDecision)
		if decisionsNum == totalPartitions {
			t.Errorf("Too many partitions assigned to consumer %s", consumer)
		}

		for topicAndPartition := range ownershipDecision {
			if owner, exists := assignments[topicAndPartition]; exists {
				t.Errorf("Consumer %s tried to own topic %s and partition %d previously owned by consumer %s", consumer, topicAndPartition.Topic, topicAndPartition.Partition, owner)
			}
			assignments[topicAndPartition] = consumer
		}

		totalDecisions += decisionsNum
	}

	assert(t, totalDecisions, totalPartitions)

	//NOT every topic has the same number of streams within a consumer instance
	failed := false
	defer func() {
		r := recover()
		if r != nil {
			failed = true
		}
	}()

	context.ConsumerId = "consumerid1"
	context.MyTopicThreadIds = map[string][]ConsumerThreadId{
		"topic1": []ConsumerThreadId{
			ConsumerThreadId{"consumerid1", 0},
			ConsumerThreadId{"consumerid1", 1}},
	}
	context.ConsumersForTopic = map[string][]ConsumerThreadId{
		"topic1": consumerThreadIds,
		"topic2": []ConsumerThreadId{
			ConsumerThreadId{"consumerid1", 0},
			ConsumerThreadId{"consumerid1", 1},
			ConsumerThreadId{"consumerid2", 0},
		},
	}
	assignor(context)

	assert(t, failed, true)
}

func TestRangeAssignor(t *testing.T) {
	//basic scenario
	assignor := newPartitionAssignor("range")
	context := &assignmentContext{
		Group:              "group",
		PartitionsForTopic: partitionsForTopic,
		ConsumersForTopic:  consumersForTopic,
		Consumers:          consumers,
	}

	var totalDecisions = 0
	for _, consumer := range consumers {
		context.ConsumerId = consumer
		context.MyTopicThreadIds = map[string][]ConsumerThreadId{
			"topic1": []ConsumerThreadId{
				ConsumerThreadId{consumer, 0},
				ConsumerThreadId{consumer, 1}},
		}
		ownershipDecision := assignor(context)
		decisionsNum := len(ownershipDecision)
		if decisionsNum == totalPartitions {
			t.Errorf("too many partitions assigned to consumer %s", consumer)
		}

		totalDecisions += decisionsNum
		t.Logf("%v\n", ownershipDecision)
	}

	assert(t, totalDecisions, totalPartitions)
}
