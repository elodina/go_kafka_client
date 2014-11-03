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
	"fmt"
)

var (
	consumers = []string { "consumerid1", "consumerid2" }
	consumerThreadIds = []*ConsumerThreadId {
		&ConsumerThreadId{"consumerid1", 0},
		&ConsumerThreadId{"consumerid1", 1},
		&ConsumerThreadId{"consumerid2", 0},
		&ConsumerThreadId{"consumerid2", 1},
	}
	myTopicThreadIds = map[string][]*ConsumerThreadId {
		"topic1": []*ConsumerThreadId { &ConsumerThreadId{"consumerid1", 0},
										&ConsumerThreadId{"consumerid1", 1}, },
		"topic2": []*ConsumerThreadId { &ConsumerThreadId{"consumerid1", 0},
										&ConsumerThreadId{"consumerid1", 1}, },
		"topic3": []*ConsumerThreadId { &ConsumerThreadId{"consumerid1", 0},
										&ConsumerThreadId{"consumerid1", 1}, },
	}
	partitionsForTopic = map[string][]int {
		"topic1": []int{0, 1, 2, 3, 4, 5},
		"topic2": []int{0, 1, 2, 3, 4},
		"topic3": []int{0},
	}
	consumersForTopic = map[string][]*ConsumerThreadId {
		"topic1": consumerThreadIds,
		"topic2": consumerThreadIds,
		"topic3": consumerThreadIds,
	}
)

func TestRoundRobinAssignor(t *testing.T) {
	assignor := NewPartitionAssignor("roundrobin")
	context := &AssignmentContext{
		ConsumerId: "consumerid1",
		Group: "group",
		MyTopicThreadIds: myTopicThreadIds,
		PartitionsForTopic: partitionsForTopic,
		ConsumersForTopic: consumersForTopic,
		Consumers: consumers,
	}
	ownershipDecision := assignor(context)
	fmt.Printf("%v\n", ownershipDecision)
}


func TestRangeAssignor(t *testing.T) {
	assignor := NewPartitionAssignor("range")
	context := &AssignmentContext{
		ConsumerId: "consumerid1",
		Group: "group",
		MyTopicThreadIds: myTopicThreadIds,
		PartitionsForTopic: partitionsForTopic,
		ConsumersForTopic: consumersForTopic,
		Consumers: consumers,
	}
	ownershipDecision := assignor(context)
	fmt.Printf("%v\n", ownershipDecision)
}
