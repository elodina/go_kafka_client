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

package producer

import (
	"errors"
	"testing"
)

func TestManualPartitioner(t *testing.T) {
	partitioner := new(ManualPartitioner)

	partitions := []int32{0, 1, 2}
	partition, err := partitioner.Partition(&ProducerRecord{Partition: 0}, partitions)
	assert(t, partition, int32(0))
	assert(t, err, nil)

	partition, err = partitioner.Partition(&ProducerRecord{Partition: 2}, partitions)
	assert(t, partition, int32(2))
	assert(t, err, nil)

	partition, err = partitioner.Partition(&ProducerRecord{Partition: 3}, partitions)
	assert(t, partition, int32(-1))
	assert(t, err, errors.New("Invalid partition"))
}
