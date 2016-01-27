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

package siesta

// LatestTime is a value used to request for the latest offset (i.e. the offset of the next coming message).
const LatestTime int64 = -1

// EarliestTime is a value used to request for the earliest available offset.
const EarliestTime int64 = -2

// OffsetRequest describes the valid offset range available for a set of topic-partitions.
type OffsetRequest struct {
	RequestInfo map[string][]*PartitionOffsetRequestInfo
}

// Key returns the Kafka API key for OffsetRequest.
func (or *OffsetRequest) Key() int16 {
	return 2
}

// Version returns the Kafka request version for backwards compatibility.
func (or *OffsetRequest) Version() int16 {
	return 0
}

// AddPartitionOffsetRequestInfo is a convenience method to add a PartitionOffsetRequestInfo to this request.
func (or *OffsetRequest) AddPartitionOffsetRequestInfo(topic string, partition int32, time int64, maxNumOffsets int32) {
	if or.RequestInfo == nil {
		or.RequestInfo = make(map[string][]*PartitionOffsetRequestInfo)
	}

	or.RequestInfo[topic] = append(or.RequestInfo[topic], &PartitionOffsetRequestInfo{Partition: partition, Time: time, MaxNumOffsets: maxNumOffsets})
}

func (or *OffsetRequest) Write(encoder Encoder) {
	//Normal client consumers should always specify ReplicaId as -1 as they have no node id
	encoder.WriteInt32(-1)
	encoder.WriteInt32(int32(len(or.RequestInfo)))

	for topic, partitionOffsetInfos := range or.RequestInfo {
		encoder.WriteString(topic)
		encoder.WriteInt32(int32(len(partitionOffsetInfos)))
		for _, info := range partitionOffsetInfos {
			encoder.WriteInt32(info.Partition)
			encoder.WriteInt64(int64(info.Time))
			encoder.WriteInt32(info.MaxNumOffsets)
		}
	}
}

// OffsetResponse contains the starting offset of each segment for the requested partition as well as the "log end offset"
// i.e. the offset of the next message that would be appended to the given partition.
type OffsetResponse struct {
	PartitionErrorAndOffsets map[string]map[int32]*PartitionOffsetsResponse
}

func (or *OffsetResponse) Read(decoder Decoder) *DecodingError {
	offsetsLength, err := decoder.GetInt32()
	if err != nil {
		return NewDecodingError(err, reasonInvalidOffsetsLength)
	}

	or.PartitionErrorAndOffsets = make(map[string]map[int32]*PartitionOffsetsResponse)
	for i := int32(0); i < offsetsLength; i++ {
		topic, err := decoder.GetString()
		if err != nil {
			return NewDecodingError(err, reasonInvalidOffsetTopic)
		}
		offsetsForTopic := make(map[int32]*PartitionOffsetsResponse)
		or.PartitionErrorAndOffsets[topic] = offsetsForTopic

		partitionOffsetsLength, err := decoder.GetInt32()
		if err != nil {
			return NewDecodingError(err, reasonInvalidPartitionOffsetsLength)
		}

		for j := int32(0); j < partitionOffsetsLength; j++ {
			partition, err := decoder.GetInt32()
			if err != nil {
				return NewDecodingError(err, reasonInvalidPartitionOffsetsPartition)
			}

			partitionOffsets := new(PartitionOffsetsResponse)
			decodingErr := partitionOffsets.Read(decoder)
			if decodingErr != nil {
				return decodingErr
			}
			or.PartitionErrorAndOffsets[topic][partition] = partitionOffsets
		}
	}

	return nil
}

// PartitionOffsetRequestInfo contains partition specific configurations to fetch offsets.
type PartitionOffsetRequestInfo struct {
	Partition     int32
	Time          int64
	MaxNumOffsets int32
}

// PartitionOffsetsResponse contain offsets for a single partition and an error if it occurred.
type PartitionOffsetsResponse struct {
	Error   error
	Offsets []int64
}

func (po *PartitionOffsetsResponse) Read(decoder Decoder) *DecodingError {
	errCode, err := decoder.GetInt16()
	if err != nil {
		return NewDecodingError(err, reasonInvalidPartitionOffsetsErrorCode)
	}
	po.Error = BrokerErrors[errCode]

	offsetsLength, err := decoder.GetInt32()
	if err != nil {
		return NewDecodingError(err, reasonInvalidPartitionOffsetsOffsetsLength)
	}
	po.Offsets = make([]int64, offsetsLength)
	for i := int32(0); i < offsetsLength; i++ {
		offset, err := decoder.GetInt64()
		if err != nil {
			return NewDecodingError(err, reasonInvalidPartitionOffset)
		}
		po.Offsets[i] = offset
	}

	return nil
}

var (
	reasonInvalidOffsetsLength                 = "Invalid length for Offsets field"
	reasonInvalidOffsetTopic                   = "Invalid topic in offset map"
	reasonInvalidPartitionOffsetsLength        = "Invalid length for partition offsets field"
	reasonInvalidPartitionOffsetsPartition     = "Invalid partition in partition offset"
	reasonInvalidPartitionOffsetsErrorCode     = "Invalid error code in partition offset"
	reasonInvalidPartitionOffsetsOffsetsLength = "Invalid length for offsets field in partition offset"
	reasonInvalidPartitionOffset               = "Invalid offset in partition offset"
)
