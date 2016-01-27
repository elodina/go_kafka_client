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

// OffsetCommitRequest is used to commit offsets for a group/topic/partition.
type OffsetCommitRequest struct {
	GroupID     string
	RequestInfo map[string]map[int32]*OffsetAndMetadata
}

// NewOffsetCommitRequest creates a new OffsetCommitRequest for a given consumer group.
func NewOffsetCommitRequest(group string) *OffsetCommitRequest {
	return &OffsetCommitRequest{GroupID: group}
}

// Key returns the Kafka API key for OffsetCommitRequest.
func (ocr *OffsetCommitRequest) Key() int16 {
	return 8
}

// Version returns the Kafka request version for backwards compatibility.
func (ocr *OffsetCommitRequest) Version() int16 {
	return 0
}

func (ocr *OffsetCommitRequest) Write(encoder Encoder) {
	encoder.WriteString(ocr.GroupID)
	encoder.WriteInt32(int32(len(ocr.RequestInfo)))

	for topic, partitionOffsetAndMetadata := range ocr.RequestInfo {
		encoder.WriteString(topic)
		encoder.WriteInt32(int32(len(partitionOffsetAndMetadata)))
		for partition, offsetAndMetadata := range partitionOffsetAndMetadata {
			encoder.WriteInt32(partition)
			encoder.WriteInt64(offsetAndMetadata.Offset)
			encoder.WriteInt64(offsetAndMetadata.Timestamp)
			encoder.WriteString(offsetAndMetadata.Metadata)
		}
	}
}

// AddOffset is a convenience method to add an offset for a topic partition.
func (ocr *OffsetCommitRequest) AddOffset(topic string, partition int32, offset int64, timestamp int64, metadata string) {
	if ocr.RequestInfo == nil {
		ocr.RequestInfo = make(map[string]map[int32]*OffsetAndMetadata)
	}

	partitionOffsetAndMetadata, exists := ocr.RequestInfo[topic]
	if !exists {
		ocr.RequestInfo[topic] = make(map[int32]*OffsetAndMetadata)
		partitionOffsetAndMetadata = ocr.RequestInfo[topic]
	}

	partitionOffsetAndMetadata[partition] = &OffsetAndMetadata{Offset: offset, Timestamp: timestamp, Metadata: metadata}
}

// OffsetCommitResponse contains errors for partitions if they occur.
type OffsetCommitResponse struct {
	CommitStatus map[string]map[int32]error
}

func (ocr *OffsetCommitResponse) Read(decoder Decoder) *DecodingError {
	ocr.CommitStatus = make(map[string]map[int32]error)

	offsetsLength, err := decoder.GetInt32()
	if err != nil {
		return NewDecodingError(err, reasonInvalidOffsetsMapLength)
	}

	for i := int32(0); i < offsetsLength; i++ {
		topic, err := decoder.GetString()
		if err != nil {
			return NewDecodingError(err, reasonInvalidOffsetsTopic)
		}
		errorsForTopic := make(map[int32]error)
		ocr.CommitStatus[topic] = errorsForTopic

		partitionsLength, err := decoder.GetInt32()
		if err != nil {
			return NewDecodingError(err, reasonInvalidOffsetsPartitionsLength)
		}

		for j := int32(0); j < partitionsLength; j++ {
			partition, err := decoder.GetInt32()
			if err != nil {
				return NewDecodingError(err, reasonInvalidOffsetsPartition)
			}

			errCode, err := decoder.GetInt16()
			if err != nil {
				return NewDecodingError(err, reasonInvalidOffsetsErrorCode)
			}

			errorsForTopic[partition] = BrokerErrors[errCode]
		}
	}

	return nil
}

// OffsetAndMetadata contains offset for a partition and optional metadata.
type OffsetAndMetadata struct {
	Offset    int64
	Timestamp int64
	Metadata  string
}

var (
	reasonInvalidOffsetsMapLength        = "Invalid length for Offsets field"
	reasonInvalidOffsetsTopic            = "Invalid topic in OffsetCommitResponse"
	reasonInvalidOffsetsPartitionsLength = "Invalid length for partitions in OffsetCommitResponse"
	reasonInvalidOffsetsPartition        = "Invalid partition in OffsetCommitResponse"
	reasonInvalidOffsetsErrorCode        = "Invalid error code in OffsetCommitResponse"
)
