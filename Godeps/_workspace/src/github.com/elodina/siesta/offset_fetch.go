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

// OffsetFetchRequest is used to fetch offsets for a consumer group and given topic partitions.
type OffsetFetchRequest struct {
	GroupID     string
	RequestInfo map[string][]int32
}

// NewOffsetFetchRequest creates a new OffsetFetchRequest for a given consumer group.
func NewOffsetFetchRequest(group string) *OffsetFetchRequest {
	return &OffsetFetchRequest{GroupID: group}
}

// Key returns the Kafka API key for OffsetFetchRequest.
func (ofr *OffsetFetchRequest) Key() int16 {
	return 9
}

// Version returns the Kafka request version for backwards compatibility.
func (ofr *OffsetFetchRequest) Version() int16 {
	return 0
}

func (ofr *OffsetFetchRequest) Write(encoder Encoder) {
	encoder.WriteString(ofr.GroupID)
	encoder.WriteInt32(int32(len(ofr.RequestInfo)))

	for topic, partitions := range ofr.RequestInfo {
		encoder.WriteString(topic)
		encoder.WriteInt32(int32(len(partitions)))

		for _, partition := range partitions {
			encoder.WriteInt32(partition)
		}
	}
}

// AddOffset is a convenience method to add a topic partition to this OffsetFetchRequest.
func (ofr *OffsetFetchRequest) AddOffset(topic string, partition int32) {
	if ofr.RequestInfo == nil {
		ofr.RequestInfo = make(map[string][]int32)
	}

	ofr.RequestInfo[topic] = append(ofr.RequestInfo[topic], partition)
}

// OffsetFetchResponse contains fetched offsets for each requested topic partition.
type OffsetFetchResponse struct {
	Offsets map[string]map[int32]*OffsetMetadataAndError
}

func (ofr *OffsetFetchResponse) Read(decoder Decoder) *DecodingError {
	offsetsLength, err := decoder.GetInt32()
	if err != nil {
		return NewDecodingError(err, reasonInvalidOffsetsMapLength)
	}

	ofr.Offsets = make(map[string]map[int32]*OffsetMetadataAndError)
	for i := int32(0); i < offsetsLength; i++ {
		topic, err := decoder.GetString()
		if err != nil {
			return NewDecodingError(err, reasonInvalidOffsetFetchResponseTopic)
		}

		offsetsForTopic := make(map[int32]*OffsetMetadataAndError)
		ofr.Offsets[topic] = offsetsForTopic

		partitionsLength, err := decoder.GetInt32()
		if err != nil {
			return NewDecodingError(err, reasonInvalidOffsetFetchResponsePartitionsLength)
		}

		for j := int32(0); j < partitionsLength; j++ {
			partition, err := decoder.GetInt32()
			if err != nil {
				return NewDecodingError(err, reasonInvalidOffsetFetchResponsePartition)
			}

			fetchedOffset := new(OffsetMetadataAndError)
			if err := fetchedOffset.Read(decoder); err != nil {
				return err
			}

			offsetsForTopic[partition] = fetchedOffset
		}
	}

	return nil
}

// OffsetMetadataAndError contains a fetched offset for a topic partition, optional metadata and an error if it occurred.
type OffsetMetadataAndError struct {
	Offset   int64
	Metadata string
	Error    error
}

func (ofr *OffsetMetadataAndError) Read(decoder Decoder) *DecodingError {
	offset, err := decoder.GetInt64()
	if err != nil {
		return NewDecodingError(err, reasonInvalidOffsetFetchResponseOffset)
	}
	ofr.Offset = offset

	//TODO metadata returned by Kafka is always empty even if was passed in OffsetCommitRequest. bug?
	metadata, err := decoder.GetString()
	if err != nil {
		return NewDecodingError(err, reasonInvalidOffsetFetchResponseMetadata)
	}
	ofr.Metadata = metadata

	errCode, err := decoder.GetInt16()
	if err != nil {
		return NewDecodingError(err, reasonInvalidOffsetFetchResponseErrorCode)
	}
	ofr.Error = BrokerErrors[errCode]

	return nil
}

var (
	reasonInvalidOffsetFetchResponseTopic            = "Invalid topic in OffsetFetchResponse"
	reasonInvalidOffsetFetchResponsePartitionsLength = "Invalid length for partition data in OffsetFetchResponse"
	reasonInvalidOffsetFetchResponsePartition        = "Invalid partition in OffsetFetchResponse"
	reasonInvalidOffsetFetchResponseOffset           = "Invalid offset in OffsetFetchResponse"
	reasonInvalidOffsetFetchResponseMetadata         = "Invalid metadata in OffsetFetchResponse"
	reasonInvalidOffsetFetchResponseErrorCode        = "Invalid error code in OffsetFetchResponse"
)
