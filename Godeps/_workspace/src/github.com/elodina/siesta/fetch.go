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

// FetchRequest is used to fetch a chunk of one or more logs for some topic-partitions.
type FetchRequest struct {
	MaxWait     int32
	MinBytes    int32
	RequestInfo map[string][]*PartitionFetchInfo
}

// Write writes the FetchRequest to the given Encoder.
func (fr *FetchRequest) Write(encoder Encoder) {
	//Normal client consumers should always specify ReplicaId as -1 as they have no node id
	encoder.WriteInt32(-1)
	encoder.WriteInt32(fr.MaxWait)
	encoder.WriteInt32(fr.MinBytes)
	encoder.WriteInt32(int32(len(fr.RequestInfo)))

	for topic, partitionFetchInfos := range fr.RequestInfo {
		encoder.WriteString(topic)
		encoder.WriteInt32(int32(len(partitionFetchInfos)))
		for _, info := range partitionFetchInfos {
			encoder.WriteInt32(info.Partition)
			encoder.WriteInt64(info.Offset)
			encoder.WriteInt32(info.FetchSize)
		}
	}
}

// Key returns the Kafka API key for FetchRequest.
func (fr *FetchRequest) Key() int16 {
	return 1
}

// Version returns the Kafka request version for backwards compatibility.
func (fr *FetchRequest) Version() int16 {
	return 0
}

// AddFetch is a convenience method to add a PartitionFetchInfo.
func (fr *FetchRequest) AddFetch(topic string, partition int32, offset int64, fetchSize int32) {
	if fr.RequestInfo == nil {
		fr.RequestInfo = make(map[string][]*PartitionFetchInfo)
	}

	fr.RequestInfo[topic] = append(fr.RequestInfo[topic], &PartitionFetchInfo{Partition: partition, Offset: offset, FetchSize: fetchSize})
}

// FetchResponse contains FetchResponseData for all requested topics and partitions.
type FetchResponse struct {
	Data map[string]map[int32]*FetchResponsePartitionData
}

func (fr *FetchResponse) Read(decoder Decoder) *DecodingError {
	fr.Data = make(map[string]map[int32]*FetchResponsePartitionData)

	blocksLength, err := decoder.GetInt32()
	if err != nil {
		return NewDecodingError(err, reasonInvalidBlocksLength)
	}

	for i := int32(0); i < blocksLength; i++ {
		topic, err := decoder.GetString()
		if err != nil {
			return NewDecodingError(err, reasonInvalidBlockTopic)
		}
		fr.Data[topic] = make(map[int32]*FetchResponsePartitionData)

		fetchResponseDataLength, err := decoder.GetInt32()
		if err != nil {
			return NewDecodingError(err, reasonInvalidFetchResponseDataLength)
		}
		for j := int32(0); j < fetchResponseDataLength; j++ {
			partition, err := decoder.GetInt32()
			if err != nil {
				return NewDecodingError(err, reasonInvalidFetchResponseDataPartition)
			}

			fetchResponseData := new(FetchResponsePartitionData)
			decodingErr := fetchResponseData.Read(decoder)
			if decodingErr != nil {
				return decodingErr
			}

			fr.Data[topic][partition] = fetchResponseData
		}
	}

	return nil
}

// GetMessages traverses this FetchResponse and collects all messages.
// Returns an error if FetchResponse contains one.
// Messages should be ordered by offset.
func (fr *FetchResponse) GetMessages() ([]*MessageAndMetadata, error) {
	var messages []*MessageAndMetadata

	collector := func(topic string, partition int32, offset int64, key []byte, value []byte) error {
		messages = append(messages, &MessageAndMetadata{
			Topic:     topic,
			Partition: partition,
			Offset:    offset,
			Key:       key,
			Value:     value,
		})
		return nil
	}

	err := fr.CollectMessages(collector)
	return messages, err
}

// Error returns the error message for a given topic and pertion of this FetchResponse
func (fr *FetchResponse) Error(topic string, partition int32) error {
	t, ok := fr.Data[topic]
	if !ok {
		return nil
	}
	p, ok := t[partition]
	if !ok {
		return nil
	}
	return p.Error
}

// CollectMessages traverses this FetchResponse and applies a collector function to each message
// giving the possibility to avoid response -> siesta.Message -> other.Message conversion if necessary.
func (fr *FetchResponse) CollectMessages(collector func(topic string, partition int32, offset int64, key []byte, value []byte) error) error {
	for topic, partitionAndData := range fr.Data {
		for partition, data := range partitionAndData {
			if data.Error != ErrNoError {
				return data.Error
			}
			for _, messageAndOffset := range data.Messages {
				if messageAndOffset.Message.Nested != nil {
					for _, nested := range messageAndOffset.Message.Nested {
						err := collector(topic, partition, nested.Offset, nested.Message.Key, nested.Message.Value)
						if err != nil {
							return err
						}
					}
				} else {
					err := collector(topic, partition, messageAndOffset.Offset, messageAndOffset.Message.Key, messageAndOffset.Message.Value)
					if err != nil {
						return err
					}
				}
			}
		}
	}

	return nil
}

// PartitionFetchInfo contains information about what partition to fetch, what offset to fetch from and the maximum bytes to include in the message set for this partition.
type PartitionFetchInfo struct {
	Partition int32
	Offset    int64
	FetchSize int32
}

// FetchResponsePartitionData contains fetched messages for a single partition, the offset at the end of the log for this partition and an error code.
type FetchResponsePartitionData struct {
	Error               error
	HighwaterMarkOffset int64
	Messages            []*MessageAndOffset
}

func (frd *FetchResponsePartitionData) Read(decoder Decoder) *DecodingError {
	errCode, err := decoder.GetInt16()
	if err != nil {
		return NewDecodingError(err, reasonInvalidFetchResponseDataErrorCode)
	}
	frd.Error = BrokerErrors[errCode]

	highwaterMarkOffset, err := decoder.GetInt64()
	if err != nil {
		return NewDecodingError(err, reasonInvalidFetchResponseDataHighwaterMarkOffset)
	}
	frd.HighwaterMarkOffset = highwaterMarkOffset

	if _, err = decoder.GetInt32(); err != nil {
		return NewDecodingError(err, reasonInvalidMessageSetLength)
	}

	messages, decodingErr := ReadMessageSet(decoder)
	if decodingErr != nil {
		return decodingErr
	}
	frd.Messages = messages

	return nil
}

const (
	reasonInvalidBlocksLength                         = "Invalid length for Blocks field"
	reasonInvalidBlockTopic                           = "Invalid topic in block"
	reasonInvalidFetchResponseDataLength              = "Invalid length for FetchResponseData field"
	reasonInvalidFetchResponseDataPartition           = "Invalid partition in FetchResponseData"
	reasonInvalidFetchResponseDataErrorCode           = "Invalid error code in FetchResponseData"
	reasonInvalidFetchResponseDataHighwaterMarkOffset = "Invalid highwater mark offset in FetchResponseData"
	reasonInvalidMessageSetLength                     = "Invalid MessageSet length"
)
