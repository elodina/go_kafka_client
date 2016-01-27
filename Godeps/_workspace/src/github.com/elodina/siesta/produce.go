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

// ProduceRequest is used to send message sets to the server.
type ProduceRequest struct {
	RequiredAcks int16
	AckTimeoutMs int32
	Data         map[string]map[int32][]*MessageAndOffset
}

// Key returns the Kafka API key for ProduceRequest.
func (pr *ProduceRequest) Key() int16 {
	return 0
}

// Version returns the Kafka request version for backwards compatibility.
func (pr *ProduceRequest) Version() int16 {
	return 0
}

func (pr *ProduceRequest) Write(encoder Encoder) {
	encoder.WriteInt16(pr.RequiredAcks)
	encoder.WriteInt32(pr.AckTimeoutMs)
	encoder.WriteInt32(int32(len(pr.Data)))

	for topic, partitionData := range pr.Data {
		encoder.WriteString(topic)
		encoder.WriteInt32(int32(len(partitionData)))

		for partition, data := range partitionData {
			encoder.WriteInt32(partition)
			encoder.Reserve(&LengthSlice{})
			for _, messageAndOffset := range data {
				messageAndOffset.Write(encoder)
			}
			encoder.UpdateReserved()
		}
	}
}

// AddMessage is a convenience method to add a single message to be produced to a topic partition.
func (pr *ProduceRequest) AddMessage(topic string, partition int32, message *Message) {
	if pr.Data == nil {
		pr.Data = make(map[string]map[int32][]*MessageAndOffset)
	}

	if pr.Data[topic] == nil {
		pr.Data[topic] = make(map[int32][]*MessageAndOffset)
	}

	pr.Data[topic][partition] = append(pr.Data[topic][partition], &MessageAndOffset{Message: message})
}

// ProduceResponse contains highest assigned offsets by topic partitions and errors if they occurred.
type ProduceResponse struct {
	Status map[string]map[int32]*ProduceResponseStatus
}

func (pr *ProduceResponse) Read(decoder Decoder) *DecodingError {
	pr.Status = make(map[string]map[int32]*ProduceResponseStatus)

	topicsLength, err := decoder.GetInt32()
	if err != nil {
		return NewDecodingError(err, reasonInvalidProduceTopicsLength)
	}

	for i := int32(0); i < topicsLength; i++ {
		topic, err := decoder.GetString()
		if err != nil {
			return NewDecodingError(err, reasonInvalidProduceTopic)
		}

		blocksForTopic := make(map[int32]*ProduceResponseStatus)
		pr.Status[topic] = blocksForTopic

		partitionsLength, err := decoder.GetInt32()
		if err != nil {
			return NewDecodingError(err, reasonInvalidProducePartitionsLength)
		}

		for j := int32(0); j < partitionsLength; j++ {
			partition, err := decoder.GetInt32()
			if err != nil {
				return NewDecodingError(err, reasonInvalidProducePartition)
			}

			data := new(ProduceResponseStatus)
			errCode, err := decoder.GetInt16()
			if err != nil {
				return NewDecodingError(err, reasonInvalidProduceErrorCode)
			}
			data.Error = BrokerErrors[errCode]

			offset, err := decoder.GetInt64()
			if err != nil {
				return NewDecodingError(err, reasonInvalidProduceOffset)
			}
			data.Offset = offset

			blocksForTopic[partition] = data
		}
	}

	return nil
}

// ProduceResponseStatus contains a highest assigned offset from a ProduceRequest and an error if it occurred.
type ProduceResponseStatus struct {
	Error  error
	Offset int64
}

const (
	reasonInvalidProduceTopicsLength     = "Invalid topics length in ProduceResponse"
	reasonInvalidProduceTopic            = "Invalid topic in ProduceResponse"
	reasonInvalidProducePartitionsLength = "Invalid partitions length in ProduceResponse"
	reasonInvalidProducePartition        = "Invalid partition in ProduceResponse"
	reasonInvalidProduceErrorCode        = "Invalid error code in ProduceResponse"
	reasonInvalidProduceOffset           = "Invalid offset in ProduceResponse"
)
