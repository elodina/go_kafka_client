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

// ConsumerMetadataRequest is used to discover the current offset coordinator to issue its offset commit and fetch requests.
type ConsumerMetadataRequest struct {
	Group string
}

// NewConsumerMetadataRequest creates a new ConsumerMetadataRequest for a given consumer group.
func NewConsumerMetadataRequest(group string) *ConsumerMetadataRequest {
	return &ConsumerMetadataRequest{Group: group}
}

// Key returns the Kafka API key for ConsumerMetadataRequest.
func (cmr *ConsumerMetadataRequest) Key() int16 {
	return 10
}

// Version returns the Kafka request version for backwards compatibility.
func (cmr *ConsumerMetadataRequest) Version() int16 {
	return 0
}

// Write writes the ConsumerMetadataRequest to the given Encoder.
func (cmr *ConsumerMetadataRequest) Write(encoder Encoder) {
	encoder.WriteString(cmr.Group)
}

// ConsumerMetadataResponse contains information about the current offset coordinator and error if it occurred.
type ConsumerMetadataResponse struct {
	Error       error
	Coordinator *Broker
}

func (cmr *ConsumerMetadataResponse) Read(decoder Decoder) *DecodingError {
	errCode, err := decoder.GetInt16()
	if err != nil {
		return NewDecodingError(err, reasonInvalidConsumerMetadataErrorCode)
	}
	cmr.Error = BrokerErrors[errCode]

	cmr.Coordinator = new(Broker)
	coordID, err := decoder.GetInt32()
	if err != nil {
		return NewDecodingError(err, reasonInvalidConsumerMetadataCoordinatorID)
	}
	cmr.Coordinator.ID = coordID

	coordHost, err := decoder.GetString()
	if err != nil {
		return NewDecodingError(err, reasonInvalidConsumerMetadataCoordinatorHost)
	}
	cmr.Coordinator.Host = coordHost

	coordPort, err := decoder.GetInt32()
	if err != nil {
		return NewDecodingError(err, reasonInvalidConsumerMetadataCoordinatorPort)
	}
	cmr.Coordinator.Port = coordPort

	return nil
}

const (
	reasonInvalidConsumerMetadataErrorCode       = "Invalid error code in consumer metadata"
	reasonInvalidConsumerMetadataCoordinatorID   = "Invalid coordinator id in consumer metadata"
	reasonInvalidConsumerMetadataCoordinatorHost = "Invalid coordinator host in consumer metadata"
	reasonInvalidConsumerMetadataCoordinatorPort = "Invalid coordinator port in consumer metadata"
)
