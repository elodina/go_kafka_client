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

import "fmt"

// TopicMetadataRequest is used to get topics, their partitions, leader brokers for them and where these brokers are located.
type TopicMetadataRequest struct {
	Topics []string
}

// NewMetadataRequest creates a new MetadataRequest to fetch metadata for given topics.
// Passing it an empty slice will request metadata for all topics.
func NewMetadataRequest(topics []string) *TopicMetadataRequest {
	return &TopicMetadataRequest{
		Topics: topics,
	}
}

func (mr *TopicMetadataRequest) Write(encoder Encoder) {
	encoder.WriteInt32(int32(len(mr.Topics)))
	for _, topic := range mr.Topics {
		encoder.WriteString(topic)
	}
}

// Key returns the Kafka API key for TopicMetadataRequest.
func (mr *TopicMetadataRequest) Key() int16 {
	return 3
}

// Version returns the Kafka request version for backwards compatibility.
func (mr *TopicMetadataRequest) Version() int16 {
	return 0
}

// MetadataResponse contains information about brokers in cluster and topics that exist.
type MetadataResponse struct {
	Brokers        []*Broker
	TopicsMetadata []*TopicMetadata
}

func (tmr *MetadataResponse) Read(decoder Decoder) *DecodingError {
	brokersLength, err := decoder.GetInt32()
	if err != nil {
		return NewDecodingError(err, reasonInvalidBrokersLength)
	}

	tmr.Brokers = make([]*Broker, brokersLength)
	for i := int32(0); i < brokersLength; i++ {
		broker := new(Broker)
		err := broker.Read(decoder)
		if err != nil {
			return err
		}
		tmr.Brokers[i] = broker
	}

	metadataLength, err := decoder.GetInt32()
	if err != nil {
		return NewDecodingError(err, reasonInvalidMetadataLength)
	}

	tmr.TopicsMetadata = make([]*TopicMetadata, metadataLength)
	for i := int32(0); i < metadataLength; i++ {
		topicMetadata := new(TopicMetadata)
		err := topicMetadata.Read(decoder)
		if err != nil {
			return err
		}
		tmr.TopicsMetadata[i] = topicMetadata
	}

	return nil
}

// Broker contains information about a Kafka broker in cluster - its ID, host name and port.
type Broker struct {
	ID   int32
	Host string
	Port int32
}

func (n *Broker) String() string {
	return fmt.Sprintf("%s:%d", n.Host, n.Port)
}

func (n *Broker) Read(decoder Decoder) *DecodingError {
	nodeID, err := decoder.GetInt32()
	if err != nil {
		return NewDecodingError(err, reasonInvalidBrokerNodeID)
	}
	n.ID = nodeID

	host, err := decoder.GetString()
	if err != nil {
		return NewDecodingError(err, reasonInvalidBrokerHost)
	}
	n.Host = host

	port, err := decoder.GetInt32()
	if err != nil {
		return NewDecodingError(err, reasonInvalidBrokerPort)
	}
	n.Port = port

	return nil
}

// TopicMetadata contains information about topic - its name, number of partitions, leaders, ISRs and errors if they occur.
type TopicMetadata struct {
	Error              error
	Topic              string
	PartitionsMetadata []*PartitionMetadata
}

func (tm *TopicMetadata) Read(decoder Decoder) *DecodingError {
	errCode, err := decoder.GetInt16()
	if err != nil {
		return NewDecodingError(err, reasonInvalidTopicMetadataErrorCode)
	}
	tm.Error = BrokerErrors[errCode]

	topicName, err := decoder.GetString()
	if err != nil {
		return NewDecodingError(err, reasonInvalidTopicMetadataTopicName)
	}
	tm.Topic = topicName

	metadataLength, err := decoder.GetInt32()
	if err != nil {
		return NewDecodingError(err, reasonInvalidPartitionMetadataLength)
	}

	tm.PartitionsMetadata = make([]*PartitionMetadata, metadataLength)
	for i := int32(0); i < metadataLength; i++ {
		metadata := new(PartitionMetadata)
		err := metadata.Read(decoder)
		if err != nil {
			return err
		}
		tm.PartitionsMetadata[i] = metadata
	}

	return nil
}

// PartitionMetadata contains information about a topic partition - its id, leader, replicas, ISRs and error if it occurred.
type PartitionMetadata struct {
	Error       error
	PartitionID int32
	Leader      int32
	Replicas    []int32
	ISR         []int32
}

func (pm *PartitionMetadata) Read(decoder Decoder) *DecodingError {
	errCode, err := decoder.GetInt16()
	if err != nil {
		return NewDecodingError(err, reasonInvalidPartitionMetadataErrorCode)
	}
	pm.Error = BrokerErrors[errCode]

	partition, err := decoder.GetInt32()
	if err != nil {
		return NewDecodingError(err, reasonInvalidPartitionMetadataPartition)
	}
	pm.PartitionID = partition

	leader, err := decoder.GetInt32()
	if err != nil {
		return NewDecodingError(err, reasonInvalidPartitionMetadataLeader)
	}
	pm.Leader = leader

	replicasLength, err := decoder.GetInt32()
	if err != nil {
		return NewDecodingError(err, reasonInvalidPartitionMetadataReplicasLength)
	}

	pm.Replicas = make([]int32, replicasLength)
	for i := int32(0); i < replicasLength; i++ {
		replica, err := decoder.GetInt32()
		if err != nil {
			return NewDecodingError(err, reasonInvalidPartitionMetadataReplica)
		}
		pm.Replicas[i] = replica
	}

	isrLength, err := decoder.GetInt32()
	if err != nil {
		return NewDecodingError(err, reasonInvalidPartitionMetadataIsrLength)
	}

	pm.ISR = make([]int32, isrLength)
	for i := int32(0); i < isrLength; i++ {
		isr, err := decoder.GetInt32()
		if err != nil {
			return NewDecodingError(err, reasonInvalidPartitionMetadataIsr)
		}
		pm.ISR[i] = isr
	}

	return nil
}

var (
	reasonInvalidBrokersLength                   = "Invalid length for Brokers field"
	reasonInvalidMetadataLength                  = "Invalid length for TopicMetadata field"
	reasonInvalidBrokerNodeID                    = "Invalid broker node id"
	reasonInvalidBrokerHost                      = "Invalid broker host"
	reasonInvalidBrokerPort                      = "Invalid broker port"
	reasonInvalidTopicMetadataErrorCode          = "Invalid topic metadata error code"
	reasonInvalidTopicMetadataTopicName          = "Invalid topic metadata topic name"
	reasonInvalidPartitionMetadataLength         = "Invalid length for Partition Metadata field"
	reasonInvalidPartitionMetadataErrorCode      = "Invalid partition metadata error code"
	reasonInvalidPartitionMetadataPartition      = "Invalid partition in partition metadata"
	reasonInvalidPartitionMetadataLeader         = "Invalid leader in partition metadata"
	reasonInvalidPartitionMetadataReplicasLength = "Invalid length for Replicas field"
	reasonInvalidPartitionMetadataReplica        = "Invalid replica in partition metadata"
	reasonInvalidPartitionMetadataIsrLength      = "Invalid length for Isr field"
	reasonInvalidPartitionMetadataIsr            = "Invalid isr in partition metadata"
)
