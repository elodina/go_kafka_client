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
	"fmt"
	"time"

	"github.com/elodina/siesta"
)

type ErrorType int

const (
	ErrorTypeOffsetOutOfRange ErrorType = iota
	ErrorTypeCorruptedResponse
	ErrorTypeOther
)

// LowLevelClient is a low-level Kafka client that manages broker connections, responsible to fetch metadata and is able
// to handle Fetch and Offset requests.
//TODO not sure that's a good name for this interface
type LowLevelClient interface {
	// This will be called right after connecting to ConsumerCoordinator so this client can initialize itself
	// with bootstrap broker list for example. May return an error to signal this client is unable to work with given configuration.
	Initialize() error

	// This will be called each time the fetch request to Kafka should be issued. Topic, partition and offset are self-explanatory.
	// Should return a slice of Messages and an error if a fetch error occurred.
	// Note that for performance reasons it makes sense to keep open broker connections and reuse them on every fetch call.
	Fetch(topic string, partition int32, offset int64) ([]*Message, error)

	// This will be called when call to Fetch returns an error. As every client has different error mapping we ask here explicitly
	// what kind of error was returned.
	GetErrorType(error) ErrorType

	// This will be called to handle OffsetOutOfRange error. OffsetTime will be either "smallest" or "largest".
	// Should return a corresponding offset value and an error if it occurred.
	GetAvailableOffset(topic string, partition int32, offsetTime string) (int64, error)

	// This will be called to gracefully shutdown this client.
	Close()
}

// SiestaClient implements LowLevelClient and OffsetStorage and uses github.com/elodina/siesta as underlying implementation.
type SiestaClient struct {
	config    *ConsumerConfig
	connector siesta.Connector
}

// Creates a new SiestaClient using a given ConsumerConfig.
func NewSiestaClient(config *ConsumerConfig) *SiestaClient {
	return &SiestaClient{
		config: config,
	}
}

// Returns a string representation of this SiestaClient.
func (this *SiestaClient) String() string {
	return "Siesta client"
}

// This will be called right after connecting to ConsumerCoordinator so this client can initialize itself
// with bootstrap broker list for example. May return an error to signal this client is unable to work with given configuration.
func (this *SiestaClient) Initialize() error {
	bootstrapBrokers, err := BootstrapBrokers(this.config.Coordinator)
	if err != nil {
		return err
	}

	connectorConfig := siesta.NewConnectorConfig()
	connectorConfig.BrokerList = bootstrapBrokers
	connectorConfig.ReadTimeout = this.config.SocketTimeout
	connectorConfig.WriteTimeout = this.config.SocketTimeout
	connectorConfig.ConnectTimeout = this.config.SocketTimeout
	connectorConfig.FetchSize = this.config.FetchMessageMaxBytes
	connectorConfig.ClientID = this.config.Clientid

	this.connector, err = siesta.NewDefaultConnector(connectorConfig)
	if err != nil {
		return err
	}

	return nil
}

// This will be called each time the fetch request to Kafka should be issued. Topic, partition and offset are self-explanatory.
// Returns slice of Messages and an error if a fetch error occurred.
func (this *SiestaClient) Fetch(topic string, partition int32, offset int64) ([]*Message, error) {
	Tracef(this, "Fetching %s %d from %d", topic, partition, offset)
	response, err := this.connector.Fetch(topic, partition, offset)
	if err != nil {
		return nil, err
	}

	messages := make([]*Message, 0)

	timestamp := time.Now().UnixNano() / int64(time.Millisecond)
	collector := func(topic string, partition int32, offset int64, key []byte, value []byte) error {
		decodedKey, err := this.config.KeyDecoder.Decode(key)
		if err != nil {
			//TODO: what if we fail to decode the key: fail-fast or fail-safe strategy?
			Error(this, err.Error())
			return err
		}
		decodedValue, err := this.config.ValueDecoder.Decode(value)
		if err != nil {
			//TODO: what if we fail to decode the value: fail-fast or fail-safe strategy?
			Error(this, err.Error())
			return err
		}

		if this.config.Debug {
			decodedKey = []int64{timestamp}
		}

		messages = append(messages, &Message{
			Key:                 key,
			Value:               value,
			DecodedKey:          decodedKey,
			DecodedValue:        decodedValue,
			Topic:               topic,
			Partition:           partition,
			Offset:              offset,
			HighwaterMarkOffset: response.Data[topic][partition].HighwaterMarkOffset,
		})
		return nil
	}

	return messages, response.CollectMessages(collector)
}

// Tells the caller what kind of error it is.
func (this *SiestaClient) GetErrorType(err error) ErrorType {
	switch {
	case err == siesta.ErrOffsetOutOfRange:
		return ErrorTypeOffsetOutOfRange
	case err == siesta.ErrEOF:
		return ErrorTypeCorruptedResponse
	default:
		return ErrorTypeOther
	}
}

// This will be called to handle OffsetOutOfRange error. OffsetTime will be either "smallest" or "largest".
func (this *SiestaClient) GetAvailableOffset(topic string, partition int32, offsetTime string) (int64, error) {
	time := siesta.LatestTime
	if offsetTime == "smallest" {
		time = siesta.EarliestTime
	}
	return this.connector.GetAvailableOffset(topic, partition, time)
}

// Gets the offset for a given group, topic and partition.
// May return an error if fails to retrieve the offset.
func (this *SiestaClient) GetOffset(group string, topic string, partition int32) (int64, error) {
	offset, err := this.connector.GetOffset(group, topic, partition)
	if err == siesta.ErrUnknownTopicOrPartition {
		return -1, nil
	}
	return offset, err
}

// Commits the given offset for a given group, topic and partition.
// May return an error if fails to commit the offset.
func (this *SiestaClient) CommitOffset(group string, topic string, partition int32, offset int64) error {
	return this.connector.CommitOffset(group, topic, partition, offset)
}

// Gracefully shuts down this client.
func (this *SiestaClient) Close() {
	<-this.connector.Close()
}

// BootstrapBrokers queries the ConsumerCoordinator for all known brokers in the cluster to be used later as a bootstrap list for the LowLevelClient.
func BootstrapBrokers(coordinator ConsumerCoordinator) ([]string, error) {
	bootstrapBrokers := make([]string, 0)
	brokers, err := coordinator.GetAllBrokers()
	if err != nil {
		return nil, err
	}
	for _, broker := range brokers {
		bootstrapBrokers = append(bootstrapBrokers, fmt.Sprintf("%s:%d", broker.Host, broker.Port))
	}

	return bootstrapBrokers, nil
}
