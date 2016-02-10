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

// Siesta is a low-level Apache Kafka client in Go.

package siesta

import (
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

// InvalidOffset is a constant that is used to denote an invalid or uninitialized offset.
const InvalidOffset int64 = -1

// Connector is an interface that should provide ways to clearly interact with Kafka cluster and hide all broker management stuff from user.
type Connector interface {
	// GetTopicMetadata is primarily used to discover leaders for given topics and how many partitions these topics have.
	// Passing it an empty topic list will retrieve metadata for all topics in a cluster.
	GetTopicMetadata(topics []string) (*MetadataResponse, error)

	// GetAvailableOffset issues an offset request to a specified topic and partition with a given offset time.
	// More on offset time here - https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetRequest
	GetAvailableOffset(topic string, partition int32, offsetTime int64) (int64, error)

	// Fetch issues a single fetch request to a broker responsible for a given topic and partition and returns a FetchResponse that contains messages starting from a given offset.
	Fetch(topic string, partition int32, offset int64) (*FetchResponse, error)

	// GetOffset gets the offset for a given group, topic and partition from Kafka. A part of new offset management API.
	GetOffset(group string, topic string, partition int32) (int64, error)

	// CommitOffset commits the offset for a given group, topic and partition to Kafka. A part of new offset management API.
	CommitOffset(group string, topic string, partition int32, offset int64) error

	GetLeader(topic string, partition int32) (BrokerLink, error)

	// Tells the Connector to close all existing connections and stop.
	// This method is NOT blocking but returns a channel which will get a single value once the closing is finished.
	Close() <-chan bool
}

// ConnectorConfig is used to pass multiple configuration values for a Connector
type ConnectorConfig struct {
	// BrokerList is a bootstrap list to discover other brokers in a cluster. At least one broker is required.
	BrokerList []string

	// ReadTimeout is a timeout to read the response from a TCP socket.
	ReadTimeout time.Duration

	// WriteTimeout is a timeout to write the request to a TCP socket.
	WriteTimeout time.Duration

	// ConnectTimeout is a timeout to connect to a TCP socket.
	ConnectTimeout time.Duration

	// Sets whether the connection should be kept alive.
	KeepAlive bool

	// A keep alive period for a TCP connection.
	KeepAliveTimeout time.Duration

	// Maximum number of open connections for a connector.
	MaxConnections int

	// Maximum number of open connections for a single broker for a connector.
	MaxConnectionsPerBroker int

	// Maximum fetch size in bytes which will be used in all Consume() calls.
	FetchSize int32

	// The minimum amount of data the server should return for a fetch request. If insufficient data is available the request will block
	FetchMinBytes int32

	// The maximum amount of time the server will block before answering the fetch request if there isn't sufficient data to immediately satisfy FetchMinBytes
	FetchMaxWaitTime int32

	// Number of retries to get topic metadata.
	MetadataRetries int

	// Backoff value between topic metadata requests.
	MetadataBackoff time.Duration

	// Number of retries to commit an offset.
	CommitOffsetRetries int

	// Backoff value between commit offset requests.
	CommitOffsetBackoff time.Duration

	// Number of retries to get consumer metadata.
	ConsumerMetadataRetries int

	// Backoff value between consumer metadata requests.
	ConsumerMetadataBackoff time.Duration

	// ClientID that will be used by a connector to identify client requests by broker.
	ClientID string
}

// NewConnectorConfig returns a new ConnectorConfig with sane defaults.
func NewConnectorConfig() *ConnectorConfig {
	return &ConnectorConfig{
		ReadTimeout:             5 * time.Second,
		WriteTimeout:            5 * time.Second,
		ConnectTimeout:          5 * time.Second,
		KeepAlive:               true,
		KeepAliveTimeout:        1 * time.Minute,
		MaxConnections:          5,
		MaxConnectionsPerBroker: 5,
		FetchMinBytes:           1,
		FetchSize:               1024000,
		FetchMaxWaitTime:        1000,
		MetadataRetries:         5,
		MetadataBackoff:         200 * time.Millisecond,
		CommitOffsetRetries:     5,
		CommitOffsetBackoff:     200 * time.Millisecond,
		ConsumerMetadataRetries: 15,
		ConsumerMetadataBackoff: 500 * time.Millisecond,
		ClientID:                "siesta",
	}
}

// Validate validates this ConnectorConfig. Returns a corresponding error if the ConnectorConfig is invalid and nil otherwise.
func (cc *ConnectorConfig) Validate() error {
	if cc == nil {
		return errors.New("Please provide a ConnectorConfig.")
	}

	if len(cc.BrokerList) == 0 {
		return errors.New("BrokerList must have at least one broker.")
	}

	if cc.ReadTimeout < time.Millisecond {
		return errors.New("ReadTimeout must be at least 1ms.")
	}

	if cc.WriteTimeout < time.Millisecond {
		return errors.New("WriteTimeout must be at least 1ms.")
	}

	if cc.ConnectTimeout < time.Millisecond {
		return errors.New("ConnectTimeout must be at least 1ms.")
	}

	if cc.KeepAliveTimeout < time.Millisecond {
		return errors.New("KeepAliveTimeout must be at least 1ms.")
	}

	if cc.MaxConnections < 1 {
		return errors.New("MaxConnections cannot be less than 1.")
	}

	if cc.MaxConnectionsPerBroker < 1 {
		return errors.New("MaxConnectionsPerBroker cannot be less than 1.")
	}

	if cc.FetchSize < 1 {
		return errors.New("FetchSize cannot be less than 1.")
	}

	if cc.MetadataRetries < 0 {
		return errors.New("MetadataRetries cannot be less than 0.")
	}

	if cc.MetadataBackoff < time.Millisecond {
		return errors.New("MetadataBackoff must be at least 1ms.")
	}

	if cc.CommitOffsetRetries < 0 {
		return errors.New("CommitOffsetRetries cannot be less than 0.")
	}

	if cc.CommitOffsetBackoff < time.Millisecond {
		return errors.New("CommitOffsetBackoff must be at least 1ms.")
	}

	if cc.ConsumerMetadataRetries < 0 {
		return errors.New("ConsumerMetadataRetries cannot be less than 0.")
	}

	if cc.ConsumerMetadataBackoff < time.Millisecond {
		return errors.New("ConsumerMetadataBackoff must be at least 1ms.")
	}

	if cc.ClientID == "" {
		return errors.New("ClientId cannot be empty.")
	}

	return nil
}

// DefaultConnector is a default (and only one for now) Connector implementation for Siesta library.
type DefaultConnector struct {
	config         ConnectorConfig
	leaders        map[string]map[int32]*brokerLink
	links          []*brokerLink
	bootstrapLinks []*brokerLink
	lock           sync.Mutex

	//offset coordination part
	offsetCoordinators map[string]int32
}

// NewDefaultConnector creates a new DefaultConnector with a given ConnectorConfig. May return an error if the passed config is invalid.
func NewDefaultConnector(config *ConnectorConfig) (*DefaultConnector, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	connector := &DefaultConnector{
		config:             *config,
		leaders:            make(map[string]map[int32]*brokerLink),
		offsetCoordinators: make(map[string]int32),
	}

	return connector, nil
}

// Returns a string representation of this DefaultConnector.
func (dc *DefaultConnector) String() string {
	return "Default Connector"
}

// GetTopicMetadata is primarily used to discover leaders for given topics and how many partitions these topics have.
// Passing it an empty topic list will retrieve metadata for all topics in a cluster.
func (dc *DefaultConnector) GetTopicMetadata(topics []string) (*MetadataResponse, error) {
	for i := 0; i <= dc.config.MetadataRetries; i++ {
		if metadata, err := dc.getMetadata(topics); err == nil {
			return metadata, nil
		}

		Debugf(dc, "GetTopicMetadata for %s failed after %d try", topics, i)
		time.Sleep(dc.config.MetadataBackoff)
	}

	return nil, fmt.Errorf("Could not get topic metadata for %s after %d retries", topics, dc.config.MetadataRetries)
}

// GetAvailableOffset issues an offset request to a specified topic and partition with a given offset time.
func (dc *DefaultConnector) GetAvailableOffset(topic string, partition int32, offsetTime int64) (int64, error) {
	request := new(OffsetRequest)
	request.AddPartitionOffsetRequestInfo(topic, partition, offsetTime, 1)
	response, err := dc.sendToAllAndReturnFirstSuccessful(request, dc.offsetValidator)
	if response != nil {
		return response.(*OffsetResponse).PartitionErrorAndOffsets[topic][partition].Offsets[0], err
	}

	return -1, err
}

// Fetch issues a single fetch request to a broker responsible for a given topic and partition and returns a FetchResponse that contains messages starting from a given offset.
func (dc *DefaultConnector) Fetch(topic string, partition int32, offset int64) (*FetchResponse, error) {
	response, err := dc.tryFetch(topic, partition, offset)
	if err != nil {
		return response, err
	}

	if response.Error(topic, partition) == ErrNotLeaderForPartition {
		Infof(dc, "Sent a fetch reqest to a non-leader broker. Refleshing metadata for topic %s and retrying the request", topic)
		dc.refreshMetadata([]string{topic})
		response, err = dc.tryFetch(topic, partition, offset)
	}

	return response, err
}

func (dc *DefaultConnector) tryFetch(topic string, partition int32, offset int64) (*FetchResponse, error) {
	link := dc.getLeader(topic, partition)
	if link == nil {
		leader, err := dc.tryGetLeader(topic, partition, dc.config.MetadataRetries)
		if err != nil {
			return nil, err
		}
		link = leader
	}

	request := new(FetchRequest)
	request.MinBytes = dc.config.FetchMinBytes
	request.MaxWait = dc.config.FetchMaxWaitTime
	request.AddFetch(topic, partition, offset, dc.config.FetchSize)
	bytes, err := dc.syncSendAndReceive(link, request)
	if err != nil {
		dc.removeLeader(topic, partition)
		return nil, err
	}

	decoder := NewBinaryDecoder(bytes)
	response := new(FetchResponse)
	decodingErr := response.Read(decoder)
	if decodingErr != nil {
		dc.removeLeader(topic, partition)
		Errorf(dc, "Could not decode a FetchResponse. Reason: %s", decodingErr.Reason())
		return nil, decodingErr.Error()
	}

	return response, nil
}

// GetOffset gets the offset for a given group, topic and partition from Kafka. A part of new offset management API.
func (dc *DefaultConnector) GetOffset(group string, topic string, partition int32) (int64, error) {
	coordinator, err := dc.getOffsetCoordinator(group)
	if err != nil {
		return InvalidOffset, err
	}

	request := NewOffsetFetchRequest(group)
	request.AddOffset(topic, partition)
	bytes, err := dc.syncSendAndReceive(coordinator, request)
	if err != nil {
		return InvalidOffset, err
	}
	response := new(OffsetFetchResponse)
	decodingErr := dc.decode(bytes, response)
	if decodingErr != nil {
		Errorf(dc, "Could not decode an OffsetFetchResponse. Reason: %s", decodingErr.Reason())
		return InvalidOffset, decodingErr.Error()
	}

	topicOffsets, exist := response.Offsets[topic]
	if !exist {
		return InvalidOffset, fmt.Errorf("OffsetFetchResponse does not contain information about requested topic")
	}

	if offset, exists := topicOffsets[partition]; !exists {
		return InvalidOffset, fmt.Errorf("OffsetFetchResponse does not contain information about requested partition")
	} else if offset.Error != ErrNoError {
		return InvalidOffset, offset.Error
	} else {
		return offset.Offset, nil
	}
}

// CommitOffset commits the offset for a given group, topic and partition to Kafka. A part of new offset management API.
func (dc *DefaultConnector) CommitOffset(group string, topic string, partition int32, offset int64) error {
	for i := 0; i <= dc.config.CommitOffsetRetries; i++ {
		if err := dc.tryCommitOffset(group, topic, partition, offset); err == nil {
			return nil
		}

		Debugf(dc, "Failed to commit offset %d for group %s, topic %s, partition %d after %d try", offset, group, topic, partition, i)
		time.Sleep(dc.config.CommitOffsetBackoff)
	}

	return fmt.Errorf("Could not get commit offset %d for group %s, topic %s, partition %d after %d retries", offset, group, topic, partition, dc.config.CommitOffsetRetries)
}

func (dc *DefaultConnector) GetLeader(topic string, partition int32) (BrokerLink, error) {
	link := dc.getLeader(topic, partition)
	if link == nil {
		link, err := dc.tryGetLeader(topic, partition, dc.config.MetadataRetries)
		if err != nil {
			return nil, err
		}
		return link, nil
	}

	return link, nil
}

// Close tells the Connector to close all existing connections and stop.
// This method is NOT blocking but returns a channel which will get a single value once the closing is finished.
func (dc *DefaultConnector) Close() <-chan bool {
	closed := make(chan bool)
	go func() {
		dc.closeBrokerLinks()
		for _, link := range dc.bootstrapLinks {
			link.stop <- true
		}
		dc.bootstrapLinks = nil
		dc.links = nil
		closed <- true
	}()

	return closed
}

func (dc *DefaultConnector) closeBrokerLinks() {
	for _, link := range dc.links {
		link.stop <- true
	}
}

func (dc *DefaultConnector) refreshMetadata(topics []string) {
	if len(dc.bootstrapLinks) == 0 {
		for i := 0; i < len(dc.config.BrokerList); i++ {
			broker := dc.config.BrokerList[i]
			hostPort := strings.Split(broker, ":")
			if len(hostPort) != 2 {
				panic(fmt.Sprintf("incorrect broker connection string: %s", broker))
			}

			port, err := strconv.Atoi(hostPort[1])
			if err != nil {
				panic(fmt.Sprintf("incorrect port in broker connection string: %s", broker))
			}

			dc.bootstrapLinks = append(dc.bootstrapLinks, NewBrokerLink(&Broker{ID: -1, Host: hostPort[0], Port: int32(port)},
				dc.config.KeepAlive,
				dc.config.KeepAliveTimeout,
				dc.config.MaxConnectionsPerBroker))
		}
	}

	response, err := dc.sendToAllLinks(dc.links, NewMetadataRequest(topics), dc.topicMetadataValidator(topics))
	if err != nil {
		Warnf(dc, "Could not get topic metadata from all known brokers, trying bootstrap brokers...")
		if response, err = dc.sendToAllLinks(dc.bootstrapLinks, NewMetadataRequest(topics), dc.topicMetadataValidator(topics)); err != nil {
			Errorf(dc, "Could not get topic metadata from all known brokers")
			return
		}
	}
	dc.refreshLeaders(response.(*MetadataResponse))
}

func (dc *DefaultConnector) refreshLeaders(response *MetadataResponse) {
	brokers := make(map[int32]*brokerLink)
	for _, broker := range response.Brokers {
		brokers[broker.ID] = NewBrokerLink(broker, dc.config.KeepAlive, dc.config.KeepAliveTimeout, dc.config.MaxConnectionsPerBroker)
	}

	if len(brokers) != 0 && len(response.TopicsMetadata) != 0 {
		dc.closeBrokerLinks()
		dc.links = make([]*brokerLink, 0)
	}

	for _, metadata := range response.TopicsMetadata {
		for _, partitionMetadata := range metadata.PartitionsMetadata {
			if leader, exists := brokers[partitionMetadata.Leader]; exists {
				dc.putLeader(metadata.Topic, partitionMetadata.PartitionID, leader)
			} else {
				Warnf(dc, "Topic Metadata response has no leader present for topic %s, parition %d", metadata.Topic, partitionMetadata.PartitionID)
				//TODO: warn about incomplete broker list
			}
		}
	}
}

func (dc *DefaultConnector) getMetadata(topics []string) (*MetadataResponse, error) {
	response, err := dc.sendToAllAndReturnFirstSuccessful(NewMetadataRequest(topics), dc.topicMetadataValidator(topics))
	if response != nil {
		return response.(*MetadataResponse), err
	}

	return nil, err
}

func (dc *DefaultConnector) tryGetLeader(topic string, partition int32, retries int) (*brokerLink, error) {
	for i := 0; i <= retries; i++ {
		dc.refreshMetadata([]string{topic})
		if link := dc.getLeader(topic, partition); link != nil {
			return link, nil
		}
		time.Sleep(dc.config.MetadataBackoff)
	}

	return nil, fmt.Errorf("Could not get leader for %s:%d after %d retries", topic, partition, retries)
}

func (dc *DefaultConnector) getLeader(topic string, partition int32) *brokerLink {
	leadersForTopic, exists := dc.leaders[topic]
	if !exists {
		return nil
	}

	return leadersForTopic[partition]
}

func (dc *DefaultConnector) putLeader(topic string, partition int32, leader *brokerLink) {
	Tracef(dc, "putLeader for topic %s, partition %d - %s", topic, partition, leader.broker)
	dc.lock.Lock()
	defer dc.lock.Unlock()

	if _, exists := dc.leaders[topic]; !exists {
		dc.leaders[topic] = make(map[int32]*brokerLink)
	}

	exists := false
	for _, link := range dc.links {
		if *link.broker == *leader.broker {
			exists = true
			break
		}
	}

	if !exists {
		dc.links = append(dc.links, leader)
	}

	dc.leaders[topic][partition] = leader
}

func (dc *DefaultConnector) removeLeader(topic string, partition int32) {
	dc.lock.Lock()
	defer dc.lock.Unlock()

	if leadersForTopic, exists := dc.leaders[topic]; exists {
		delete(leadersForTopic, partition)
	}
}

func (dc *DefaultConnector) refreshOffsetCoordinator(group string) error {
	for i := 0; i <= dc.config.ConsumerMetadataRetries; i++ {
		if err := dc.tryRefreshOffsetCoordinator(group); err == nil {
			return nil
		}

		Debugf(dc, "Failed to get consumer coordinator for group %s after %d try", group, i)
		time.Sleep(dc.config.ConsumerMetadataBackoff)
	}

	return fmt.Errorf("Could not get consumer coordinator for group %s after %d retries", group, dc.config.ConsumerMetadataRetries)
}

func (dc *DefaultConnector) tryRefreshOffsetCoordinator(group string) error {
	request := NewConsumerMetadataRequest(group)

	response, err := dc.sendToAllAndReturnFirstSuccessful(request, dc.consumerMetadataValidator)
	if err != nil {
		Infof(dc, "Could not get consumer metadata from all known brokers")
		return err
	}
	dc.offsetCoordinators[group] = response.(*ConsumerMetadataResponse).Coordinator.ID

	return nil
}

func (dc *DefaultConnector) getOffsetCoordinator(group string) (*brokerLink, error) {
	coordinatorID, exists := dc.offsetCoordinators[group]
	if !exists {
		err := dc.refreshOffsetCoordinator(group)
		if err != nil {
			return nil, err
		}
		coordinatorID = dc.offsetCoordinators[group]
	}

	Debugf(dc, "Offset coordinator for group %s: %d", group, coordinatorID)

	var brokerLink *brokerLink
	for _, link := range dc.links {
		if link.broker.ID == coordinatorID {
			brokerLink = link
			break
		}
	}

	if brokerLink == nil {
		return nil, fmt.Errorf("Could not find broker with node id %d", coordinatorID)
	}

	return brokerLink, nil
}

func (dc *DefaultConnector) tryCommitOffset(group string, topic string, partition int32, offset int64) error {
	coordinator, err := dc.getOffsetCoordinator(group)
	if err != nil {
		return err
	}

	request := NewOffsetCommitRequest(group)
	request.AddOffset(topic, partition, offset, time.Now().Unix(), "")

	bytes, err := dc.syncSendAndReceive(coordinator, request)
	if err != nil {
		return err
	}

	response := new(OffsetCommitResponse)
	decodingErr := dc.decode(bytes, response)
	if decodingErr != nil {
		Errorf(dc, "Could not decode an OffsetCommitResponse. Reason: %s", decodingErr.Reason())
		return decodingErr.Error()
	}

	topicErrors, exist := response.CommitStatus[topic]
	if !exist {
		return fmt.Errorf("OffsetCommitResponse does not contain information about requested topic")
	}

	if partitionError, exist := topicErrors[partition]; !exist {
		return fmt.Errorf("OffsetCommitResponse does not contain information about requested partition")
	} else if partitionError != ErrNoError {
		return partitionError
	}

	return nil
}

func (dc *DefaultConnector) decode(bytes []byte, response Response) *DecodingError {
	decoder := NewBinaryDecoder(bytes)
	decodingErr := response.Read(decoder)
	if decodingErr != nil {
		Errorf(dc, "Could not decode a response. Reason: %s", decodingErr.Reason())
		return decodingErr
	}

	return nil
}

func (dc *DefaultConnector) sendToAllAndReturnFirstSuccessful(request Request, check func([]byte) Response) (Response, error) {
	if len(dc.links) == 0 {
		dc.refreshMetadata(nil)
	}

	response, err := dc.sendToAllLinks(dc.links, request, check)
	if err != nil {
		response, err = dc.sendToAllLinks(dc.bootstrapLinks, request, check)
	}

	return response, err
}

func (dc *DefaultConnector) sendToAllLinks(links []*brokerLink, request Request, check func([]byte) Response) (Response, error) {
	if len(links) == 0 {
		return nil, errors.New("Empty broker list")
	}

	responses := make(chan *rawResponseAndError, len(links))
	for i := 0; i < len(links); i++ {
		link := links[i]
		go func() {
			bytes, err := dc.syncSendAndReceive(link, request)
			responses <- &rawResponseAndError{bytes, link, err}
		}()
	}

	var response *rawResponseAndError
	for i := 0; i < len(links); i++ {
		response = <-responses
		if response.err == nil {
			if checkResult := check(response.bytes); checkResult != nil {
				return checkResult, nil
			}

			response.err = errors.New("Check result did not pass")
		}

		//		Infof(dc, "Could not process request with broker %s:%d", response.link.broker.Host, response.link.broker.Port)
	}

	return nil, response.err
}

func (dc *DefaultConnector) syncSendAndReceive(link *brokerLink, request Request) ([]byte, error) {
	id, conn, err := link.GetConnection()
	if err != nil {
		link.Failed()
		return nil, err
	}

	if err := dc.send(id, conn, request); err != nil {
		link.Failed()
		return nil, err
	}

	bytes, err := dc.receive(conn)
	if err != nil {
		link.Failed()
		return nil, err
	}

	link.Succeeded()
	link.connectionPool.Return(conn)
	return bytes, err
}

func (dc *DefaultConnector) send(correlationID int32, conn *net.TCPConn, request Request) error {
	writer := NewRequestHeader(correlationID, dc.config.ClientID, request)
	bytes := make([]byte, writer.Size())
	encoder := NewBinaryEncoder(bytes)
	writer.Write(encoder)

	conn.SetWriteDeadline(time.Now().Add(dc.config.WriteTimeout))
	_, err := conn.Write(bytes)
	return err
}

func (dc *DefaultConnector) receive(conn *net.TCPConn) ([]byte, error) {
	conn.SetReadDeadline(time.Now().Add(dc.config.ReadTimeout))
	header := make([]byte, 8)
	_, err := io.ReadFull(conn, header)
	if err != nil {
		return nil, err
	}

	decoder := NewBinaryDecoder(header)
	length, err := decoder.GetInt32()
	if err != nil {
		return nil, err
	}
	response := make([]byte, length-4)
	_, err = io.ReadFull(conn, response)
	if err != nil {
		return nil, err
	}

	return response, nil
}

func (dc *DefaultConnector) topicMetadataValidator(topics []string) func(bytes []byte) Response {
	return func(bytes []byte) Response {
		response := new(MetadataResponse)
		err := dc.decode(bytes, response)
		if err != nil {
			return nil
		}

		if len(topics) > 0 {
			for _, topic := range topics {
				var topicMetadata *TopicMetadata
				for _, topicMetadata = range response.TopicsMetadata {
					if topicMetadata.Topic == topic {
						break
					}
				}

				if topicMetadata.Error != ErrNoError {
					Infof(dc, "Topic metadata err: %s", topicMetadata.Error)
					return nil
				}

				for _, partitionMetadata := range topicMetadata.PartitionsMetadata {
					if partitionMetadata.Error != ErrNoError && partitionMetadata.Error != ErrReplicaNotAvailable {
						Infof(dc, "Partition metadata err: %s", partitionMetadata.Error)
						return nil
					}
				}
			}
		}

		return response
	}
}

func (dc *DefaultConnector) consumerMetadataValidator(bytes []byte) Response {
	response := new(ConsumerMetadataResponse)
	err := dc.decode(bytes, response)
	if err != nil || response.Error != ErrNoError {
		return nil
	}

	return response
}

func (dc *DefaultConnector) offsetValidator(bytes []byte) Response {
	response := new(OffsetResponse)
	err := dc.decode(bytes, response)
	if err != nil {
		return nil
	}
	for _, offsets := range response.PartitionErrorAndOffsets {
		for _, offset := range offsets {
			if offset.Error != ErrNoError {
				return nil
			}
		}
	}

	return response
}

type BrokerLink interface {
	Failed()
	Succeeded()
	GetConnection() (int32, *net.TCPConn, error)
	ReturnConnection(*net.TCPConn)
}

type brokerLink struct {
	sync.RWMutex
	broker                    *Broker
	connectionPool            *connectionPool
	lastConnectTime           time.Time
	lastSuccessfulConnectTime time.Time
	failedAttempts            int
	correlationIds            chan int32
	stop                      chan bool
}

func NewBrokerLink(broker *Broker, keepAlive bool, keepAliveTimeout time.Duration, maxConnectionsPerBroker int) *brokerLink {
	brokerConnect := fmt.Sprintf("%s:%d", broker.Host, broker.Port)
	correlationIds := make(chan int32)
	stop := make(chan bool)

	go correlationIDGenerator(correlationIds, stop)

	return &brokerLink{
		broker:         broker,
		connectionPool: newConnectionPool(brokerConnect, maxConnectionsPerBroker, keepAlive, keepAliveTimeout),
		correlationIds: correlationIds,
		stop:           stop,
	}
}

func (bl *brokerLink) Failed() {
	bl.Lock()
	defer bl.Unlock()
	bl.lastConnectTime = time.Now()
	bl.failedAttempts++
}

func (bl *brokerLink) Succeeded() {
	bl.Lock()
	defer bl.Unlock()
	timestamp := time.Now()
	bl.lastConnectTime = timestamp
	bl.lastSuccessfulConnectTime = timestamp
}

func (bl *brokerLink) ReturnConnection(conn *net.TCPConn) {
	bl.connectionPool.Return(conn)
}

func (bl *brokerLink) GetConnection() (int32, *net.TCPConn, error) {
	correlationID := <-bl.correlationIds
	conn, err := bl.connectionPool.Borrow()
	return correlationID, conn, err
}

func correlationIDGenerator(out chan int32, stop chan bool) {
	var correlationID int32
	for {
		select {
		case out <- correlationID:
			correlationID++
		case <-stop:
			return
		}
	}
}

type rawResponseAndError struct {
	bytes []byte
	link  BrokerLink
	err   error
}
