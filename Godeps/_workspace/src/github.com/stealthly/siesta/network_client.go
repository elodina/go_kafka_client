package siesta

import "net"

type NetworkClient struct {
	connector               Connector
	metadata                Metadata
	socketSendBuffer        int
	socketReceiveBuffer     int
	clientId                string
	nodeIndexOffset         int
	correlation             int
	metadataFetchInProgress bool
	lastNoNodeAvailableMs   int64
	selector                *Selector
	connections             map[string]*net.TCPConn
	requiredAcks            int
	ackTimeoutMs            int32
}

type NetworkClientConfig struct {
}

func NewNetworkClient(config NetworkClientConfig, connector Connector, producerConfig *ProducerConfig) *NetworkClient {
	client := &NetworkClient{}
	client.connector = connector
	client.requiredAcks = producerConfig.RequiredAcks
	client.ackTimeoutMs = producerConfig.AckTimeoutMs
	selectorConfig := NewSelectorConfig(producerConfig)
	client.selector = NewSelector(selectorConfig)
	client.connections = make(map[string]*net.TCPConn, 0)
	return client
}

func (nc *NetworkClient) send(topic string, partition int32, batch []*ProducerRecord) {
	leader, err := nc.connector.GetLeader(topic, partition)
	if err != nil {
		for _, record := range batch {
			record.metadataChan <- &RecordMetadata{Error: err}
		}
	}

	request := new(ProduceRequest)
	request.RequiredAcks = int16(nc.requiredAcks)
	request.AckTimeoutMs = nc.ackTimeoutMs
	for _, record := range batch {
		request.AddMessage(record.Topic, record.partition, &Message{Key: record.encodedKey, Value: record.encodedValue})
	}
	responseChan := nc.selector.Send(leader, request)

	if nc.requiredAcks > 0 {
		go listenForResponse(topic, partition, batch, responseChan)
	} else {
		// acks = 0 case, just complete all requests
		for _, record := range batch {
			record.metadataChan <- &RecordMetadata{
				Offset:    -1,
				Topic:     topic,
				Partition: partition,
				Error:     ErrNoError,
			}
		}
	}
}

func listenForResponse(topic string, partition int32, batch []*ProducerRecord, responseChan <-chan *rawResponseAndError) {
	response := <-responseChan
	if response.err != nil {
		for _, record := range batch {
			record.metadataChan <- &RecordMetadata{Error: response.err}
		}
	}

	decoder := NewBinaryDecoder(response.bytes)
	produceResponse := new(ProduceResponse)
	decodingErr := produceResponse.Read(decoder)
	if decodingErr != nil {
		for _, record := range batch {
			record.metadataChan <- &RecordMetadata{Error: decodingErr.err}
		}
	}

	status := produceResponse.Status[topic][partition]
	currentOffset := status.Offset
	for _, record := range batch {
		record.metadataChan <- &RecordMetadata{
			Topic:     topic,
			Partition: partition,
			Offset:    currentOffset,
			Error:     status.Error,
		}
		currentOffset++
	}
}

func (nc *NetworkClient) close() {
	nc.selector.Close()
}
