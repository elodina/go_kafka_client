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
	"bufio"
	"github.com/Shopify/sarama"
	"net"
	"strings"
	"time"
)

type SyslogMessage struct {
	Message   string
	Timestamp int64
}

// SyslogProducerConfig defines configuration options for SyslogProducer
type SyslogProducerConfig struct {
	// Syslog producer config.
	ProducerConfig *ProducerConfig

	// Number of producer instances.
	NumProducers int

	// Number of messages that are buffered to produce.
	ChannelSize int

	Topic string

	// Receive messages from this TCP address and post them to topic.
	TCPAddr string

	// Receive messages from this UDP address and post them to topic.
	UDPAddr string

	// Kafka Broker List host:port,host:port
	BrokerList string

	// Transformer func(message syslogparser.LogParts, topic string) *sarama.ProducerMessage
	Transformer func(message *SyslogMessage, topic string) *sarama.ProducerMessage
}

// Creates an empty SyslogProducerConfig.
func NewSyslogProducerConfig() *SyslogProducerConfig {
	return &SyslogProducerConfig{
		Transformer: simpleTransformFunc,
	}
}

type SyslogProducer struct {
	config        *SyslogProducerConfig
	incoming      chan *SyslogMessage
	closeChannels []chan bool

	producers []sarama.AsyncProducer
}

func NewSyslogProducer(config *SyslogProducerConfig) *SyslogProducer {
	return &SyslogProducer{
		config:   config,
		incoming: make(chan *SyslogMessage),
	}
}

func (this *SyslogProducer) String() string {
	return "syslog-producer"
}

func (this *SyslogProducer) Start() {
	Trace(this, "Starting...")
	this.startTCPServer()
	this.startUDPServer()
	this.startProducers()
}

func (this *SyslogProducer) Stop() {
	Trace(this, "Stopping..")

	for _, closeChannel := range this.closeChannels {
		closeChannel <- true
	}
	close(this.incoming)

	for _, producer := range this.producers {
		producer.Close()
	}
}

func (this *SyslogProducer) startTCPServer() {
	Trace(this, "Starting TCP server")
	tcpAddr, err := net.ResolveTCPAddr("tcp", this.config.TCPAddr)
	if err != nil {
		panic(err)
	}

	listener, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		panic(err)
	}
	closeChannel := make(chan bool, 1)
	this.closeChannels = append(this.closeChannels, closeChannel)

	go func() {
		for {
			select {
			case <-closeChannel:
				return
			default:
			}
			connection, err := listener.Accept()
			if err != nil {
				return
			}

			this.scan(connection)
		}
	}()
	Infof(this, "Listening for messages at TCP %s", this.config.TCPAddr)
}

func (this *SyslogProducer) startUDPServer() {
	Trace(this, "Starting UDP server")
	udpAddr, err := net.ResolveUDPAddr("udp", this.config.UDPAddr)
	if err != nil {
		panic(err)
	}

	connection, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		panic(err)
	}
	closeChannel := make(chan bool, 1)
	this.closeChannels = append(this.closeChannels, closeChannel)

	go func() {
		for {
			select {
			case <-closeChannel:
				return
			default:
			}

			this.scan(connection)
		}
	}()
	Infof(this, "Listening for messages at UDP %s", this.config.UDPAddr)
}

func (this *SyslogProducer) scan(connection net.Conn) {
	scanner := bufio.NewScanner(connection)
	for scanner.Scan() {
		timestamp := time.Now().UnixNano() / int64(time.Millisecond)
		this.incoming <- &SyslogMessage{scanner.Text(), timestamp}
	}
}

func (this *SyslogProducer) startProducers() {
	brokerList := strings.Split(this.config.BrokerList, ",")
	conf := this.config.ProducerConfig
	config := sarama.NewConfig()
	config.ClientID = conf.Clientid
	config.ChannelBufferSize = conf.SendBufferSize
	switch strings.ToLower(conf.CompressionCodec) {
	case "none":
		config.Producer.Compression = sarama.CompressionNone
	case "gzip":
		config.Producer.Compression = sarama.CompressionGZIP
	case "snappy":
		config.Producer.Compression = sarama.CompressionSnappy
	}
	config.Producer.Flush.Bytes = conf.FlushByteCount
	config.Producer.Flush.Frequency = conf.FlushTimeout
	config.Producer.Flush.Messages = conf.BatchSize
	config.Producer.Flush.MaxMessages = conf.MaxMessagesPerRequest
	config.Producer.MaxMessageBytes = conf.MaxMessageBytes
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.RequiredAcks = sarama.RequiredAcks(conf.Acks)
	config.Producer.Retry.Backoff = conf.RetryBackoff
	config.Producer.Timeout = conf.Timeout

	for i := 0; i < this.config.NumProducers; i++ {
		client, err := sarama.NewClient(brokerList, config)
		if err != nil {
			panic(err)
		}

		Tracef(this, "Starting new producer with config: %#v", config)
		producer, err := sarama.NewAsyncProducerFromClient(client)
		if err != nil {
			panic(err)
		}
		this.producers = append(this.producers, producer)
		go this.produceRoutine(producer)
	}
}

func (this *SyslogProducer) produceRoutine(producer sarama.AsyncProducer) {
	for msg := range this.incoming {
		Tracef(this, "Got message: %s", msg)
		producer.Input() <- this.config.Transformer(msg, this.config.Topic)
	}
}

func simpleTransformFunc(msg *SyslogMessage, topic string) *sarama.ProducerMessage {
	return &sarama.ProducerMessage{Topic: topic, Value: sarama.StringEncoder(msg.Message)}
}
