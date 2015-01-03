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
	"encoding/json"
	"github.com/Shopify/sarama"
	syslog "github.com/mcuadros/go-syslog"
	"strings"
)

// SyslogProducerConfig defines configuration options for SyslogProducer
type SyslogProducerConfig struct {
	// Syslog producer config.
	ProducerConfig *ProducerConfig

	// Number of producer instances.
	NumProducers int

	// Number of messages that are buffered to produce.
	ChannelSize int

	// Message format. Either RFC5424 or RFC3164
	Format syslog.Format

	Topic string

	// Receive messages from this address and post them to topic.
	TCPAddr string
}

// Creates an empty SyslogProducerConfig.
func NewSyslogProducerConfig() *SyslogProducerConfig {
	return &SyslogProducerConfig{}
}

type SyslogProducer struct {
	config   *SyslogProducerConfig
	server   *syslog.Server
	incoming syslog.LogPartsChannel

	producers []*sarama.Producer
}

func NewSyslogProducer(config *SyslogProducerConfig) *SyslogProducer {
	return &SyslogProducer{
		config: config,
	}
}

func (this *SyslogProducer) String() string {
	return "syslog-producer"
}

func (this *SyslogProducer) Start() {
	Trace(this, "Starting...")
	this.startTCPServer()
	this.startProducers()
}

func (this *SyslogProducer) Stop() {
	Trace(this, "Stopping..")
	close(this.incoming)

	for _, producer := range this.producers {
		producer.Close()
	}

	this.server.Kill()
}

func (this *SyslogProducer) startTCPServer() {
	Trace(this, "Starting TCP server")
	channel := make(syslog.LogPartsChannel)
	handler := syslog.NewChannelHandler(channel)

	server := syslog.NewServer()
	server.SetFormat(this.config.Format)
	server.SetHandler(handler)
	if err := server.ListenTCP(this.config.TCPAddr); err != nil {
		panic(err)
	}
	if err := server.Boot(); err != nil {
		panic(err)
	}

	this.server = server
	this.incoming = channel
	Infof(this, "Listening for messages at TCP %s", this.config.TCPAddr)
}

func (this *SyslogProducer) startProducers() {
	for i := 0; i < this.config.NumProducers; i++ {
		conf := this.config.ProducerConfig
		client, err := sarama.NewClient(conf.Clientid, conf.BrokerList, sarama.NewClientConfig())
		if err != nil {
			panic(err)
		}

		config := sarama.NewProducerConfig()
		config.ChannelBufferSize = conf.SendBufferSize
		switch strings.ToLower(conf.CompressionCodec) {
		case "none":
			config.Compression = sarama.CompressionNone
		case "gzip":
			config.Compression = sarama.CompressionGZIP
		case "snappy":
			config.Compression = sarama.CompressionSnappy
		}
		config.FlushByteCount = conf.FlushByteCount
		config.FlushFrequency = conf.FlushTimeout
		config.FlushMsgCount = conf.BatchSize
		config.MaxMessageBytes = conf.MaxMessageBytes
		config.MaxMessagesPerReq = conf.MaxMessagesPerRequest
		config.Partitioner = sarama.NewRandomPartitioner
		config.RequiredAcks = sarama.RequiredAcks(conf.Acks)
		config.RetryBackoff = conf.RetryBackoff
		config.Timeout = conf.Timeout

		producer, err := sarama.NewProducer(client, config)
		if err != nil {
			panic(err)
		}
		this.producers = append(this.producers, producer)
		go this.produceRoutine(producer)
	}
}

func (this *SyslogProducer) produceRoutine(producer *sarama.Producer) {
	for msg := range this.incoming {
		Tracef(this, "Got message: %s", msg)
		//TODO custom transformations
		b, err := json.Marshal(msg)
		if err != nil {
			Errorf(this, "Failed to marshal %s as JSON", msg)
		}

		producer.Input() <- &sarama.MessageToSend{Topic: this.config.Topic, Value: sarama.ByteEncoder(b)}
	}
}
