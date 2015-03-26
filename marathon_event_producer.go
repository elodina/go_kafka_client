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
	"github.com/stealthly/go-avro"
	"io/ioutil"
	"net/http"
	"strings"
)

type MarathonEventProducerConfig struct {
	// Marathon event producer config.
	ProducerConfig *ProducerConfig

	// Destination topic for all incoming messages.
	Topic string

	// Kafka Broker List host:port,host:port
	BrokerList string

	//HTTP endpoint binding port
	Port int

	// HTTP endpoint url pattern to listen, e.g. "/marathon".
	Pattern string

	// URL to Confluent Schema Registry. This triggers all messages to be sent in Avro format.
	SchemaRegistryUrl string

	// Avro schema to use when producing messages in Avro format.
	AvroSchema avro.Schema

	// Function that generates producer instances
	ProducerConstructor ProducerConstructor
}

// Creates an empty MarathonEventProducerConfig.
func NewMarathonEventProducerConfig() *MarathonEventProducerConfig {
	return &MarathonEventProducerConfig{
		ProducerConstructor: NewSaramaProducer,
	}
}

type MarathonEventProducer struct {
	config   *MarathonEventProducerConfig
	incoming chan interface{}

	producer Producer
}

func NewMarathonEventProducer(config *MarathonEventProducerConfig) *MarathonEventProducer {
	return &MarathonEventProducer{
		config:   config,
		incoming: make(chan interface{}),
	}
}

func (this *MarathonEventProducer) String() string {
	return "marathon-event-producer"
}

func (this *MarathonEventProducer) Start() {
	Trace(this, "Starting...")
	this.startHTTPServer()
	this.startProducer()
}

func (this *MarathonEventProducer) startHTTPServer() {
	if this.config.SchemaRegistryUrl != "" {
		http.HandleFunc(this.config.Pattern, this.avroHandleFunc)
	} else {
		http.HandleFunc(this.config.Pattern, this.plainHandleFunc)
	}

	go http.ListenAndServe(fmt.Sprintf(":%d", this.config.Port), nil)
}

func (this *MarathonEventProducer) startProducer() {
	if this.config.SchemaRegistryUrl != "" {
		this.config.ProducerConfig.KeyEncoder = NewKafkaAvroEncoder(this.config.SchemaRegistryUrl)
		this.config.ProducerConfig.ValueEncoder = this.config.ProducerConfig.KeyEncoder
	}
	this.config.ProducerConfig.BrokerList = strings.Split(this.config.BrokerList, ",")

	this.producer = this.config.ProducerConstructor(this.config.ProducerConfig)
	go this.produceRoutine()
}

func (this *MarathonEventProducer) Stop() {
	Trace(this, "Stopping..")

	close(this.incoming)

	this.producer.Close()
}

func (this *MarathonEventProducer) produceRoutine() {
	for msg := range this.incoming {
		this.producer.Input() <- &ProducerMessage{Topic: this.config.Topic, Value: msg}
	}
}

func (this *MarathonEventProducer) plainHandleFunc(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		panic(err)
	}

	this.incoming <- body
}

func (this *MarathonEventProducer) avroHandleFunc(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		panic(err)
	}

	message := avro.NewGenericRecord(this.config.AvroSchema)
	message.Set("source", r.RemoteAddr)
	message.Set("headers", r.Header)
	message.Set("body", body)

	this.incoming <- message
}
