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
	"github.com/elodina/siesta"
	"github.com/elodina/siesta-producer"
	"hash/fnv"
	"time"
)

// MirrorMakerConfig defines configuration options for MirrorMaker
type MirrorMakerConfig struct {
	// Whitelist of topics to mirror. Exactly one whitelist or blacklist is allowed.
	Whitelist string

	// Blacklist of topics to mirror. Exactly one whitelist or blacklist is allowed.
	Blacklist string

	// Consumer configurations to consume from a source cluster.
	ConsumerConfigs []string

	// Embedded producer config.
	ProducerConfig string

	// Number of producer instances.
	NumProducers int

	// Number of consumption streams.
	NumStreams int

	// Flag to preserve partition number. E.g. if message was read from partition 5 it'll be written to partition 5. Note that this can affect performance.
	PreservePartitions bool

	// Flag to preserve message order. E.g. message sequence 1, 2, 3, 4, 5 will remain 1, 2, 3, 4, 5 in destination topic. Note that this can affect performance.
	PreserveOrder bool

	// Destination topic prefix. E.g. if message was read from topic "test" and prefix is "dc1_" it'll be written to topic "dc1_test".
	TopicPrefix string

	// Number of messages that are buffered between the consumer and producer.
	ChannelSize int

	// Message keys encoder for producer
	KeyEncoder producer.Serializer

	// Message values encoder for producer
	ValueEncoder producer.Serializer

	// Message keys decoder for consumer
	KeyDecoder Decoder

	// Message values decoder for consumer
	ValueDecoder Decoder
}

// Creates an empty MirrorMakerConfig.
func NewMirrorMakerConfig() *MirrorMakerConfig {
	return &MirrorMakerConfig{
		KeyEncoder:   producer.ByteSerializer,
		ValueEncoder: producer.ByteSerializer,
		KeyDecoder:   &ByteDecoder{},
		ValueDecoder: &ByteDecoder{},
	}
}

// MirrorMaker is a tool to mirror source Kafka cluster into a target (mirror) Kafka cluster.
// It uses a Kafka consumer to consume messages from the source cluster, and re-publishes those messages to the target cluster.
type MirrorMaker struct {
	config          *MirrorMakerConfig
	metricReporter  *KafkaMetricReporter
	consumers       []*Consumer
	producers       []producer.Producer
	messageChannels []chan *Message
	stopped         chan struct{}
}

// Creates a new MirrorMaker using given MirrorMakerConfig.
func NewMirrorMaker(config *MirrorMakerConfig) *MirrorMaker {
	return &MirrorMaker{
		config:  config,
		stopped: make(chan struct{}),
	}
}

// Starts the MirrorMaker. This method is blocking and should probably be run in a separate goroutine.
func (this *MirrorMaker) Start() {
	this.initializeMessageChannels()
	this.startConsumers()
	this.startProducers()
	<-this.stopped
}

// Gracefully stops the MirrorMaker.
func (this *MirrorMaker) Stop() {
	consumerCloseChannels := make([]<-chan bool, 0)
	for _, consumer := range this.consumers {
		consumerCloseChannels = append(consumerCloseChannels, consumer.Close())
	}

	for _, ch := range consumerCloseChannels {
		<-ch
	}

	for _, ch := range this.messageChannels {
		close(ch)
	}

	//TODO maybe drain message channel first?
	for _, producer := range this.producers {
		producer.Close(time.Second)
	}

	Info("", "Sending stopped")
	this.stopped <- struct{}{}
	Info("", "Sent stopped")
}

func (this *MirrorMaker) startConsumers() {
	for _, consumerConfigFile := range this.config.ConsumerConfigs {
		config, err := ConsumerConfigFromFile(consumerConfigFile)
		if err != nil {
			panic(err)
		}
		config.KeyDecoder = this.config.KeyDecoder
		config.ValueDecoder = this.config.ValueDecoder

		zkConfig, err := ZookeeperConfigFromFile(consumerConfigFile)
		if err != nil {
			panic(err)
		}
		config.AutoOffsetReset = SmallestOffset
		config.Coordinator = NewZookeeperCoordinator(zkConfig)
		config.WorkerFailureCallback = func(_ *WorkerManager) FailedDecision {
			return CommitOffsetAndContinue
		}
		config.WorkerFailedAttemptCallback = func(_ *Task, _ WorkerResult) FailedDecision {
			return CommitOffsetAndContinue
		}
		if this.config.PreserveOrder {
			numProducers := this.config.NumProducers
			config.NumWorkers = 1 // NumWorkers must be 1 to guarantee order
			config.Strategy = func(_ *Worker, msg *Message, id TaskId) WorkerResult {
				this.messageChannels[topicPartitionHash(msg)%numProducers] <- msg

				return NewSuccessfulResult(id)
			}
		} else {
			config.Strategy = func(_ *Worker, msg *Message, id TaskId) WorkerResult {
				this.messageChannels[0] <- msg

				return NewSuccessfulResult(id)
			}
		}

		consumer := NewConsumer(config)
		this.consumers = append(this.consumers, consumer)
		if this.config.Whitelist != "" {
			go consumer.StartWildcard(NewWhiteList(this.config.Whitelist), this.config.NumStreams)
		} else if this.config.Blacklist != "" {
			go consumer.StartWildcard(NewBlackList(this.config.Blacklist), this.config.NumStreams)
		} else {
			panic("Consume pattern not specified")
		}
	}
}

func (this *MirrorMaker) initializeMessageChannels() {
	if this.config.PreserveOrder {
		for i := 0; i < this.config.NumProducers; i++ {
			this.messageChannels = append(this.messageChannels, make(chan *Message, this.config.ChannelSize))
		}
	} else {
		this.messageChannels = append(this.messageChannels, make(chan *Message, this.config.ChannelSize))
	}
}

func (this *MirrorMaker) startProducers() {
	for i := 0; i < this.config.NumProducers; i++ {
		conf, err := producer.ProducerConfigFromFile(this.config.ProducerConfig)
		if err != nil {
			panic(err)
		}
		if this.config.PreservePartitions {
			conf.Partitioner = producer.NewManualPartitioner()
		}
		connectorConfig := siesta.NewConnectorConfig()
		connectorConfig.BrokerList = conf.BrokerList
		connector, err := siesta.NewDefaultConnector(connectorConfig)
		if err != nil {
			panic(err)
		}

		producer := producer.NewKafkaProducer(conf, this.config.KeyEncoder, this.config.ValueEncoder, connector)
		this.producers = append(this.producers, producer)
		if this.config.PreserveOrder {
			go this.produceRoutine(producer, i)
		} else {
			go this.produceRoutine(producer, 0)
		}
	}
}

func (this *MirrorMaker) produceRoutine(p producer.Producer, channelIndex int) {
	for msg := range this.messageChannels[channelIndex] {
		p.Send(&producer.ProducerRecord{
			Topic:     this.config.TopicPrefix + msg.Topic,
			Partition: msg.Partition,
			Key:       msg.Key,
			Value:     msg.DecodedValue,
		})
	}
}

func topicPartitionHash(msg *Message) int {
	h := fnv.New32a()
	h.Write([]byte(fmt.Sprintf("%s%d", msg.Topic, msg.Partition)))
	return int(h.Sum32())
}
