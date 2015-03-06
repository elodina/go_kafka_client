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
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"strings"

	"github.com/Shopify/sarama"
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
}

// Creates an empty MirrorMakerConfig.
func NewMirrorMakerConfig() *MirrorMakerConfig {
	return &MirrorMakerConfig{}
}

// MirrorMaker is a tool to mirror source Kafka cluster into a target (mirror) Kafka cluster.
// It uses a Kafka consumer to consume messages from the source cluster, and re-publishes those messages to the target cluster.
type MirrorMaker struct {
	config          *MirrorMakerConfig
	consumers       []*Consumer
	producers       []*sarama.Producer
	messageChannels []chan *Message
}

// Creates a new MirrorMaker using given MirrorMakerConfig.
func NewMirrorMaker(config *MirrorMakerConfig) *MirrorMaker {
	return &MirrorMaker{
		config: config,
	}
}

// Starts the MirrorMaker. This method is blocking and should probably be run in a separate goroutine.
func (this *MirrorMaker) Start() {
	this.initializeMessageChannels()
	this.startConsumers()
	this.startProducers()
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
		producer.Close()
	}
}

func (this *MirrorMaker) startConsumers() {
	for _, consumerConfigFile := range this.config.ConsumerConfigs {
		config, err := ConsumerConfigFromFile(consumerConfigFile)
		if err != nil {
			panic(err)
		}
		zkConfig, err := ZookeeperConfigFromFile(consumerConfigFile)
		if err != nil {
			panic(err)
		}
		config.NumWorkers = 1
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
		conf, err := ProducerConfigFromFile(this.config.ProducerConfig)
		if err != nil {
			panic(err)
		}
		if err = conf.Validate(); err != nil {
			panic(err)
		}

		config := sarama.NewConfig()
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
		config.Producer.Flush.MaxMessages = conf.BatchSize
		config.Producer.MaxMessageBytes = conf.MaxMessageBytes
		if this.config.PreservePartitions {
			config.Producer.Partitioner = NewIntPartitioner
		} else {
			config.Producer.Partitioner = sarama.NewRandomPartitioner
		}
		config.Producer.RequiredAcks = sarama.RequiredAcks(conf.Acks)
		config.Producer.Retry.Backoff = conf.RetryBackoff
		config.Producer.Timeout = conf.Timeout

		producer, err := sarama.NewProducer(conf.BrokerList, config)
		if err != nil {
			panic(err)
		}
		this.producers = append(this.producers, producer)
		if this.config.PreserveOrder {
			go this.produceRoutine(producer, i)
		} else {
			go this.produceRoutine(producer, 0)
		}
	}
}

func (this *MirrorMaker) produceRoutine(producer *sarama.Producer, channelIndex int) {
	for msg := range this.messageChannels[channelIndex] {
		var key sarama.Encoder
		if !this.config.PreservePartitions {
			key = sarama.ByteEncoder(msg.Key)
		} else {
			key = Int32Encoder(msg.Partition)
		}
		producer.Input() <- &sarama.ProducerMessage{Topic: this.config.TopicPrefix + msg.Topic, Key: key, Value: sarama.ByteEncoder(msg.Value)}
	}
}

func topicPartitionHash(msg *Message) int {
	h := fnv.New32a()
	h.Write([]byte(fmt.Sprintf("%s%d", msg.Topic, msg.Partition)))
	return int(h.Sum32())
}

// IntPartitioner is used when we want to preserve partitions.
// This partitioner should be used ONLY with Int32Encoder as it contains unsafe conversions (for performance reasons mostly).
type IntPartitioner struct{}

// PartitionerConstructor function used by Sarama library.
func NewIntPartitioner() sarama.Partitioner {
	return new(IntPartitioner)
}

// Partition takes the key and partition count and chooses a partition. IntPartitioner should ONLY receive Int32Encoder keys.
// Passing it a Int32Encoder(2) key means it should assign the incoming message partition = 2 (assuming this partition exists, otherwise there's no guarantee which partition will be picked).
func (this *IntPartitioner) Partition(key sarama.Encoder, numPartitions int32) (int32, error) {
	if key == nil {
		panic("IntPartitioner does not work without keys.")
	}
	b, err := key.Encode()
	if err != nil {
		return -1, err
	}

	buf := bytes.NewBuffer(b)
	partition, err := binary.ReadUvarint(buf)
	if err != nil {
		return -1, err
	}

	return int32(partition) % numPartitions, nil
}

// Another Partitioner method used by Sarama.
func (this *IntPartitioner) RequiresConsistency() bool {
	return true
}

// Int32Encoder takes an int32 and is able to transform it into a []byte.
type Int32Encoder int32

// Encodes the current value into a []byte. Should never return an error.
func (this Int32Encoder) Encode() ([]byte, error) {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(this))
	return buf, nil
}

// Length for Int32Encoder is always 4.
func (this Int32Encoder) Length() int {
	return 4
}
