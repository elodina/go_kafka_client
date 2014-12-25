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
	"github.com/Shopify/sarama"
	"strings"
	"bytes"
	"encoding/binary"
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
	config *MirrorMakerConfig
	consumers []*Consumer
	producers []*sarama.Producer
	messageChannel chan *Message
}

// Creates a new MirrorMaker using given MirrorMakerConfig.
func NewMirrorMaker(config *MirrorMakerConfig) *MirrorMaker {
	return &MirrorMaker{
		config: config,
		messageChannel: make(chan *Message, config.ChannelSize),
	}
}

// Starts the MirrorMaker. This method is blocking and should probably be run in a separate goroutine.
func (this *MirrorMaker) Start() {
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

	close(this.messageChannel)

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
		config.Strategy = func(_ *Worker, msg *Message, id TaskId) WorkerResult {
			this.messageChannel <- msg

			return NewSuccessfulResult(id)
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

func (this *MirrorMaker) startProducers() {
	for i := 0; i < this.config.NumProducers; i++ {
		conf, err := ProducerConfigFromFile(this.config.ProducerConfig)
		if err != nil {
			panic(err)
		}
		if err = conf.Validate(); err != nil {
			panic(err)
		}

		client, err := sarama.NewClient(conf.Clientid, conf.BrokerList, sarama.NewClientConfig())
		if err != nil {
			panic(err)
		}

		config := sarama.NewProducerConfig()
		config.ChannelBufferSize = conf.SendBufferSize
		switch strings.ToLower(conf.CompressionCodec) {
		case "none": config.Compression = sarama.CompressionNone
		case "gzip": config.Compression = sarama.CompressionGZIP
		case "snappy": config.Compression = sarama.CompressionSnappy
		}
		config.FlushByteCount = conf.FlushByteCount
		config.FlushFrequency = conf.FlushTimeout
		config.FlushMsgCount = conf.BatchSize
		config.MaxMessageBytes = conf.MaxMessageBytes
		config.MaxMessagesPerReq = conf.MaxMessagesPerRequest
		if this.config.PreservePartitions {
			config.Partitioner = NewIntPartitioner
		} else {
			config.Partitioner = sarama.NewRandomPartitioner
		}
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

func (this *MirrorMaker) produceRoutine(producer *sarama.Producer) {
	for msg := range this.messageChannel {
		var key sarama.Encoder
		if !this.config.PreservePartitions {
			key = sarama.ByteEncoder(msg.Key)
		} else {
			key = Int32Encoder(msg.Partition)
		}
		producer.Input() <- &sarama.MessageToSend{Topic: this.config.TopicPrefix + msg.Topic, Key: key, Value: sarama.ByteEncoder(msg.Value)}
	}
}

// IntPartitioner is used when we want to preserve partitions.
// This partitioner should be used ONLY with Int32Encoder as it contains unsafe conversions (for performance reasons mostly).
type IntPartitioner struct {}

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
