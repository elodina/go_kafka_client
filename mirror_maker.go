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
	"time"
	"fmt"
)

type MirrorMakerConfig struct {
	Whitelist string
	Blacklist string
	ConsumerConfigs []string
	ProducerConfig string
	NumProducers int
	NumStreams int
	PreservePartitions bool
	TopicPrefix string
	ChannelSize int
}

func NewMirrorMakerConfig() *MirrorMakerConfig {
	return &MirrorMakerConfig{}
}

type MirrorMaker struct {
	config *MirrorMakerConfig
	consumers []*Consumer
	producers []*sarama.Producer
	producerRoutineStoppers []chan bool
	messageChannel chan *Message
	timestamp int64
}

func NewMirrorMaker(config *MirrorMakerConfig) *MirrorMaker {
	return &MirrorMaker{
		config: config,
		messageChannel: make(chan *Message, config.ChannelSize),
		timestamp: time.Now().Unix(),
	}
}

func (this *MirrorMaker) Start() {
	this.startConsumers()
	this.startProducers()
}

func (this *MirrorMaker) Stop() {
	consumerCloseChannels := make([]<-chan bool, 0)
	for _, consumer := range this.consumers {
		consumerCloseChannels = append(consumerCloseChannels, consumer.Close())
	}

	for _, ch := range consumerCloseChannels {
		<-ch
	}

	for _, stopper := range this.producerRoutineStoppers {
		stopper <- true
	}

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
		config.Groupid = fmt.Sprintf("mirrormaker-%d", this.timestamp)
		zkConfig, err := ZookeeperConfigFromFile(consumerConfigFile)
		if err != nil {
			panic(err)
		}
		config.NumWorkers = 1
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
		} else {
			go consumer.StartWildcard(NewBlackList(this.config.Blacklist), this.config.NumStreams)
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
			config.Partitioner = sarama.NewHashPartitioner
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
		stopper := make(chan bool)
		this.producerRoutineStoppers = append(this.producerRoutineStoppers, stopper)
		go this.produceRoutine(producer, stopper)
	}
}

func (this *MirrorMaker) produceRoutine(producer *sarama.Producer, stopper chan bool) {
	for {
		select {
			case <-stopper: {
				Info("mirrormaker", "Producer stop triggered")
				return
			}
			case msg := <-this.messageChannel: {
				producer.Input() <- &sarama.MessageToSend{Topic: this.config.TopicPrefix + msg.Topic, Key: sarama.ByteEncoder(msg.Key), Value: sarama.ByteEncoder(msg.Value)}
			}
		}
	}
}
