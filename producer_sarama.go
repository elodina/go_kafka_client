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

package go_kafka_client

import (
	"github.com/Shopify/sarama"
	"strings"
)

type SaramaProducer struct {
	saramaProducer *sarama.Producer
    input chan *ProducerMessage
    successes chan *ProducerMessage
    errors chan *FailedMessage
	config         *ProducerConfig
}

func NewSaramaProducer(conf *ProducerConfig) Producer {
	if err := conf.Validate(); err != nil {
		panic(err)
	}

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
	config.RequiredAcks = sarama.RequiredAcks(conf.Acks)
	config.RetryBackoff = conf.RetryBackoff
	config.Timeout = conf.Timeout
	config.AckSuccesses = conf.AckSuccesses

	partitionerFactory := &SaramaPartitionerFactory{conf.Partitioner}
	config.Partitioner = partitionerFactory.PartitionerConstructor

	producer, err := sarama.NewProducer(client, config)
	if err != nil {
		panic(err)
	}
	saramaProducer := &SaramaProducer{
		saramaProducer: producer,
		config:         conf,
	}
    saramaProducer.initSuccesses()
    saramaProducer.initErrors()
    saramaProducer.initInput()

    return saramaProducer
}

func (this *SaramaProducer) Errors() <-chan *FailedMessage {
    return this.errors
}

func (this *SaramaProducer) initErrors() {
	this.errors = make(chan *FailedMessage, this.config.SendBufferSize)
	go func() {
		for saramaError := range this.saramaProducer.Errors() {
			key, err := saramaError.Msg.Key.Encode()
			if err != nil {
				panic(err)
			}
			value, err := saramaError.Msg.Value.Encode()
			if err != nil {
				panic(err)
			}
			msg := &ProducerMessage{
				Topic:     saramaError.Msg.Topic,
				Key:       key,
				Value:     value,
				partition: saramaError.Msg.Partition(),
				offset:    saramaError.Msg.Offset(),
			}
			this.errors <- &FailedMessage{msg, saramaError.Err}
		}
	}()
}

func (this *SaramaProducer) Successes() <-chan *ProducerMessage {
    return this.successes
}

func (this *SaramaProducer) initSuccesses() {
	this.successes = make(chan *ProducerMessage, this.config.SendBufferSize)
	go func() {
		for saramaMessage := range this.saramaProducer.Successes() {
			key, err := saramaMessage.Key.Encode()
			if err != nil {
				panic(err)
			}
			value, err := saramaMessage.Value.Encode()
			if err != nil {
				panic(err)
			}
			msg := &ProducerMessage{
				Topic:     saramaMessage.Topic,
				Key:       key,
				Value:     value,
				partition: saramaMessage.Partition(),
				offset:    saramaMessage.Offset(),
			}
			this.successes <- msg
		}
	}()
}

func (this *SaramaProducer) Input() chan<- *ProducerMessage {
    return this.input
}

func (this *SaramaProducer) initInput() {
	this.input = make(chan *ProducerMessage, this.config.SendBufferSize)
	go func() {
		for message := range this.input {
			encodedKey, err := this.getKeyEncoder(message).Encode(message.Key)
			if err != nil {
				panic(err)
			}
			key := sarama.ByteEncoder(encodedKey)

			encodedValue, err := this.getValueEncoder(message).Encode(message.Value)
			if err != nil {
				panic(err)
			}
			value := sarama.ByteEncoder(encodedValue)
			saramaMessage := &sarama.ProducerMessage{
				Topic: message.Topic,
				Key:   key,
				Value: value,
				Metadata: message.Metadata,
			}
			this.saramaProducer.Input() <- saramaMessage
		}
	}()
}

func (this *SaramaProducer) Close() error {
	return this.saramaProducer.Close()
}

func (this *SaramaProducer) AsyncClose() {
	this.saramaProducer.AsyncClose()
}

func (this *SaramaProducer) getKeyEncoder(message *ProducerMessage) Encoder {
	if message.KeyEncoder == nil {
		return this.config.KeyEncoder
	} else {
		return message.KeyEncoder
	}
}

func (this *SaramaProducer) getValueEncoder(message *ProducerMessage) Encoder {
	if message.ValueEncoder == nil {
		return this.config.ValueEncoder
	} else {
		return message.ValueEncoder
	}
}

type SaramaPartitionerFactory struct {
	partitioner PartitionerConstructor
}

func (this *SaramaPartitionerFactory) PartitionerConstructor() sarama.Partitioner {
	return &SaramaPartitioner{
		partitioner: this.partitioner(),
	}
}

type SaramaPartitioner struct {
	partitioner Partitioner
}

func (this *SaramaPartitioner) Partition(message *sarama.ProducerMessage, numPartitions int32) (int32, error) {
	mmMessage := &ProducerMessage {
		Topic: message.Topic,
		Key:   message.Key,
		Value: message.Value,
		Metadata: message.Metadata,
	}
	return this.partitioner.Partition(mmMessage, numPartitions)
}

func (this *SaramaPartitioner) RequiresConsistency() bool {
	return this.partitioner.RequiresConsistency()
}
