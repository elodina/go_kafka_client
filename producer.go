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

type Producer interface {
	Errors() <-chan *FailedMessage
	Successes() <-chan *Message
	Input() chan<- *Message
	Close() error
	AsyncClose()
}

type SaramaProducer struct {
	saramaProducer *sarama.Producer
}

func NewSaramaProducer(conf *ProducerConfig) *SaramaProducer {
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

	producer, err := sarama.NewProducer(client, config)
	if err != nil {
		panic(err)
	}
	return &SaramaProducer{
		saramaProducer: producer,
	}
}

func (this *SaramaProducer) Errors() <-chan *FailedMessage {
	errorChannel := make(chan *FailedMessage)
	go func () {
		for saramaError := range this.saramaProducer.Errors() {
			key, err := saramaError.Msg.Key.Encode()
			if err != nil {
				panic(err)
			}
			value, err := saramaError.Msg.Value.Encode()
			if err != nil {
				panic(err)
			}
			msg := &Message{
				Key: key,
				Value: value,
				DecodedKey: key,
				DecodedValue: value,
				Topic: saramaError.Msg.Topic,
				Partition: saramaError.Msg.Partition(),
				Offset: saramaError.Msg.Offset(),
			}
			errorChannel <- &FailedMessage{msg, saramaError.Err}
		}
	}()

	return errorChannel
}

func (this *SaramaProducer) Successes() <-chan *Message {
	successChannel := make(chan *Message)
	go func () {
		for saramaMessage := range this.saramaProducer.Successes() {
			key, err := saramaMessage.Key.Encode()
			if err != nil {
				panic(err)
			}
			value, err := saramaMessage.Value.Encode()
			if err != nil {
				panic(err)
			}
			msg := &Message{
				Key: key,
				Value: value,
				DecodedKey: key,
				DecodedValue: value,
				Topic: saramaMessage.Topic,
				Partition: saramaMessage.Partition(),
				Offset: saramaMessage.Offset(),
			}
			successChannel <- msg
		}
	}()

	return successChannel
}

func (this *SaramaProducer) Input() chan<- *Message {
	messageChannel := make(chan *Message)
	go func () {
		for message := range messageChannel {
			var key sarama.Encoder
			if message.Key != nil {
				key = sarama.ByteEncoder(message.Key)
			} else {
				key = Int32Encoder(message.Partition)
			}
			saramaMessage := &sarama.ProducerMessage {
				Topic: message.Topic,
				Key: key,
				Value: sarama.ByteEncoder(message.Value),
			}
			this.saramaProducer.Input() <- saramaMessage
		}
	}()

	return messageChannel
}

func (this *SaramaProducer) Close() error {
	return this.saramaProducer.Close()
}

func (this *SaramaProducer) AsyncClose() {
	this.saramaProducer.AsyncClose()
}
