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
	"errors"
	"time"
)

type ProducerConfig struct {
	Clientid              string
	BrokerList            []string
	SendBufferSize        int
	CompressionCodec      string
	FlushByteCount        int
	FlushTimeout          time.Duration
	BatchSize             int
	MaxMessageBytes       int
	MaxMessagesPerRequest int
	Acks                  int
	RetryBackoff          time.Duration
	Timeout               time.Duration

	//Retries            int //TODO ??
}

func DefaultProducerConfig() *ProducerConfig {
	return &ProducerConfig{
		Clientid:        "mirrormaker",
		MaxMessageBytes: 1000000,
		Acks:            1,
		RetryBackoff:    250 * time.Millisecond,
	}
}

// ProducerConfigFromFile is a helper function that loads a producer's configuration information from file.
// The file accepts the following fields:
//  client.id
//  metadata.broker.list
//  send.buffer.size
//  compression.codec
//  flush.byte.count
//  flush.timeout
//  batch.size
//  max.message.bytes
//  max.messages.per.request
//  acks
//  retry.backoff
//  timeout
// The configuration file entries should be constructed in key=value syntax. A # symbol at the beginning
// of a line indicates a comment. Blank lines are ignored.
func ProducerConfigFromFile(filename string) (*ProducerConfig, error) {
	p, err := LoadConfiguration(filename)
	if err != nil {
		return nil, err
	}

	config := DefaultProducerConfig()
	setStringConfig(&config.Clientid, p["client.id"])
	setStringSliceConfig(&config.BrokerList, p["metadata.broker.list"], ",")
	if err := setIntConfig(&config.SendBufferSize, p["send.buffer.size"]); err != nil {
		return nil, err
	}
	setStringConfig(&config.CompressionCodec, p["compression.codec"])
	if err := setIntConfig(&config.FlushByteCount, p["flush.byte.count"]); err != nil {
		return nil, err
	}
	if err := setDurationConfig(&config.FlushTimeout, p["flush.timeout"]); err != nil {
		return nil, err
	}
	if err := setIntConfig(&config.BatchSize, p["batch.size"]); err != nil {
		return nil, err
	}
	if err := setIntConfig(&config.MaxMessageBytes, p["max.message.bytes"]); err != nil {
		return nil, err
	}
	if err := setIntConfig(&config.MaxMessagesPerRequest, p["max.messages.per.request"]); err != nil {
		return nil, err
	}
	if err := setIntConfig(&config.Acks, p["acks"]); err != nil {
		return nil, err
	}
	if err := setDurationConfig(&config.RetryBackoff, p["retry.backoff"]); err != nil {
		return nil, err
	}
	if err := setDurationConfig(&config.Timeout, p["timeout"]); err != nil {
		return nil, err
	}

	return config, nil
}

func (this *ProducerConfig) Validate() error {
	if len(this.BrokerList) == 0 {
		return errors.New("Broker list cannot be empty")
	}

	return nil
}
