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
	"bytes"
	"encoding/binary"
	"errors"
	hashing "hash"
	"hash/fnv"
	"math/rand"
	"time"
)

type Producer interface {
	Errors() <-chan *FailedMessage
	Successes() <-chan *ProducerMessage
	Input() chan<- *ProducerMessage
	Close() error
	AsyncClose()
}

type ProducerConstructor func(config *ProducerConfig) Producer

type ProducerMessage struct {
	Topic        string
	Key          interface{}
	Value        interface{}
	KeyEncoder   Encoder
	ValueEncoder Encoder

	offset    int64
	partition int32
}

type Partitioner interface {
	Partition(key []byte, numPartitions int32) (int32, error)
	RequiresConsistency() bool
}

type PartitionerConstructor func() Partitioner

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
	Partitioner           PartitionerConstructor
	KeyEncoder            Encoder
	ValueEncoder          Encoder
	AckSuccesses          bool

	//Retries            int //TODO ??
}

func DefaultProducerConfig() *ProducerConfig {
	return &ProducerConfig{
		Clientid:              "mirrormaker",
		MaxMessageBytes:       1000000,
		Acks:                  1,
		RetryBackoff:          250 * time.Millisecond,
		KeyEncoder:            &ByteEncoder{},
		ValueEncoder:          &ByteEncoder{},
		Partitioner:           NewRandomPartitioner,
		Timeout:               10 * time.Second,
		BatchSize:             10,
		MaxMessagesPerRequest: 100,
		FlushByteCount:        65535,
		FlushTimeout:          5 * time.Second,
		AckSuccesses:          false,
		SendBufferSize:        1,
		CompressionCodec:      "none",
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
// of a line indicates a comment. Blank lines are ignored. The file should end with a newline character.
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

	if this.Partitioner == nil {
		return errors.New("Producer partitioner cannot be empty")
	}

	return nil
}

// Partitioner sends messages to partitions that correspond message keys
type FixedPartitioner struct{}

func NewFixedPartitioner() Partitioner {
	return &FixedPartitioner{}
}

func (this *FixedPartitioner) Partition(key []byte, numPartitions int32) (int32, error) {
	if key == nil {
		panic("FixedPartitioner does not work without keys.")
	}

	var partition int32
	buf := bytes.NewBuffer(key)
	binary.Read(buf, binary.LittleEndian, &partition)
	if (partition < 0) {
		return -1, errors.New("Partition turned to be -1 (too big to be int32 little endian?)")
	}

	return int32(partition) % numPartitions, nil
}

func (this *FixedPartitioner) RequiresConsistency() bool {
	return true
}

// RandomPartitioner implements the Partitioner interface by choosing a random partition each time.
type RandomPartitioner struct {
	generator *rand.Rand
}

func NewRandomPartitioner() Partitioner {
	return &RandomPartitioner{
		generator: rand.New(rand.NewSource(time.Now().UTC().UnixNano())),
	}
}

func (this *RandomPartitioner) Partition(key []byte, numPartitions int32) (int32, error) {
	return int32(this.generator.Intn(int(numPartitions))), nil
}

func (this *RandomPartitioner) RequiresConsistency() bool {
	return false
}

// RoundRobinPartitioner implements the Partitioner interface by walking through the available partitions one at a time.
type RoundRobinPartitioner struct {
	partition int32
}

func NewRoundRobinPartitioner() Partitioner {
	return &RoundRobinPartitioner{}
}

func (this *RoundRobinPartitioner) Partition(key []byte, numPartitions int32) (int32, error) {
	if this.partition >= numPartitions {
		this.partition = 0
	}
	ret := this.partition
	this.partition++
	return ret, nil
}

func (this *RoundRobinPartitioner) RequiresConsistency() bool {
	return false
}

// HashPartitioner implements the Partitioner interface. If the key is nil, or fails to encode, then a random partition
// is chosen. Otherwise the FNV-1a hash of the encoded bytes is used modulus the number of partitions. This ensures that messages
// with the same key always end up on the same partition.
type HashPartitioner struct {
	random Partitioner
	hasher hashing.Hash32
}

func NewHashPartitioner() Partitioner {
	p := new(HashPartitioner)
	p.random = NewRandomPartitioner()
	p.hasher = fnv.New32a()
	return p
}

func (this *HashPartitioner) Partition(key []byte, numPartitions int32) (int32, error) {
	if key == nil {
		return this.random.Partition(key, numPartitions)
	}

	this.hasher.Reset()
	_, err := this.hasher.Write(key)
	if err != nil {
		return -1, err
	}
	hash := int32(this.hasher.Sum32())
	if hash < 0 {
		hash = -hash
	}
	return hash % numPartitions, nil
}

func (this *HashPartitioner) RequiresConsistency() bool {
	return true
}

// ConstantPartitioner implements the Partitioner interface by just returning a constant value.
type ConstantPartitioner struct {
	Constant int32
}

func (p *ConstantPartitioner) Partition(key Encoder, numPartitions int32) (int32, error) {
	return p.Constant, nil
}

func (p *ConstantPartitioner) RequiresConsistency() bool {
	return true
}
