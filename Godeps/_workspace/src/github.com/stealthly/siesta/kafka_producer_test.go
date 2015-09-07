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

package siesta

import (
	"fmt"
	"testing"
	"time"
)

func TestProducerSend1(t *testing.T) {
	connector := testConnector(t)
	producerConfig := &ProducerConfig{
		BatchSize:       1,
		ClientID:        "siesta",
		MaxRequests:     10,
		SendRoutines:    10,
		ReceiveRoutines: 10,
		ReadTimeout:     5 * time.Second,
		WriteTimeout:    5 * time.Second,
		RequiredAcks:    1,
		AckTimeoutMs:    2000,
		Linger:          1 * time.Second,
	}
	producer := NewKafkaProducer(producerConfig, ByteSerializer, StringSerializer, connector)
	producer.Send(&ProducerRecord{Topic: "siesta", Value: "hello world"})

	select {
	case metadata := <-producer.RecordsMetadata:
		assert(t, metadata.Error, ErrNoError)
		assert(t, metadata.Topic, "siesta")
		assert(t, metadata.Partition, int32(0))
	case <-time.After(5 * time.Second):
		t.Error("Could not get produce response within 5 seconds")
	}

	producer.Close(1 * time.Second)
}

func TestProducerSend1000(t *testing.T) {
	connector := testConnector(t)
	producerConfig := &ProducerConfig{
		BatchSize:       100,
		ClientID:        "siesta",
		MaxRequests:     10,
		SendRoutines:    10,
		ReceiveRoutines: 10,
		ReadTimeout:     5 * time.Second,
		WriteTimeout:    5 * time.Second,
		RequiredAcks:    1,
		AckTimeoutMs:    2000,
		Linger:          1 * time.Second,
	}
	producer := NewKafkaProducer(producerConfig, ByteSerializer, StringSerializer, connector)
	for i := 0; i < 1000; i++ {
		producer.Send(&ProducerRecord{Topic: "siesta", Value: fmt.Sprintf("%d", i)})
	}

	for i := 0; i < 1000; i++ {
		select {
		case metadata := <-producer.RecordsMetadata:
			assert(t, metadata.Error, ErrNoError)
			assert(t, metadata.Topic, "siesta")
			assert(t, metadata.Partition, int32(0))
		case <-time.After(5 * time.Second):
			t.Fatal("Could not get produce response within 5 seconds")
		}
	}

	producer.Close(1 * time.Second)
}

func TestProducerRequiredAcks0(t *testing.T) {
	connector := testConnector(t)
	producerConfig := &ProducerConfig{
		BatchSize:       100,
		ClientID:        "siesta",
		MaxRequests:     10,
		SendRoutines:    10,
		ReceiveRoutines: 10,
		ReadTimeout:     5 * time.Second,
		WriteTimeout:    5 * time.Second,
		RequiredAcks:    0,
		Linger:          1 * time.Second,
	}
	producer := NewKafkaProducer(producerConfig, ByteSerializer, StringSerializer, connector)

	go func() {
		for i := 0; i < 1000; i++ {
			select {
			case metadata := <-producer.RecordsMetadata:
				assert(t, metadata.Error, ErrNoError)
				assert(t, metadata.Topic, "siesta")
				assert(t, metadata.Partition, int32(0))
				assert(t, metadata.Offset, int64(-1))
			case <-time.After(5 * time.Second):
				t.Fatal("Could not get produce response within 5 seconds")
			}
		}
	}()

	for i := 0; i < 1000; i++ {
		producer.Send(&ProducerRecord{Topic: "siesta", Value: fmt.Sprintf("%d", i)})
	}

	producer.Close(1 * time.Second)
}

func TestProducerFlushTimeout(t *testing.T) {
	connector := testConnector(t)
	producerConfig := &ProducerConfig{
		BatchSize:       1000,
		ClientID:        "siesta",
		MaxRequests:     10,
		SendRoutines:    10,
		ReceiveRoutines: 10,
		ReadTimeout:     5 * time.Second,
		WriteTimeout:    5 * time.Second,
		RequiredAcks:    0,
		Linger:          500 * time.Millisecond,
	}
	producer := NewKafkaProducer(producerConfig, ByteSerializer, StringSerializer, connector)
	for i := 0; i < 100; i++ {
		producer.Send(&ProducerRecord{Topic: "siesta", Value: fmt.Sprintf("%d", i)})
	}

	for i := 0; i < 100; i++ {
		select {
		case metadata := <-producer.RecordsMetadata:
			assert(t, metadata.Error, ErrNoError)
			assert(t, metadata.Topic, "siesta")
			assert(t, metadata.Partition, int32(0))
			assert(t, metadata.Offset, int64(-1))
		case <-time.After(5 * time.Second):
			t.Fatal("Could not get produce response within 5 seconds")
		}
	}

	producer.Close(1 * time.Second)
}
