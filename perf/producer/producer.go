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

package main

import (
	"flag"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/stealthly/go-avro"
	kafka "github.com/stealthly/go_kafka_client"
	sp "github.com/stealthly/go_kafka_client/syslog/syslog_proto"
	"os"
	"strings"
	"time"
)

var brokerList = flag.String("broker.list", "", "Broker List to produce messages too.")
var schemaRegistry = flag.String("schema.registry", "", "Confluent schema registry url")
var topic1 = flag.String("topic1", "", "Topic to produce generated values to.")
var topic2 = flag.String("topic2", "", "Topic to produce generated values to after getting an ack from topic1.")
var avroSchema = flag.String("avsc", "../avro/timings.avsc", "Avro schema to use.")
var perSecond = flag.Int("msg.per.sec", 0, "Messages per second to send.")

var protobuf = true

func main() {
	parseAndValidateArgs()

	if protobuf {
		produceLogLineProtobuf()
	} else {
		produceAvro()
	}
}

func produceLogLineProtobuf() {
	config1 := kafka.DefaultProducerConfig()
	config1.BrokerList = strings.Split(*brokerList, ",")
	config1.AckSuccesses = true
	producer1 := kafka.NewSaramaProducer(config1)

	config2 := kafka.DefaultProducerConfig()
	config2.BrokerList = strings.Split(*brokerList, ",")
	producer2 := kafka.NewSaramaProducer(config2)

	go func() {
		for message := range producer1.Successes() {
			line := &sp.LogLine{}
			proto.Unmarshal(message.Value.([]byte), line)
			line.Timings = append(line.Timings, time.Now().UnixNano()/int64(time.Millisecond))
			bytes, err := proto.Marshal(line)
			if err != nil {
				panic(err)
			}

			producer2.Input() <- &kafka.ProducerMessage{Topic: *topic2, Value: bytes}
		}
	}()

	for _ = range time.Tick(1 * time.Second) {
		messagesSent := 0
		for messagesSent < *perSecond {
			line := &sp.LogLine{}
			line.Line = proto.String("")
			line.Timings = []int64{time.Now().UnixNano() / int64(time.Millisecond)}
			bytes, err := proto.Marshal(line)
			if err != nil {
				panic(err)
			}

			message := &kafka.ProducerMessage{Topic: *topic1, Value: bytes}
			producer1.Input() <- message
			messagesSent++
		}
	}
}

func produceAvro() {
	config1 := kafka.DefaultProducerConfig()
	config1.BrokerList = strings.Split(*brokerList, ",")
	config1.ValueEncoder = kafka.NewKafkaAvroEncoder(*schemaRegistry)
	config1.AckSuccesses = true

	producer1 := kafka.NewSaramaProducer(config1)

	config2 := kafka.DefaultProducerConfig()
	config2.BrokerList = strings.Split(*brokerList, ",")
	config2.ValueEncoder = kafka.NewKafkaAvroEncoder(*schemaRegistry)
	producer2 := kafka.NewSaramaProducer(config2)

	avroSchema, err := avro.ParseSchemaFile(*avroSchema)
	if err != nil {
		panic(err)
	}

    _, err = kafka.NewCachedSchemaRegistryClient(*schemaRegistry).Register(avroSchema.GetName() + "-value", avroSchema)
    if err != nil {
        panic(err)
    }

	decoder := kafka.NewKafkaAvroDecoder(*schemaRegistry)
	go func() {
		for message := range producer1.Successes() {
			rawRecord, err := decoder.Decode(message.Value.([]byte))
			if err != nil {
				panic(err)
			}
			record := rawRecord.(*avro.GenericRecord)
			timings := record.Get("timings").([]interface{})
			timings = append(timings, time.Now().UnixNano() / int64(time.Millisecond))
			record.Set("timings", timings)

			producer2.Input() <- &kafka.ProducerMessage{Topic: *topic2, Value: record}
		}
	}()

	for _ = range time.Tick(1 * time.Second) {
		messagesSent := 0
		for messagesSent < *perSecond {
			record := avro.NewGenericRecord(avroSchema)
			record.Set("id", int64(0))
			record.Set("timings", []int64{time.Now().UnixNano() / int64(time.Millisecond)})
			record.Set("value", []byte{})

			message := &kafka.ProducerMessage{Topic: *topic1, Value: record}
			producer1.Input() <- message
			messagesSent++
		}
	}
}

func parseAndValidateArgs() {
	flag.Parse()
	if *brokerList == "" {
		fmt.Println("Broker list is required")
		os.Exit(1)
	}

    if *schemaRegistry != "" {
	    protobuf = false
        if *avroSchema == "" {
            fmt.Println("Avro schema is required")
            os.Exit(1)
        }
	}

	if *perSecond <= 0 {
		fmt.Println("Messages per second should be greater than 0")
		os.Exit(1)
	}

	if *topic1 == "" {
		fmt.Println("Topic 1 is required")
		os.Exit(1)
	}

	if *topic2 == "" {
		fmt.Println("Topic 2 is required")
		os.Exit(1)
	}
}
