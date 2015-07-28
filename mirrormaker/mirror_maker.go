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
	kafka "github.com/stealthly/go_kafka_client"
	"os"
	"os/signal"
	"runtime"
)

type consumerConfigs []string

func (i *consumerConfigs) String() string {
	return fmt.Sprintf("%s", *i)
}

func (i *consumerConfigs) Set(value string) error {
	*i = append(*i, value)
	return nil
}

var whitelist = flag.String("whitelist", "", "regex pattern for whitelist. Providing both whitelist and blacklist is an error.")
var blacklist = flag.String("blacklist", "", "regex pattern for blacklist. Providing both whitelist and blacklist is an error.")
var consumerConfig consumerConfigs
var producerConfig = flag.String("producer.config", "", "Path to producer configuration file.")
var numProducers = flag.Int("num.producers", 1, "Number of producers.")
var numStreams = flag.Int("num.streams", 1, "Number of consumption streams.")
var preservePartitions = flag.Bool("preserve.partitions", false, "preserve partition number. E.g. if message was read from partition 5 it'll be written to partition 5.")
var preserveOrder = flag.Bool("preserve.order", false, "E.g. message sequence 1, 2, 3, 4, 5 will remain 1, 2, 3, 4, 5 in destination topic.")
var prefix = flag.String("prefix", "", "Destination topic prefix.")
var queueSize = flag.Int("queue.size", 10000, "Number of messages that are buffered between the consumer and producer.")
var maxProcs = flag.Int("max.procs", runtime.NumCPU(), "Maximum number of CPUs that can be executing simultaneously.")
var schemaRegistryUrl = flag.String("schema.registry.url", "", "Avro schema registry URL for message encoding/decoding")
var timingsProducerConfig = flag.String("timings.producer.config", "", "Path to producer configuration file for timings.")

func parseAndValidateArgs() *kafka.MirrorMakerConfig {
	flag.Var(&consumerConfig, "consumer.config", "Path to consumer configuration file.")
	flag.Parse()
	runtime.GOMAXPROCS(*maxProcs)

	if (*whitelist != "" && *blacklist != "") || (*whitelist == "" && *blacklist == "") {
		fmt.Println("Exactly one of whitelist or blacklist is required.")
		os.Exit(1)
	}
	if *producerConfig == "" {
		fmt.Println("Producer config is required.")
		os.Exit(1)
	}
	if len(consumerConfig) == 0 {
		fmt.Println("At least one consumer config is required.")
		os.Exit(1)
	}
	if *queueSize < 0 {
		fmt.Println("Queue size should be equal or greater than 0")
		os.Exit(1)
	}
	if *timingsProducerConfig == "" && *schemaRegistryUrl == "" {
		fmt.Println("--schema.registry.url parameter is required when --timings is used")
	}

	config := kafka.NewMirrorMakerConfig()
	config.Blacklist = *blacklist
	config.Whitelist = *whitelist
	config.ChannelSize = *queueSize
	config.ConsumerConfigs = []string(consumerConfig)
	config.NumProducers = *numProducers
	config.NumStreams = *numStreams
	config.PreservePartitions = *preservePartitions
	config.PreserveOrder = *preserveOrder
	config.ProducerConfig = *producerConfig
	config.TopicPrefix = *prefix
	if *schemaRegistryUrl != "" {
		config.KeyEncoder = kafka.NewKafkaAvroEncoder(*schemaRegistryUrl)
		config.ValueEncoder = kafka.NewKafkaAvroEncoder(*schemaRegistryUrl)
		config.KeyDecoder = kafka.NewKafkaAvroDecoder(*schemaRegistryUrl)
		config.ValueDecoder = kafka.NewKafkaAvroDecoder(*schemaRegistryUrl)
	}
	config.TimingsProducerConfig = *timingsProducerConfig

	return config
}

func main() {
	config := parseAndValidateArgs()
	mirrorMaker := kafka.NewMirrorMaker(config)
	go mirrorMaker.Start()

	ctrlc := make(chan os.Signal, 1)
	signal.Notify(ctrlc, os.Interrupt)
	<-ctrlc
	mirrorMaker.Stop()
}
