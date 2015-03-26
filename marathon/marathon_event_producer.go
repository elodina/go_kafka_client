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
	"github.com/stealthly/go-avro"
	kafka "github.com/stealthly/go_kafka_client"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"time"
)

var logLevel = flag.String("log.level", "info", "Log level for built-in logger.")
var port = flag.Int("port", 9000, "HTTP endpoint binding port. Defaults to 9000.")
var pattern = flag.String("pattern", "/", `HTTP endpoint url pattern to listen, e.g. "/marathon". Defaults to "/".`)
var registry = flag.String("schema.registry", "", "URL to Confluent Schema Registry. Setting this triggers all events to be sent in Avro format. Turned off by default.")
var avroSchema = flag.String("avsc", "http_request.avsc", "Avro schema to use when producing Avro messages.")
var producerConfig = flag.String("producer.config", "", "Path to producer configuration file.")
var topic = flag.String("topic", "", "Topic to produce messages into.")
var maxProcs = flag.Int("max.procs", runtime.NumCPU(), "Maximum number of CPUs that can be executing simultaneously.")
var brokerList = flag.String("broker.list", "", "Broker List to produce messages too.")
var requiredAcks = flag.Int("required.acks", 1, "Required acks for producer. 0 - no server response. 1 - the server will wait the data is written to the local log. -1 - the server will block until the message is committed by all in sync replicas.")
var acksTimeout = flag.Int("acks.timeout", 1000, "This provides a maximum time in milliseconds the server can await the receipt of the number of acknowledgements in RequiredAcks.")

func main() {
	config := parseAndValidateArgs()
	producer := kafka.NewMarathonEventProducer(config)
	go producer.Start()

	ctrlc := make(chan os.Signal, 1)
	signal.Notify(ctrlc, os.Interrupt)
	<-ctrlc
	producer.Stop()
}

func parseAndValidateArgs() *kafka.MarathonEventProducerConfig {
	flag.Parse()

	setLogLevel()
	runtime.GOMAXPROCS(*maxProcs)

	if *brokerList == "" {
		fmt.Println("broker.list is required.")
		os.Exit(1)
	}

	if *topic == "" {
		fmt.Println("Topic is required.")
		os.Exit(1)
	}

	config := kafka.NewMarathonEventProducerConfig()
	conf, err := kafka.ProducerConfigFromFile(*producerConfig)
	useFile := true
	if err != nil {
		//we dont have a producer configuraiton which is ok
		useFile = false
	} else {
		if err = conf.Validate(); err != nil {
			panic(err)
		}
	}

	if useFile {
		config.ProducerConfig = conf
	} else {
		config.ProducerConfig = kafka.DefaultProducerConfig()
		config.ProducerConfig.Acks = *requiredAcks
		config.ProducerConfig.Timeout = time.Duration(*acksTimeout) * time.Millisecond
	}

	config.Topic = *topic
	config.BrokerList = *brokerList
	config.Port = *port
	config.Pattern = *pattern
	config.SchemaRegistryUrl = *registry

	if config.SchemaRegistryUrl != "" {
		schema, err := avro.ParseSchemaFile(*avroSchema)
		if err != nil {
			fmt.Printf("Could not parse schema file: %s\n", err)
			os.Exit(1)
		}
		config.AvroSchema = schema
	}

	return config
}

func setLogLevel() {
	var level kafka.LogLevel
	switch strings.ToLower(*logLevel) {
	case "trace":
		level = kafka.TraceLevel
	case "debug":
		level = kafka.DebugLevel
	case "info":
		level = kafka.DebugLevel
	case "warn":
		level = kafka.DebugLevel
	case "error":
		level = kafka.DebugLevel
	case "critical":
		level = kafka.DebugLevel
	default:
		{
			fmt.Printf("Invalid log level: %s\n", *logLevel)
			os.Exit(1)
		}
	}
	kafka.Logger = kafka.NewDefaultLogger(level)
}
