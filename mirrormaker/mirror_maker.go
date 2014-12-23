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
	"os"
	kafka "github.com/stealthly/go_kafka_client"
	"os/signal"
	"github.com/Shopify/sarama"
	"strings"
)

type consumerConfigs []string

func (i *consumerConfigs) String() string {
	return fmt.Sprintf("%s", *i)
}

func (i *consumerConfigs) Set(value string) error {
	*i = append(*i, value)
	return nil
}

var consumers []*kafka.Consumer
var producers []*sarama.Producer
var producerRoutineStoppers []chan bool
var messageChannel chan *kafka.Message

var whitelist = flag.String("whitelist", "", "regex pattern for whitelist. Providing both whitelist and blacklist is an error.")
var blacklist = flag.String("blacklist", "", "regex pattern for blacklist. Providing both whitelist and blacklist is an error.")
var consumerConfig consumerConfigs
var producerConfig = flag.String("producer.config", "", "Path to producer configuration file.")
var numProducers = flag.Int("num.producers", 1, "Number of producers.")
var numStreams = flag.Int("num.streams", 1, "Number of consumption streams.")
var preservePartitions = flag.Bool("preserve.partitions", false, "preserve partition number. E.g. if message was read from partition 5 it'll be written to partition 5.")
var prefix = flag.String("prefix", "", "Destination topic prefix.")
var queueSize = flag.Int("queue.size", 10000, "Number of messages that are buffered between the consumer and producer.")

func parseAndValidateArgs() {
	flag.Var(&consumerConfig, "consumer.config", "Path to consumer configuration file.")
	flag.Parse()

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
}

func main() {
	parseAndValidateArgs()
	messageChannel = make(chan *kafka.Message, *queueSize)

	startConsumers()
	startProducers()

	ctrlc := make(chan os.Signal, 1)
	signal.Notify(ctrlc, os.Interrupt)
	<-ctrlc
	shutdown()
}

func startConsumers() {
	for _, consumerConfigFile := range consumerConfig {
		config, err := kafka.ConsumerConfigFromFile(consumerConfigFile)
		if err != nil {
			panic(err)
		}
		zkConfig, err := kafka.ZookeeperConfigFromFile(consumerConfigFile)
		if err != nil {
			panic(err)
		}
		config.NumWorkers = 1
		config.Coordinator = kafka.NewZookeeperCoordinator(zkConfig)
		config.WorkerFailureCallback = func(_ *kafka.WorkerManager) kafka.FailedDecision {
			return kafka.CommitOffsetAndContinue
		}
		config.WorkerFailedAttemptCallback = func(_ *kafka.Task, _ kafka.WorkerResult) kafka.FailedDecision {
			return kafka.CommitOffsetAndContinue
		}
		config.Strategy = func(_ *kafka.Worker, msg *kafka.Message, id kafka.TaskId) kafka.WorkerResult {
			kafka.Info("mirrormaker", string(msg.Value))
			messageChannel <- msg

			return kafka.NewSuccessfulResult(id)
		}

		consumer := kafka.NewConsumer(config)
		consumers = append(consumers, consumer)
		if *whitelist != "" {
			go consumer.StartWildcard(kafka.NewWhiteList(*whitelist), *numStreams)
		} else {
			go consumer.StartWildcard(kafka.NewBlackList(*blacklist), *numStreams)
		}
	}
}

func startProducers() {
	for i := 0; i < *numProducers; i++ {
		conf, err := kafka.ProducerConfigFromFile(*producerConfig)
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
		if *preservePartitions {
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
		producers = append(producers, producer)
		stopper := make(chan bool)
		producerRoutineStoppers = append(producerRoutineStoppers, stopper)
		go produceRoutine(producer, stopper)
	}
}

func produceRoutine(producer *sarama.Producer, stopper chan bool) {
	for {
		select {
			case <-stopper: {
				kafka.Info("mirrormaker", "Producer stop triggered")
				return
			}
			case msg := <-messageChannel: {
				kafka.Infof("mirrormaker", "Producing message: %s", msg)
				producer.Input() <- &sarama.MessageToSend{Topic: *prefix + msg.Topic, Key: sarama.ByteEncoder(msg.Key), Value: sarama.ByteEncoder(msg.Value)}
			}
		}
	}
}

func shutdown() {
	consumerCloseChannels := make([]<-chan bool, 0)
	for _, consumer := range consumers {
		consumerCloseChannels = append(consumerCloseChannels, consumer.Close())
	}

	for _, ch := range consumerCloseChannels {
		<-ch
	}

	for _, stopper := range producerRoutineStoppers {
		stopper <- true
	}

	for _, producer := range producers {
		producer.Close()
	}
}
