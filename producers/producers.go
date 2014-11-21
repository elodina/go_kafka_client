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

package main

import (
	kafkaClient "github.com/stealthly/go_kafka_client"
	"time"
	"github.com/stealthly/go-kafka/producer"
	"fmt"
	"strconv"
	"os"
	"os/signal"
)

func resolveConfig() (string, string, string, int, time.Duration) {
	rawConfig, err := kafkaClient.LoadConfiguration("producers.properties")
	if err != nil {
		panic(err)
	}

	zkConnect := rawConfig["zookeeper_connect"]
	brokerConnect := rawConfig["broker_connect"]
	topic := rawConfig["topic"]
	numPartitions, _ := strconv.Atoi(rawConfig["num_partitions"])
	sleepTime, _ := time.ParseDuration(rawConfig["sleep_time"])

	return zkConnect, brokerConnect, topic, numPartitions, sleepTime
}

func main() {
	numMessage := 0

	zkConnect, brokerConnect, topic, numPartitions, sleepTime := resolveConfig()

	kafkaClient.CreateMultiplePartitionsTopic(zkConnect, topic, numPartitions)

	p := producer.NewKafkaProducer(topic, []string{brokerConnect}, nil)
	defer p.Close()
	go func() {
		for {
			if err := p.Send(fmt.Sprintf("message %d!", numMessage)); err != nil {
				panic(err)
			}
			numMessage++
			time.Sleep(sleepTime)
		}
	}()

	ctrlc := make(chan os.Signal, 1)
	signal.Notify(ctrlc, os.Interrupt)
	<-ctrlc
}
