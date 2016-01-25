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
	"fmt"
	"github.com/stealthly/siesta"
	"time"
)

func main() {
	config := siesta.NewConnectorConfig()
	config.BrokerList = []string{"localhost:9092"}

	connector, err := siesta.NewDefaultConnector(config)
	if err != nil {
		panic(err)
	}

	producerConfig := siesta.NewProducerConfig()
	producerConfig.BatchSize = 100
	producerConfig.ClientID = "siesta"
	producerConfig.MaxRequests = 10
	producerConfig.SendRoutines = 10
	producerConfig.ReceiveRoutines = 10
	producerConfig.ReadTimeout = 5 * time.Second
	producerConfig.WriteTimeout = 5 * time.Second
	producerConfig.RequiredAcks = 1
	producerConfig.AckTimeoutMs = 2000
	producerConfig.Linger = time.Second

	producer := siesta.NewKafkaProducer(producerConfig, siesta.ByteSerializer, siesta.StringSerializer, connector)

	go func() {
		for meta := range producer.RecordsMetadata {
			_ = meta
		}
	}()

	i := 0
	for {
		producer.Send(&siesta.ProducerRecord{Topic: "siesta", Value: fmt.Sprintf("message-%d", i)})
		i++
	}
}
