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

	producerConfig := &siesta.ProducerConfig{
		BatchSize:       10000,
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
	producer := siesta.NewKafkaProducer(producerConfig, siesta.ByteSerializer, siesta.StringSerializer, connector)

	metadataChannel := make(chan interface{}, 10000)
	count := 0
	start := time.Now()

	for i := 0; i < 10; i++ {
		go func() {
			for {
				<-metadataChannel
				count++

				elapsed := time.Since(start)
				if elapsed.Seconds() >= 1 {
					fmt.Println(fmt.Sprintf("Per Second %d", count))
					count = 0
					start = time.Now()
				}
			}
		}()
	}

	for {
		producer.Send(&siesta.ProducerRecord{Topic: "pr1", Value: "hello world"})
		metadataChannel <- nil
	}
}
