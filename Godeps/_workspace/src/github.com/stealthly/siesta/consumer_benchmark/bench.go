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
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	args := os.Args

	if len(args) != 5 {
		fmt.Println("USAGE: consumer <brokerlist> <topic> <partition> <duration (how many seconds to run)>")
	}

	brokerList := args[1]
	topic := args[2]
	partition, err := strconv.Atoi(args[3])
	if err != nil {
		panic(err)
	}
	seconds, err := strconv.Atoi(args[4])
	if err != nil {
		panic(err)
	}

	testSiesta(brokerList, topic, int32(partition), seconds)
}

func testSiesta(brokerList string, topic string, partition int32, seconds int) {
	stop := false

	config := siesta.NewConnectorConfig()
	config.BrokerList = strings.Split(brokerList, ",")

	connector, err := siesta.NewDefaultConnector(config)
	if err != nil {
		panic(err)
	}

	messageChannel := make(chan []*siesta.MessageAndMetadata, 10000)
	count := 0
	go func() {
		for {
			messages := <-messageChannel
			count += len(messages)
		}
	}()

	//warm up
	fmt.Println("warming up")
	for i := 0; i < 5; i++ {
		connector.Fetch(topic, partition, 0)
	}
	fmt.Println("warm up finished, starting")

	go func() {
		time.Sleep(time.Duration(seconds) * time.Second)
		stop = true
	}()

	offset := int64(0)
	for !stop {
		response, err := connector.Fetch(topic, partition, offset)
		if err != nil {
			panic(err)
		}
		messages, err := response.GetMessages()
		if err != nil {
			panic(err)
		}
		messageChannel <- messages
		offset = messages[len(messages)-1].Offset
	}

	fmt.Printf("%d within %d secnods\n", count, seconds)
	fmt.Printf("%d average\n", count/seconds)
}
