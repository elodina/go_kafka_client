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
  "github.com/aybabtme/tailf"
  "github.com/Shopify/sarama"
  "strings"
  "bufio"
  "flag"
  "log"
  "os"
    "time"
)

func main() {
	var fromStart = flag.Bool("fromStart", true, "Read from beginning of file")
	var topic = flag.String("topic", "tailf", "Kafka topic to produce to")
	var clientId = flag.String("clientId", "tailf-client", "Kafka client ID")
	var brokerList = flag.String("brokerList", "127.0.0.1:9092", "Kafka broker list, comma-delimited.")
	var verbose = flag.Bool("verbose", false, "Verbose output")
	flag.Parse()
	if len(flag.Args()) != 1 { 
	    flag.Usage()
	    os.Exit(1)
	}
	var filename = flag.Arg(0)
	
	follower, err := tailf.Follow(filename, *fromStart)
	if err != nil {
		log.Fatalf("couldn't follow %q: %v", filename, err)
	}
	defer follower.Close()

	clientConfig := sarama.NewConfig()
	clientConfig.ClientID = *clientId
    clientConfig.Producer.Timeout = 10 * time.Second
	client, err := sarama.NewClient(strings.Split(*brokerList, ","), clientConfig)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	producer, err := sarama.NewAsyncProducerFromClient(client)
	if err != nil {
		panic(err)
	}
	defer producer.Close()

	scanner := bufio.NewScanner(follower)
	for scanner.Scan() {
		producer.Input() <- &sarama.ProducerMessage{Topic: *topic, Key: nil, Value: sarama.ByteEncoder(scanner.Bytes())}
		if *verbose {
			log.Println("Produced message:", scanner.Text())
		}
	}
    if err := scanner.Err(); err != nil {
        log.Fatalf("scanner error: %v", err)
    }
}