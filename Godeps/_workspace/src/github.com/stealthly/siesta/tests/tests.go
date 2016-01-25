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
	"io"
	"net"
	"time"
	//	"encoding/hex"
)

func main() {
	//	b := make([]byte, 4)
	//	siesta.NewBinaryEncoder(b).WriteInt32(750693)
	//	fmt.Printf("bytes: % x\n", b)

	//	request := siesta.NewTopicMetadataRequest([]string{"logs1"})
	//	request := new(siesta.OffsetRequest)
	//	request.AddPartitionOffsetRequestInfo("test-2", 0, siesta.LatestTime, 1)

	//	request := new(siesta.FetchRequest)
	//	request.AddFetch("test-2", 0, 0, 50)

	//	request := siesta.NewConsumerMetadataRequest("go-consumer-group")

	//	request := siesta.NewOffsetCommitRequest("other-go-group")
	//	request.AddOffset("test-2", 0, 7, 123456789, "h")

	//	request := siesta.NewOffsetFetchRequest("other-go-group")
	//	request.AddOffset("test-2", 0)

	request := new(siesta.ProduceRequest)
	request.RequiredAcks = 1
	request.AckTimeoutMs = 2000
	request.AddMessage("siesta", 0, &siesta.Message{MagicByte: -12, Value: []byte("hello world")})

	sizing := siesta.NewSizingEncoder()
	request.Write(sizing)
	bytes := make([]byte, sizing.Size())
	encoder := siesta.NewBinaryEncoder(bytes)
	request.Write(encoder)
	fmt.Printf("request % x\n", bytes)

	connection := openConnection("localhost:9092")
	writeRequest(request, connection)
	readResponse(connection)
}

func openConnection(host string) net.Conn {
	conn, err := net.DialTimeout("tcp", host, 2*time.Second)
	if err != nil {
		panic(err)
	}

	return conn
}

func writeRequest(request siesta.Request, connection net.Conn) {
	header := siesta.NewRequestHeader(123, "test-client", request)
	bytes := make([]byte, header.Size())
	encoder := siesta.NewBinaryEncoder(bytes)
	header.Write(encoder)
	fmt.Printf("full request: % x\n", bytes)

	_, err := connection.Write(bytes)
	if err != nil {
		panic(err)
	}
}

func readResponse(connection net.Conn) {
	header := make([]byte, 8)
	_, err := io.ReadFull(connection, header)
	fmt.Printf("response header: % x\n", header)

	decoder := siesta.NewBinaryDecoder(header)
	length, err := decoder.GetInt32()
	if err != nil {
		panic(err)
	}
	correlationId, err := decoder.GetInt32()
	if err != nil {
		panic(err)
	}
	fmt.Println("correlationId", correlationId)

	response := make([]byte, length-4)
	_, err = io.ReadFull(connection, response)
	if err != nil {
		panic(err)
	}

	fmt.Printf("response: % x\n", response)

	decoder = siesta.NewBinaryDecoder(response)
	//	metadataResponse := new(siesta.TopicMetadataResponse)
	//	metadataResponse := new(siesta.OffsetResponse)
	//	metadataResponse := new(siesta.FetchResponse)
	//	metadataResponse := new(siesta.ConsumerMetadataResponse)
	//	metadataResponse := new(siesta.OffsetCommitResponse)
	//	metadataResponse := new(siesta.OffsetFetchResponse)
	metadataResponse := new(siesta.ProduceResponse)
	decodingErr := metadataResponse.Read(decoder)
	if decodingErr != nil {
		panic(decodingErr.Reason())
	}

	//	fmt.Println("decoded: ", metadataResponse)
	//	for _, broker := range metadataResponse.Brokers {
	//		fmt.Println("broker", broker)
	//	}
	//
	//	for _, meta := range metadataResponse.TopicMetadata {
	//		fmt.Println("meta", meta)
	//		for _, pmeta := range meta.PartitionMetadata {
	//			fmt.Println("pmeta", pmeta)
	//		}
	//	}
	//	for _, message := range metadataResponse.GetMessages() {
	//		fmt.Printf("topic: %s, partition: %d, offset: %d, value: %s\n", message.Topic, message.Partition, message.Offset, string(message.Value))
	//	}
	//	fmt.Println("messages: ", metadataResponse.GetMessages())
	//	fmt.Println("offset", metadataResponse.Offsets["test-2"][0].Offsets[0])

	//consumer metadata
	fmt.Printf("%v\n", metadataResponse)

	//	fmt.Println(metadataResponse.Offsets["test-2"][0].Error)
	//	fmt.Println(metadataResponse.Offsets["test-2"][0].Metadata)
	//	fmt.Println(metadataResponse.Offsets["test-2"][0].Offset)

	fmt.Println(metadataResponse.Status["siesta"][0].Error)
	fmt.Println(metadataResponse.Status["siesta"][0].Offset)
}

func readResponse(rawResponse []byte) {
	header := rawResponse[:8]

	decoder := siesta.NewBinaryDecoder(header)
	length, err := decoder.GetInt32()
	if err != nil {
		panic(err)
	}
	correlationId, err := decoder.GetInt32()
	if err != nil {
		panic(err)
	}

	response := rawResponse[8:]
	response := make([]byte, length-4)
	_, err = io.ReadFull(connection, response)
	if err != nil {
		panic(err)
	}

	fmt.Printf("response: % x\n", response)

	decoder = siesta.NewBinaryDecoder(response)
	//	metadataResponse := new(siesta.TopicMetadataResponse)
	//	metadataResponse := new(siesta.OffsetResponse)
	//	metadataResponse := new(siesta.FetchResponse)
	//	metadataResponse := new(siesta.ConsumerMetadataResponse)
	//	metadataResponse := new(siesta.OffsetCommitResponse)
	//	metadataResponse := new(siesta.OffsetFetchResponse)
	metadataResponse := new(siesta.ProduceResponse)
	decodingErr := metadataResponse.Read(decoder)
	if decodingErr != nil {
		panic(decodingErr.Reason())
	}

	//	fmt.Println("decoded: ", metadataResponse)
	//	for _, broker := range metadataResponse.Brokers {
	//		fmt.Println("broker", broker)
	//	}
	//
	//	for _, meta := range metadataResponse.TopicMetadata {
	//		fmt.Println("meta", meta)
	//		for _, pmeta := range meta.PartitionMetadata {
	//			fmt.Println("pmeta", pmeta)
	//		}
	//	}
	//	for _, message := range metadataResponse.GetMessages() {
	//		fmt.Printf("topic: %s, partition: %d, offset: %d, value: %s\n", message.Topic, message.Partition, message.Offset, string(message.Value))
	//	}
	//	fmt.Println("messages: ", metadataResponse.GetMessages())
	//	fmt.Println("offset", metadataResponse.Offsets["test-2"][0].Offsets[0])

	//consumer metadata
	fmt.Printf("%v\n", metadataResponse)

	//	fmt.Println(metadataResponse.Offsets["test-2"][0].Error)
	//	fmt.Println(metadataResponse.Offsets["test-2"][0].Metadata)
	//	fmt.Println(metadataResponse.Offsets["test-2"][0].Offset)

	fmt.Println(metadataResponse.Status["siesta"][0].Error)
	fmt.Println(metadataResponse.Status["siesta"][0].Offset)
}
