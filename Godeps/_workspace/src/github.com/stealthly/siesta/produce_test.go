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

package siesta

import "testing"

var emptyProduceRequestBytes = []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
var goodProduceRequestBytes = []byte{0x00, 0x01, 0x00, 0x00, 0x07, 0xD0, 0x00, 0x00, 0x00, 0x01, 0x00, 0x06, 0x73, 0x69, 0x65, 0x73, 0x74, 0x61, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x25, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x19, 0x56, 0x5B, 0xC1, 0xEC, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0b, 0x68, 0x65, 0x6C, 0x6C, 0x6F, 0x20, 0x77, 0x6F, 0x72, 0x6C, 0x64}

var emptyProduceResponseBytes = []byte{0x00, 0x00, 0x00, 0x00}
var goodProduceResponseBytes = []byte{0x00, 0x00, 0x00, 0x01, 0x00, 0x06, 0x73, 0x69, 0x65, 0x73, 0x74, 0x61, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x77, 0xB4, 0x67}
var invalidTopicsLengthProduceResponseBytes = []byte{0x00, 0x00, 0x00}
var invalidTopicProduceResponseBytes = []byte{0x00, 0x00, 0x00, 0x01, 0x00, 0x06, 0x73, 0x69}
var invalidPartitionsLengthProduceResponseBytes = []byte{0x00, 0x00, 0x00, 0x01, 0x00, 0x06, 0x73, 0x69, 0x65, 0x73, 0x74, 0x61, 0x00, 0x00, 0x00}
var invalidPartitionProduceResponseBytes = []byte{0x00, 0x00, 0x00, 0x01, 0x00, 0x06, 0x73, 0x69, 0x65, 0x73, 0x74, 0x61, 0x00, 0x00, 0x00, 0x01, 0x00}
var invalidErrorCodeProduceResponseBytes = []byte{0x00, 0x00, 0x00, 0x01, 0x00, 0x06, 0x73, 0x69, 0x65, 0x73, 0x74, 0x61, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00}
var invalidOffsetProduceResponseBytes = []byte{0x00, 0x00, 0x00, 0x01, 0x00, 0x06, 0x73, 0x69, 0x65, 0x73, 0x74, 0x61, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x77}

func TestProduceRequest(t *testing.T) {
	emptyProduceRequest := new(ProduceRequest)
	testRequest(t, emptyProduceRequest, emptyProduceRequestBytes)

	goodProduceRequest := new(ProduceRequest)
	goodProduceRequest.RequiredAcks = 1
	goodProduceRequest.AckTimeoutMs = 2000
	goodProduceRequest.AddMessage("siesta", 0, &Message{Value: []byte("hello world")})
	testRequest(t, goodProduceRequest, goodProduceRequestBytes)
}

func TestProduceResponse(t *testing.T) {
	emptyProduceResponse := new(ProduceResponse)
	decode(t, emptyProduceResponse, emptyProduceResponseBytes)
	assert(t, len(emptyProduceResponse.Status), 0)

	goodProduceResponse := new(ProduceResponse)
	decode(t, goodProduceResponse, goodProduceResponseBytes)
	partitionResponse, exists := goodProduceResponse.Status["siesta"]
	assertFatal(t, exists, true)
	responseData, exists := partitionResponse[0]
	assertFatal(t, exists, true)
	assert(t, responseData.Error, ErrNoError)
	assert(t, responseData.Offset, int64(41399399))

	decodeErr(t, new(ProduceResponse), invalidTopicsLengthProduceResponseBytes, NewDecodingError(ErrEOF, reasonInvalidProduceTopicsLength))
	decodeErr(t, new(ProduceResponse), invalidTopicProduceResponseBytes, NewDecodingError(ErrEOF, reasonInvalidProduceTopic))
	decodeErr(t, new(ProduceResponse), invalidPartitionsLengthProduceResponseBytes, NewDecodingError(ErrEOF, reasonInvalidProducePartitionsLength))
	decodeErr(t, new(ProduceResponse), invalidPartitionProduceResponseBytes, NewDecodingError(ErrEOF, reasonInvalidProducePartition))
	decodeErr(t, new(ProduceResponse), invalidErrorCodeProduceResponseBytes, NewDecodingError(ErrEOF, reasonInvalidProduceErrorCode))
	decodeErr(t, new(ProduceResponse), invalidOffsetProduceResponseBytes, NewDecodingError(ErrEOF, reasonInvalidProduceOffset))
}
