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

var emptyConsumerMetadataRequestBytes = []byte{0x00, 0x00}
var goodConsumerMetadataRequestBytes = []byte{0x00, 0x11, 0x67, 0x6F, 0x2D, 0x63, 0x6F, 0x6E, 0x73, 0x75, 0x6D, 0x65, 0x72, 0x2D, 0x67, 0x72, 0x6F, 0x75, 0x70}

var goodConsumerMetadataResponseBytes = []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x09, 0x6C, 0x6F, 0x63, 0x61, 0x6C, 0x68, 0x6F, 0x73, 0x74, 0x00, 0x00, 0x23, 0x84}
var invalidErrCodeConsumerMetadataResponseBytes = []byte{0x00}
var invalidCoordinatorIDConsumerMetadataResponseBytes = []byte{0x00, 0x00, 0x00, 0x00, 0x00}
var invalidCoordinatorHostConsumerMetadataResponseBytes = []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x09, 0x6C, 0x6F, 0x63}
var invalidCoordinatorPortConsumerMetadataResponseBytes = []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x09, 0x6C, 0x6F, 0x63, 0x61, 0x6C, 0x68, 0x6F, 0x73, 0x74, 0x00, 0x00}

func TestConsumerMetadataRequest(t *testing.T) {
	emptyConsumerMetadataRequest := new(ConsumerMetadataRequest)
	testRequest(t, emptyConsumerMetadataRequest, emptyConsumerMetadataRequestBytes)

	goodConsumerMetadataRequest := NewConsumerMetadataRequest("go-consumer-group")
	testRequest(t, goodConsumerMetadataRequest, goodConsumerMetadataRequestBytes)
}

func TestConsumerMetadataResponse(t *testing.T) {
	goodConsumerMetadataResponse := new(ConsumerMetadataResponse)
	decode(t, goodConsumerMetadataResponse, goodConsumerMetadataResponseBytes)
	assert(t, goodConsumerMetadataResponse.Error, ErrNoError)
	assert(t, goodConsumerMetadataResponse.Coordinator.ID, int32(0))
	assert(t, goodConsumerMetadataResponse.Coordinator.Host, "localhost")
	assert(t, goodConsumerMetadataResponse.Coordinator.Port, int32(9092))

	decodeErr(t, new(ConsumerMetadataResponse), invalidErrCodeConsumerMetadataResponseBytes, NewDecodingError(ErrEOF, reasonInvalidConsumerMetadataErrorCode))
	decodeErr(t, new(ConsumerMetadataResponse), invalidCoordinatorIDConsumerMetadataResponseBytes, NewDecodingError(ErrEOF, reasonInvalidConsumerMetadataCoordinatorID))
	decodeErr(t, new(ConsumerMetadataResponse), invalidCoordinatorHostConsumerMetadataResponseBytes, NewDecodingError(ErrEOF, reasonInvalidConsumerMetadataCoordinatorHost))
	decodeErr(t, new(ConsumerMetadataResponse), invalidCoordinatorPortConsumerMetadataResponseBytes, NewDecodingError(ErrEOF, reasonInvalidConsumerMetadataCoordinatorPort))
}
