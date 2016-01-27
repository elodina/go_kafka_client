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

var emptyOffsetCommitRequestBytes = []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
var goodOffsetCommitRequestBytes = []byte{0x00, 0x11, 0x67, 0x6f, 0x2d, 0x63, 0x6f, 0x6e, 0x73, 0x75, 0x6d, 0x65, 0x72, 0x2d, 0x67, 0x72, 0x6f, 0x75, 0x70, 0x00, 0x00, 0x00, 0x01, 0x00, 0x06, 0x74, 0x65, 0x73, 0x74, 0x2d, 0x32, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x00, 0x0b, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x20, 0x77, 0x6f, 0x72, 0x6c, 0x64}

var goodOffsetCommitResponseBytes = []byte{0x00, 0x00, 0x00, 0x01, 0x00, 0x06, 0x74, 0x65, 0x73, 0x74, 0x2d, 0x32, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
var emptyOffsetCommitResponseBytes = []byte{0x00, 0x00, 0x00, 0x00}
var invalidLengthOffsetCommitResponseBytes = []byte{0x00, 0x00, 0x00}
var invalidTopicOffsetCommitResponseBytes = []byte{0x00, 0x00, 0x00, 0x01, 0x00, 0x06, 0x74, 0x65}
var invalidPartitionsLengthOffsetCommitResponseBytes = []byte{0x00, 0x00, 0x00, 0x01, 0x00, 0x06, 0x74, 0x65, 0x73, 0x74, 0x2d, 0x32, 0x00, 0x00, 0x00}
var invalidPartitionOffsetCommitResponseBytes = []byte{0x00, 0x00, 0x00, 0x01, 0x00, 0x06, 0x74, 0x65, 0x73, 0x74, 0x2d, 0x32, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00}
var invalidErrorCodeOffsetCommitResponseBytes = []byte{0x00, 0x00, 0x00, 0x01, 0x00, 0x06, 0x74, 0x65, 0x73, 0x74, 0x2d, 0x32, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00}

func TestOffsetCommitRequest(t *testing.T) {
	emptyOffsetCommitRequest := new(OffsetCommitRequest)
	testRequest(t, emptyOffsetCommitRequest, emptyOffsetCommitRequestBytes)

	goodOffsetCommitRequest := NewOffsetCommitRequest("go-consumer-group")
	goodOffsetCommitRequest.AddOffset("test-2", 0, 4, -1, "hello world")
	testRequest(t, goodOffsetCommitRequest, goodOffsetCommitRequestBytes)
}

func TestOffsetCommitResponse(t *testing.T) {
	goodOffsetCommitResponse := new(OffsetCommitResponse)
	decode(t, goodOffsetCommitResponse, goodOffsetCommitResponseBytes)
	partitionsAndErrors, exists := goodOffsetCommitResponse.CommitStatus["test-2"]
	assertFatal(t, exists, true)
	err, exists := partitionsAndErrors[0]
	assertFatal(t, exists, true)
	assert(t, err, ErrNoError)

	emptyOffsetCommitResponse := new(OffsetCommitResponse)
	decode(t, emptyOffsetCommitResponse, emptyOffsetCommitResponseBytes)
	assert(t, len(emptyOffsetCommitResponse.CommitStatus), 0)

	decodeErr(t, new(OffsetCommitResponse), invalidLengthOffsetCommitResponseBytes, NewDecodingError(ErrEOF, reasonInvalidOffsetsMapLength))
	decodeErr(t, new(OffsetCommitResponse), invalidTopicOffsetCommitResponseBytes, NewDecodingError(ErrEOF, reasonInvalidOffsetsTopic))
	decodeErr(t, new(OffsetCommitResponse), invalidPartitionsLengthOffsetCommitResponseBytes, NewDecodingError(ErrEOF, reasonInvalidOffsetsPartitionsLength))
	decodeErr(t, new(OffsetCommitResponse), invalidPartitionOffsetCommitResponseBytes, NewDecodingError(ErrEOF, reasonInvalidOffsetsPartition))
	decodeErr(t, new(OffsetCommitResponse), invalidErrorCodeOffsetCommitResponseBytes, NewDecodingError(ErrEOF, reasonInvalidOffsetsErrorCode))
}
