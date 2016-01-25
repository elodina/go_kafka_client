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

var emptyOffsetFetchRequestBytes = []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
var goodOffsetFetchRequestBytes = []byte{0x00, 0x0e, 0x6f, 0x74, 0x68, 0x65, 0x72, 0x2d, 0x67, 0x6f, 0x2d, 0x67, 0x72, 0x6f, 0x75, 0x70, 0x00, 0x00, 0x00, 0x01, 0x00, 0x06, 0x74, 0x65, 0x73, 0x74, 0x2d, 0x32, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00}

var emptyOffsetFetchResponseBytes = []byte{0x00, 0x00, 0x00, 0x00}
var goodOffsetFetchResponseBytes = []byte{0x00, 0x00, 0x00, 0x01, 0x00, 0x06, 0x74, 0x65, 0x73, 0x74, 0x2d, 0x32, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00, 0x00}
var invalidOffsetsLengthOffsetFetchResponseBytes = []byte{0x00, 0x00, 0x00}
var invalidTopicOffsetFetchResponseBytes = []byte{0x00, 0x00, 0x00, 0x01, 0x00, 0x06, 0x74, 0x65}
var invalidPartitionsLengthFetchResponseBytes = []byte{0x00, 0x00, 0x00, 0x01, 0x00, 0x06, 0x74, 0x65, 0x73, 0x74, 0x2d, 0x32, 0x00, 0x00, 0x00}
var invalidPartitionFetchResponseBytes = []byte{0x00, 0x00, 0x00, 0x01, 0x00, 0x06, 0x74, 0x65, 0x73, 0x74, 0x2d, 0x32, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00}
var invalidOffsetFetchResponseBytes = []byte{0x00, 0x00, 0x00, 0x01, 0x00, 0x06, 0x74, 0x65, 0x73, 0x74, 0x2d, 0x32, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
var invalidMetadataFetchResponseBytes = []byte{0x00, 0x00, 0x00, 0x01, 0x00, 0x06, 0x74, 0x65, 0x73, 0x74, 0x2d, 0x32, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0x00}
var invalidErrorCodeFetchResponseBytes = []byte{0x00, 0x00, 0x00, 0x01, 0x00, 0x06, 0x74, 0x65, 0x73, 0x74, 0x2d, 0x32, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00}

func TestOffsetFetchRequest(t *testing.T) {
	emptyOffsetFetchRequest := new(OffsetFetchRequest)
	testRequest(t, emptyOffsetFetchRequest, emptyOffsetFetchRequestBytes)

	goodOffsetFetchRequest := NewOffsetFetchRequest("other-go-group")
	goodOffsetFetchRequest.AddOffset("test-2", 0)
	testRequest(t, goodOffsetFetchRequest, goodOffsetFetchRequestBytes)
}

func TestOffsetFetchResponse(t *testing.T) {
	emptyOffsetFetchResponse := new(OffsetFetchResponse)
	decode(t, emptyOffsetFetchResponse, emptyOffsetFetchResponseBytes)
	assert(t, len(emptyOffsetFetchResponse.Offsets), 0)

	goodOffsetFetchResponse := new(OffsetFetchResponse)
	decode(t, goodOffsetFetchResponse, goodOffsetFetchResponseBytes)
	offsetsForTopic, exists := goodOffsetFetchResponse.Offsets["test-2"]
	assertFatal(t, exists, true)
	offsetsForPartition, exists := offsetsForTopic[0]
	assertFatal(t, exists, true)
	assert(t, offsetsForPartition.Error, ErrNoError)
	assert(t, offsetsForPartition.Metadata, "")
	assert(t, offsetsForPartition.Offset, int64(7))

	decodeErr(t, new(OffsetFetchResponse), invalidOffsetsLengthOffsetFetchResponseBytes, NewDecodingError(ErrEOF, reasonInvalidOffsetsMapLength))
	decodeErr(t, new(OffsetFetchResponse), invalidTopicOffsetFetchResponseBytes, NewDecodingError(ErrEOF, reasonInvalidOffsetFetchResponseTopic))
	decodeErr(t, new(OffsetFetchResponse), invalidPartitionsLengthFetchResponseBytes, NewDecodingError(ErrEOF, reasonInvalidOffsetFetchResponsePartitionsLength))
	decodeErr(t, new(OffsetFetchResponse), invalidPartitionFetchResponseBytes, NewDecodingError(ErrEOF, reasonInvalidOffsetFetchResponsePartition))
	decodeErr(t, new(OffsetFetchResponse), invalidOffsetFetchResponseBytes, NewDecodingError(ErrEOF, reasonInvalidOffsetFetchResponseOffset))
	decodeErr(t, new(OffsetFetchResponse), invalidMetadataFetchResponseBytes, NewDecodingError(ErrEOF, reasonInvalidOffsetFetchResponseMetadata))
	decodeErr(t, new(OffsetFetchResponse), invalidErrorCodeFetchResponseBytes, NewDecodingError(ErrEOF, reasonInvalidOffsetFetchResponseErrorCode))
}
