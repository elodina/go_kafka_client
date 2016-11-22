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

var emptyOffsetRequestBytes = []byte{0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00}
var singleOffsetRequestBytes = []byte{0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x01, 0x00, 0x04, 0x6c, 0x6f, 0x67, 0x73, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x01}
var multipleOffsetRequestBytes = []byte{0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x01, 0x00, 0x05, 0x6c, 0x6f, 0x67, 0x73, 0x31, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFE, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFE, 0x00, 0x00, 0x00, 0x01}

var emptyOffsetResponseBytes = []byte{0x00, 0x00, 0x00, 0x00}
var singleOffsetResponseBytes = []byte{0x00, 0x00, 0x00, 0x01, 0x00, 0x04, 0x6c, 0x6f, 0x67, 0x73, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xAC}
var multipleOffsetResponseBytes = []byte{0x00, 0x00, 0x00, 0x01, 0x00, 0x04, 0x6c, 0x6f, 0x67, 0x73, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x03, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xEE, 0x00, 0x00, 0x00, 0x04, 0x00, 0x02, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xCD}

var invalidTopicsLengthOffsetResponseBytes = []byte{0x00, 0x00, 0x00}
var invalidTopicOffsetResponseBytes = []byte{0x00, 0x00, 0x00, 0x01, 0x00, 0x04, 0x6c, 0x6f}
var invalidPartitionsLengthOffsetResponseBytes = []byte{0x00, 0x00, 0x00, 0x01, 0x00, 0x04, 0x6c, 0x6f, 0x67, 0x73, 0x00, 0x00}
var invalidPartitionOffsetResponseBytes = []byte{0x00, 0x00, 0x00, 0x01, 0x00, 0x04, 0x6c, 0x6f, 0x67, 0x73, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00}
var invalidErrorCodeOffsetResponseBytes = []byte{0x00, 0x00, 0x00, 0x01, 0x00, 0x04, 0x6c, 0x6f, 0x67, 0x73, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x00}
var invalidOffsetsLengthOffsetResponseBytes = []byte{0x00, 0x00, 0x00, 0x01, 0x00, 0x04, 0x6c, 0x6f, 0x67, 0x73, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00}
var invalidOffsetOffsetResponseBytes = []byte{0x00, 0x00, 0x00, 0x01, 0x00, 0x04, 0x6c, 0x6f, 0x67, 0x73, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}

func TestOffsetRequest(t *testing.T) {
	emptyOffsetRequest := new(OffsetRequest)
	testRequest(t, emptyOffsetRequest, emptyOffsetRequestBytes)

	singleOffsetRequest := new(OffsetRequest)
	singleOffsetRequest.AddPartitionOffsetRequestInfo("logs", 2, LatestTime, 1)
	testRequest(t, singleOffsetRequest, singleOffsetRequestBytes)

	multipleOffsetRequest := new(OffsetRequest)
	multipleOffsetRequest.AddPartitionOffsetRequestInfo("logs1", 0, EarliestTime, 1)
	multipleOffsetRequest.AddPartitionOffsetRequestInfo("logs1", 1, EarliestTime, 1)
	testRequest(t, multipleOffsetRequest, multipleOffsetRequestBytes)
}

func TestOffsetResponse(t *testing.T) {
	emptyOffsetResponse := new(OffsetResponse)
	decode(t, emptyOffsetResponse, emptyOffsetResponseBytes)
	assertFatal(t, len(emptyOffsetResponse.PartitionErrorAndOffsets), 0)

	singleOffsetResponse := new(OffsetResponse)
	decode(t, singleOffsetResponse, singleOffsetResponseBytes)
	assertFatal(t, len(singleOffsetResponse.PartitionErrorAndOffsets), 1)
	offsets, exists := singleOffsetResponse.PartitionErrorAndOffsets["logs"]
	assertFatal(t, exists, true)
	assertFatal(t, len(offsets), 1)
	offset, exists := offsets[2]
	assertFatal(t, exists, true)
	assert(t, offset.Error, ErrNoError)
	assertFatal(t, len(offset.Offsets), 1)
	assert(t, offset.Offsets[0], int64(172))

	multipleOffsetResponse := new(OffsetResponse)
	decode(t, multipleOffsetResponse, multipleOffsetResponseBytes)
	assertFatal(t, len(multipleOffsetResponse.PartitionErrorAndOffsets), 1)
	offsets, exists = multipleOffsetResponse.PartitionErrorAndOffsets["logs"]
	assertFatal(t, exists, true)
	assertFatal(t, len(offsets), 2)
	offset1, exists := offsets[3]
	assertFatal(t, exists, true)
	assert(t, offset1.Error, ErrOffsetOutOfRange)
	assertFatal(t, len(offset1.Offsets), 1)
	assert(t, offset1.Offsets[0], int64(238))

	offset2, exists := offsets[4]
	assertFatal(t, exists, true)
	assert(t, offset2.Error, ErrInvalidMessage)
	assertFatal(t, len(offset2.Offsets), 1)
	assert(t, offset2.Offsets[0], int64(205))

	decodeErr(t, new(OffsetResponse), invalidTopicsLengthOffsetResponseBytes, NewDecodingError(ErrEOF, reasonInvalidOffsetsLength))
	decodeErr(t, new(OffsetResponse), invalidTopicOffsetResponseBytes, NewDecodingError(ErrEOF, reasonInvalidOffsetTopic))
	decodeErr(t, new(OffsetResponse), invalidPartitionsLengthOffsetResponseBytes, NewDecodingError(ErrEOF, reasonInvalidPartitionOffsetsLength))
	decodeErr(t, new(OffsetResponse), invalidPartitionOffsetResponseBytes, NewDecodingError(ErrEOF, reasonInvalidPartitionOffsetsPartition))
	decodeErr(t, new(OffsetResponse), invalidErrorCodeOffsetResponseBytes, NewDecodingError(ErrEOF, reasonInvalidPartitionOffsetsErrorCode))
	decodeErr(t, new(OffsetResponse), invalidOffsetsLengthOffsetResponseBytes, NewDecodingError(ErrEOF, reasonInvalidPartitionOffsetsOffsetsLength))
	decodeErr(t, new(OffsetResponse), invalidOffsetOffsetResponseBytes, NewDecodingError(ErrEOF, reasonInvalidPartitionOffset))
}
