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

import (
	"math/rand"
	"testing"
)

var numValues = 100

func TestInt8EncodingDecoding(t *testing.T) {
	buffer := make([]byte, numValues)
	encoder := NewBinaryEncoder(buffer)

	randValues := make([]int8, numValues)
	for i := 0; i < numValues; i++ {
		randValue := int8(rand.Int())
		encoder.WriteInt8(randValue)
		randValues[i] = randValue
	}

	decoder := NewBinaryDecoder(buffer)
	for i := 0; i < numValues; i++ {
		value, err := decoder.GetInt8()
		checkErr(t, err)
		assert(t, value, randValues[i])
	}

	bytes := []byte{}
	decoder = NewBinaryDecoder(bytes)
	_, err := decoder.GetInt8()
	assert(t, err, ErrEOF)
}

func TestInt16EncodingDecoding(t *testing.T) {
	buffer := make([]byte, numValues*2)
	encoder := NewBinaryEncoder(buffer)

	randValues := make([]int16, numValues)
	for i := 0; i < numValues; i++ {
		randValue := int16(rand.Int())
		encoder.WriteInt16(randValue)
		randValues[i] = randValue
	}

	decoder := NewBinaryDecoder(buffer)
	for i := 0; i < numValues; i++ {
		value, err := decoder.GetInt16()
		checkErr(t, err)
		assert(t, value, randValues[i])
	}

	bytes := []byte{0x00}
	decoder = NewBinaryDecoder(bytes)
	_, err := decoder.GetInt16()
	assert(t, err, ErrEOF)
}

func TestInt32EncodingDecoding(t *testing.T) {
	buffer := make([]byte, numValues*4)
	encoder := NewBinaryEncoder(buffer)

	randValues := make([]int32, numValues)
	for i := 0; i < numValues; i++ {
		randValue := int32(rand.Int())
		encoder.WriteInt32(randValue)
		randValues[i] = randValue
	}

	decoder := NewBinaryDecoder(buffer)
	for i := 0; i < numValues; i++ {
		value, err := decoder.GetInt32()
		checkErr(t, err)
		assert(t, value, randValues[i])
	}

	bytes := []byte{0x00, 0x00, 0x00}
	decoder = NewBinaryDecoder(bytes)
	_, err := decoder.GetInt32()
	assert(t, err, ErrEOF)
}

func TestInt64EncodingDecoding(t *testing.T) {
	buffer := make([]byte, numValues*8)
	encoder := NewBinaryEncoder(buffer)

	randValues := make([]int64, numValues)
	for i := 0; i < numValues; i++ {
		randValue := int64(rand.Int())
		encoder.WriteInt64(randValue)
		randValues[i] = randValue
	}

	decoder := NewBinaryDecoder(buffer)
	for i := 0; i < numValues; i++ {
		value, err := decoder.GetInt64()
		checkErr(t, err)
		assert(t, value, randValues[i])
	}

	bytes := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	decoder = NewBinaryDecoder(bytes)
	_, err := decoder.GetInt64()
	assert(t, err, ErrEOF)
}

func TestStringEncodingDecoding(t *testing.T) {
	buffer := make([]byte, (numValues+1)*2+((numValues*(numValues+1))/2))
	encoder := NewBinaryEncoder(buffer)

	randValues := make([]string, numValues+1)
	for i := 0; i <= numValues; i++ {
		randValue := randomString(i)
		encoder.WriteString(randValue)
		randValues[i] = randValue
	}

	decoder := NewBinaryDecoder(buffer)
	for i := 0; i <= numValues; i++ {
		value, err := decoder.GetString()
		checkErr(t, err)
		assert(t, value, randValues[i])
	}

	bytesNegativeLength := []byte{0xFF, 0xFE}
	decoder = NewBinaryDecoder(bytesNegativeLength)
	_, err := decoder.GetString()
	assert(t, err, ErrEOF)

	bytesInsufficientData := []byte{0x00, 0x03, 0x6C}
	decoder = NewBinaryDecoder(bytesInsufficientData)
	_, err = decoder.GetString()
	assert(t, err, ErrEOF)
}

func TestBytesEncodingDecoding(t *testing.T) {
	buffer := make([]byte, numValues*4+((numValues*(numValues+1))/2))
	encoder := NewBinaryEncoder(buffer)

	randValues := make([][]byte, numValues)
	for i := 0; i < numValues; i++ {
		randValue := randomBytes(i)
		encoder.WriteBytes(randValue)
		randValues[i] = randValue
	}

	decoder := NewBinaryDecoder(buffer)
	for i := 0; i < numValues; i++ {
		value, err := decoder.GetBytes()
		checkErr(t, err)
		assert(t, value, randValues[i])
	}

	nilBytes := []byte{0xFF, 0xFF, 0xFF, 0xFF}
	decoder = NewBinaryDecoder(nilBytes)
	bytes, err := decoder.GetBytes()
	checkErr(t, err)
	assert(t, bytes, ([]byte)(nil))

	bytesNegativeLength := []byte{0xFF, 0xFF, 0xFF, 0xFE}
	decoder = NewBinaryDecoder(bytesNegativeLength)
	_, err = decoder.GetBytes()
	assert(t, err, ErrEOF)

	bytesInsufficientData := []byte{0x00, 0x00, 0x00, 0x03, 0x6C}
	decoder = NewBinaryDecoder(bytesInsufficientData)
	_, err = decoder.GetBytes()
	assert(t, err, ErrEOF)
}

func TestEncoderSize(t *testing.T) {
	encoder := NewBinaryEncoder(make([]byte, 8))
	assert(t, encoder.Size(), int32(8))
}

func TestEncoderReserve(t *testing.T) {
	expectingEncodedLength := []byte{0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x0C, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01}
	writeLength := func(encoder Encoder) {
		encoder.WriteInt32(1)
		encoder.Reserve(&LengthSlice{})
		encoder.WriteInt64(1)
		encoder.WriteInt32(1)
		encoder.UpdateReserved()
		encoder.WriteInt32(1)
	}

	expectingEncodedCRC := []byte{0x00, 0x00, 0x00, 0x01, 0x31, 0xB2, 0xDF, 0x49, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01}
	writeCRC := func(encoder Encoder) {
		encoder.WriteInt32(1)
		encoder.Reserve(&CrcSlice{})
		encoder.WriteInt64(1)
		encoder.WriteInt32(1)
		encoder.UpdateReserved()
		encoder.WriteInt32(1)
	}

	sizer := NewSizingEncoder()
	writeLength(sizer)
	size := sizer.Size()
	assert(t, size, int32(4+4+8+4+4))

	bytes := make([]byte, size)
	encoder := NewBinaryEncoder(bytes)
	writeLength(encoder)
	assert(t, bytes, expectingEncodedLength)

	bytes = make([]byte, size)
	encoder = NewBinaryEncoder(bytes)
	writeCRC(encoder)
	assert(t, bytes, expectingEncodedCRC)
}

func TestSizingEncoder(t *testing.T) {
	int8encoder := NewSizingEncoder()
	for i := 0; i < numValues; i++ {
		int8encoder.WriteInt8(0)
	}
	assert(t, int(int8encoder.Size()), numValues)

	int16encoder := NewSizingEncoder()
	for i := 0; i < numValues; i++ {
		int16encoder.WriteInt16(0)
	}
	assert(t, int(int16encoder.Size()), numValues*2)

	int32encoder := NewSizingEncoder()
	for i := 0; i < numValues; i++ {
		int32encoder.WriteInt32(0)
	}
	assert(t, int(int32encoder.Size()), numValues*4)

	int64encoder := NewSizingEncoder()
	for i := 0; i < numValues; i++ {
		int64encoder.WriteInt64(0)
	}
	assert(t, int(int64encoder.Size()), numValues*8)

	stringEncoder := NewSizingEncoder()
	for i := 0; i <= numValues; i++ {
		stringEncoder.WriteString(randomString(i))
	}
	//we encode N strings with length from 0 to N, so the Size() should return (numValues+1)*2 which is size for int16 string lengths including the empty one
	//and N*(N+1)/2 for actual string values
	assert(t, int(stringEncoder.Size()), (numValues+1)*2+((numValues*(numValues+1))/2))

	bytesEncoder := NewSizingEncoder()
	for i := 0; i <= numValues; i++ {
		bytesEncoder.WriteBytes(randomBytes(i))
	}
	//we encode N arrays with length from 0 to N, so the Size() should return (numValues+1)*4 which is size for int32 arrays lengths including the empty one
	//and N*(N+1)/2 for actual arrays
	assert(t, int(bytesEncoder.Size()), (numValues+1)*4+((numValues*(numValues+1))/2))
}
