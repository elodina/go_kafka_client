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
	"encoding/binary"
)

// Decoder is able to decode a Kafka wire protocol message into actual data.
type Decoder interface {
	// Gets an int8 from this decoder. Returns EOF if end of stream is reached.
	GetInt8() (int8, error)

	// Gets an int16 from this decoder. Returns EOF if end of stream is reached.
	GetInt16() (int16, error)

	// Gets an int32 from this decoder. Returns EOF if end of stream is reached.
	GetInt32() (int32, error)

	// Gets an int64 from this decoder. Returns EOF if end of stream is reached.
	GetInt64() (int64, error)

	// Gets a []byte from this decoder. Returns EOF if end of stream is reached.
	GetBytes() ([]byte, error)

	// Gets a string from this decoder. Returns EOF if end of stream is reached.
	GetString() (string, error)

	// Tells how many bytes left unread in this decoder.
	Remaining() int
}

// BinaryDecoder implements Decoder and is able to decode a Kafka wire protocol message into actual data.
type BinaryDecoder struct {
	raw []byte
	pos int
}

// NewBinaryDecoder creates a new BinaryDecoder that will decode a given []byte.
func NewBinaryDecoder(raw []byte) *BinaryDecoder {
	return &BinaryDecoder{
		raw: raw,
	}
}

// GetInt8 gets an int8 from this decoder. Returns EOF if end of stream is reached.
func (bd *BinaryDecoder) GetInt8() (int8, error) {
	if bd.Remaining() < 1 {
		bd.pos = len(bd.raw)
		return -1, ErrEOF
	}
	value := int8(bd.raw[bd.pos])
	bd.pos++
	return value, nil
}

// GetInt16 gets an int16 from this decoder. Returns EOF if end of stream is reached.
func (bd *BinaryDecoder) GetInt16() (int16, error) {
	if bd.Remaining() < 2 {
		bd.pos = len(bd.raw)
		return -1, ErrEOF
	}
	value := int16(binary.BigEndian.Uint16(bd.raw[bd.pos:]))
	bd.pos += 2
	return value, nil
}

// GetInt32 gets an int32 from this decoder. Returns EOF if end of stream is reached.
func (bd *BinaryDecoder) GetInt32() (int32, error) {
	if bd.Remaining() < 4 {
		bd.pos = len(bd.raw)
		return -1, ErrEOF
	}
	value := int32(binary.BigEndian.Uint32(bd.raw[bd.pos:]))
	bd.pos += 4
	return value, nil
}

// GetInt64 gets an int64 from this decoder. Returns EOF if end of stream is reached.
func (bd *BinaryDecoder) GetInt64() (int64, error) {
	if bd.Remaining() < 8 {
		bd.pos = len(bd.raw)
		return -1, ErrEOF
	}
	value := int64(binary.BigEndian.Uint64(bd.raw[bd.pos:]))
	bd.pos += 8
	return value, nil
}

// GetBytes gets a []byte from this decoder. Returns EOF if end of stream is reached.
func (bd *BinaryDecoder) GetBytes() ([]byte, error) {
	l, err := bd.GetInt32()

	if err != nil || l < -1 {
		return nil, ErrEOF
	}

	length := int(l)

	switch {
	case length == -1:
		return nil, nil
	case length == 0:
		return make([]byte, 0), nil
	case length > bd.Remaining():
		bd.pos = len(bd.raw)
		return nil, ErrEOF
	}
	value := bd.raw[bd.pos : bd.pos+length]
	bd.pos += length
	return value, nil
}

// GetString gets a string from this decoder. Returns EOF if end of stream is reached.
func (bd *BinaryDecoder) GetString() (string, error) {
	l, err := bd.GetInt16()

	if err != nil || l < -1 {
		return "", ErrEOF
	}

	length := int(l)

	switch {
	case length < 1:
		return "", nil
	case length > bd.Remaining():
		bd.pos = len(bd.raw)
		return "", ErrEOF
	}
	value := string(bd.raw[bd.pos : bd.pos+length])
	bd.pos += length
	return value, nil
}

// Remaining tells how many bytes left unread in this decoder.
func (bd *BinaryDecoder) Remaining() int {
	return len(bd.raw) - bd.pos
}
