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
	"bytes"
	"compress/gzip"
	"io/ioutil"
)

// CompressionCodec is a compression codec id used to distinguish various compression types.
type CompressionCodec int

const (
	// CompressionNone is a compression codec id for uncompressed data.
	CompressionNone CompressionCodec = 0

	// CompressionGZIP is a compression codec id for GZIP compression.
	CompressionGZIP CompressionCodec = 1

	// CompressionSnappy is a compression codec id for Snappy compression.
	CompressionSnappy CompressionCodec = 2

	// CompressionLZ4 is a compression codec id for LZ4 compression.
	CompressionLZ4 CompressionCodec = 3
)

const compressionCodecMask int8 = 3

// MessageAndOffset is a single message or a message set (if it is compressed) with its offset value.
type MessageAndOffset struct {
	Offset  int64
	Message *Message
}

func (mo *MessageAndOffset) Read(decoder Decoder) *DecodingError {
	offset, err := decoder.GetInt64()
	if err != nil {
		return NewDecodingError(err, reasonInvalidMessageAndOffsetOffset)
	}
	mo.Offset = offset

	_, err = decoder.GetInt32()
	if err != nil {
		return NewDecodingError(err, reasonInvalidMessageLength)
	}

	message := new(Message)
	decodingErr := message.Read(decoder)
	if decodingErr != nil {
		return decodingErr
	}
	mo.Message = message

	return nil
}

func (mo *MessageAndOffset) Write(encoder Encoder) {
	encoder.WriteInt64(mo.Offset)
	encoder.Reserve(&LengthSlice{})
	mo.Message.Write(encoder)
	encoder.UpdateReserved()
}

// ReadMessageSet decodes a nested message set if the MessageAndOffset is compressed.
func ReadMessageSet(decoder Decoder) ([]*MessageAndOffset, *DecodingError) {
	var messages []*MessageAndOffset
	for decoder.Remaining() > 0 {
		messageAndOffset := new(MessageAndOffset)
		err := messageAndOffset.Read(decoder)
		if err != nil {
			if err.Error() != ErrEOF {
				return nil, err
			}
			continue
		}
		messages = append(messages, messageAndOffset)
	}

	return messages, nil
}

// Message contains a single message and its metadata or a nested message set if compression is used.
type Message struct {
	Crc        int32
	MagicByte  int8
	Attributes int8
	Key        []byte
	Value      []byte

	Nested []*MessageAndOffset
}

func (md *Message) Read(decoder Decoder) *DecodingError {
	crc, err := decoder.GetInt32()
	if err != nil {
		return NewDecodingError(err, reasonInvalidMessageCRC)
	}
	md.Crc = crc

	magic, err := decoder.GetInt8()
	if err != nil {
		return NewDecodingError(err, reasonInvalidMessageMagicByte)
	}
	md.MagicByte = magic

	attributes, err := decoder.GetInt8()
	if err != nil {
		return NewDecodingError(err, reasonInvalidMessageAttributes)
	}
	md.Attributes = attributes

	key, err := decoder.GetBytes()
	if err != nil {
		return NewDecodingError(err, reasonInvalidMessageKey)
	}
	md.Key = key

	value, err := decoder.GetBytes()
	if err != nil {
		return NewDecodingError(err, reasonInvalidMessageValue)
	}
	md.Value = value

	compressionCodec := CompressionCodec(md.Attributes & compressionCodecMask)
	switch compressionCodec {
	case CompressionNone:
	case CompressionGZIP:
		{
			if md.Value == nil {
				return NewDecodingError(ErrNoDataToUncompress, reasonNoGzipData)
			}
			reader, err := gzip.NewReader(bytes.NewReader(md.Value))
			if err != nil {
				return NewDecodingError(err, reasonMalformedGzipData)
			}
			if md.Value, err = ioutil.ReadAll(reader); err != nil {
				return NewDecodingError(err, reasonMalformedGzipData)
			}

			messages, decodingErr := ReadMessageSet(NewBinaryDecoder(md.Value))
			if decodingErr != nil {
				return decodingErr
			}
			md.Nested = messages
		}
	case CompressionSnappy:
		{
			unsnappied, err := snappyDecode(md.Value)
			if err != nil {
				return NewDecodingError(err, reasonMalformedSnappyData)
			}
			messages, decodingErr := ReadMessageSet(NewBinaryDecoder(unsnappied))
			if decodingErr != nil {
				return decodingErr
			}
			md.Nested = messages
		}
	case CompressionLZ4:
		panic("Not implemented yet")
	}

	return nil
}

//TODO compress and write if needed
func (md *Message) Write(encoder Encoder) {
	encoder.Reserve(&CrcSlice{})
	encoder.WriteInt8(md.MagicByte)
	encoder.WriteInt8(md.Attributes)
	encoder.WriteBytes(md.Key)
	encoder.WriteBytes(md.Value)
	encoder.UpdateReserved()
}

// MessageAndMetadata is a single message and its metadata.
type MessageAndMetadata struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
}

var (
	reasonInvalidMessageAndOffsetOffset = "Invalid offset in MessageAndOffset"
	reasonInvalidMessageLength          = "Invalid Message length"
	reasonInvalidMessageCRC             = "Invalid Message CRC"
	reasonInvalidMessageMagicByte       = "Invalid Message magic byte"
	reasonInvalidMessageAttributes      = "Invalid Message attributes"
	reasonInvalidMessageKey             = "Invalid Message key"
	reasonInvalidMessageValue           = "Invalid Message value"
	reasonNoGzipData                    = "No data to uncompress for GZip encoded message"
	reasonMalformedGzipData             = "Malformed GZip encoded message"
	reasonMalformedSnappyData           = "Malformed Snappy encoded message"
)
