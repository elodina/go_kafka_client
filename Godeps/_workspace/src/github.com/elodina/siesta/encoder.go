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
	"hash/crc32"
)

// Encoder is able to encode actual data into a Kafka wire protocol byte sequence.
type Encoder interface {
	// Writes an int8 to this encoder.
	WriteInt8(int8)

	// Writes an int16 to this encoder.
	WriteInt16(int16)

	// Writes an int32 to this encoder.
	WriteInt32(int32)

	// Writes an int64 to this encoder.
	WriteInt64(int64)

	// Writes a []byte to this encoder.
	WriteBytes([]byte)

	// Writes a string to this encoder.
	WriteString(string)

	// Returns the size in bytes written to this encoder.
	Size() int32

	// Reserves a place for an updatable slice.
	// This is used as an optimization for length and crc fields.
	// The encoder reserves a place for this data and updates it later instead of pre-calculating it and doing redundant work.
	Reserve(UpdatableSlice)

	// Tells the last reserved slice to be updated with new data.
	UpdateReserved()
}

// BinaryEncoder implements Decoder and is able to encode actual data into a Kafka wire protocol byte sequence.
type BinaryEncoder struct {
	buffer []byte
	pos    int

	stack []UpdatableSlice
}

// NewBinaryEncoder creates a new BinaryEncoder that will write into a given []byte.
func NewBinaryEncoder(buffer []byte) *BinaryEncoder {
	return &BinaryEncoder{
		buffer: buffer,
		stack:  make([]UpdatableSlice, 0),
	}
}

// WriteInt8 writes an int8 to this encoder.
func (be *BinaryEncoder) WriteInt8(value int8) {
	be.buffer[be.pos] = byte(value)
	be.pos++
}

// WriteInt16 writes an int16 to this encoder.
func (be *BinaryEncoder) WriteInt16(value int16) {
	binary.BigEndian.PutUint16(be.buffer[be.pos:], uint16(value))
	be.pos += 2
}

// WriteInt32 writes an int32 to this encoder.
func (be *BinaryEncoder) WriteInt32(value int32) {
	binary.BigEndian.PutUint32(be.buffer[be.pos:], uint32(value))
	be.pos += 4
}

// WriteInt64 writes an int64 to this encoder.
func (be *BinaryEncoder) WriteInt64(value int64) {
	binary.BigEndian.PutUint64(be.buffer[be.pos:], uint64(value))
	be.pos += 8
}

// WriteString writes a string to this encoder.
func (be *BinaryEncoder) WriteString(value string) {
	be.WriteInt16(int16(len(value)))
	copy(be.buffer[be.pos:], value)
	be.pos += len(value)
}

// WriteBytes writes a []byte to this encoder.
func (be *BinaryEncoder) WriteBytes(value []byte) {
	be.WriteInt32(int32(len(value)))
	copy(be.buffer[be.pos:], value)
	be.pos += len(value)
}

// Size returns the size in bytes written to this encoder.
func (be *BinaryEncoder) Size() int32 {
	return int32(len(be.buffer))
}

// Reserve reserves a place for an updatable slice.
func (be *BinaryEncoder) Reserve(slice UpdatableSlice) {
	slice.SetPosition(be.pos)
	be.stack = append(be.stack, slice)
	be.pos += slice.GetReserveLength()
}

// UpdateReserved tells the last reserved slice to be updated with new data.
func (be *BinaryEncoder) UpdateReserved() {
	stackLength := len(be.stack) - 1
	slice := be.stack[stackLength]
	be.stack = be.stack[:stackLength]

	slice.Update(be.buffer[slice.GetPosition():be.pos])
}

// SizingEncoder is used to determine the size for []byte that will hold the actual encoded data.
// This is used as an optimization as it is cheaper to run once and determine the size instead of growing the slice dynamically.
type SizingEncoder struct {
	size int
}

// NewSizingEncoder creates a new SizingEncoder
func NewSizingEncoder() *SizingEncoder {
	return &SizingEncoder{}
}

// WriteInt8 writes an int8 to this encoder.
func (se *SizingEncoder) WriteInt8(int8) {
	se.size++
}

// WriteInt16 writes an int16 to this encoder.
func (se *SizingEncoder) WriteInt16(int16) {
	se.size += 2
}

// WriteInt32 writes an int32 to this encoder.
func (se *SizingEncoder) WriteInt32(int32) {
	se.size += 4
}

// WriteInt64 writes an int64 to this encoder.
func (se *SizingEncoder) WriteInt64(int64) {
	se.size += 8
}

// WriteString writes a string to this encoder.
func (se *SizingEncoder) WriteString(value string) {
	se.WriteInt16(int16(len(value)))
	se.size += len(value)
}

// WriteBytes writes a []byte to this encoder.
func (se *SizingEncoder) WriteBytes(value []byte) {
	se.WriteInt32(int32(len(value)))
	se.size += len(value)
}

// Size returns the size in bytes written to this encoder.
func (se *SizingEncoder) Size() int32 {
	return int32(se.size)
}

// Reserve reserves a place for an updatable slice.
func (se *SizingEncoder) Reserve(slice UpdatableSlice) {
	se.size += slice.GetReserveLength()
}

// UpdateReserved tells the last reserved slice to be updated with new data.
func (se *SizingEncoder) UpdateReserved() {
	//do nothing
}

// UpdatableSlice is an interface that is used when the encoder has to write the value based on bytes that are not yet written (e.g. calculate the CRC of the message).
type UpdatableSlice interface {
	// Returns the length to reserve for this slice.
	GetReserveLength() int

	// Set the current position within the encoder to be updated later.
	SetPosition(int)

	// Get the position within the encoder to be updated later.
	GetPosition() int

	// Update this slice. At this point all necessary data should be written to encoder.
	Update([]byte)
}

// LengthSlice is used to determine the length of upcoming message.
type LengthSlice struct {
	pos   int
	slice []byte
}

// GetReserveLength returns the length to reserve for this slice.
func (ls *LengthSlice) GetReserveLength() int {
	return 4
}

// SetPosition sets the current position within the encoder to be updated later.
func (ls *LengthSlice) SetPosition(pos int) {
	ls.pos = pos
}

// GetPosition gets the position within the encoder to be updated later.
func (ls *LengthSlice) GetPosition() int {
	return ls.pos
}

// Update this slice. At this point all necessary data should be written to encoder.
func (ls *LengthSlice) Update(slice []byte) {
	binary.BigEndian.PutUint32(slice, uint32(len(slice)-ls.GetReserveLength()))
}

// CrcSlice is used to calculate the CRC32 value of the message.
type CrcSlice struct {
	pos int
}

// GetReserveLength returns the length to reserve for this slice.
func (cs *CrcSlice) GetReserveLength() int {
	return 4
}

// SetPosition sets the current position within the encoder to be updated later.
func (cs *CrcSlice) SetPosition(pos int) {
	cs.pos = pos
}

// GetPosition gets the position within the encoder to be updated later.
func (cs *CrcSlice) GetPosition() int {
	return cs.pos
}

// Update this slice. At this point all necessary data should be written to encoder.
func (cs *CrcSlice) Update(slice []byte) {
	//TODO https://github.com/Shopify/sarama/issues/255 - maybe port the mentioned CRC algo?
	crc := crc32.ChecksumIEEE(slice[cs.GetReserveLength():])
	binary.BigEndian.PutUint32(slice, crc)
}
