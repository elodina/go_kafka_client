package avro

import (
	"encoding/binary"
	"io"
	"math"
)

// Encoder is an interface that provides low-level support for serializing Avro values.
type Encoder interface {
	// Writes a null value. Doesn't actually do anything but may advance the state of Encoder implementation if it
	// is stateful.
	WriteNull(interface{})

	// Writes a boolean value.
	WriteBoolean(bool)

	// Writes an int value.
	WriteInt(int32)

	// Writes a long value.
	WriteLong(int64)

	// Writes a float value.
	WriteFloat(float32)

	// Writes a double value.
	WriteDouble(float64)

	// Writes a bytes value.
	WriteBytes([]byte)

	// Writes a string value.
	WriteString(string)

	// WriteArrayStart should be called when starting to serialize an array providing it with a number of items in
	// array block.
	WriteArrayStart(int64)

	// WriteArrayNext should be called after finishing writing an array block either passing it the number of items in
	// next block or 0 indicating the end of array.
	WriteArrayNext(int64)

	// WriteMapStart should be called when starting to serialize a map providing it with a number of items in
	// map block.
	WriteMapStart(int64)

	// WriteMapNext should be called after finishing writing a map block either passing it the number of items in
	// next block or 0 indicating the end of map.
	WriteMapNext(int64)

	// Writes raw bytes to this Encoder.
	WriteRaw([]byte)
}

// BinaryEncoder implements Encoder and provides low-level support for serializing Avro values.
type BinaryEncoder struct {
	buffer io.Writer
}

// Creates a new BinaryEncoder that will write to a given io.Writer.
func NewBinaryEncoder(buffer io.Writer) *BinaryEncoder {
	return &BinaryEncoder{buffer: buffer}
}

// Writes a null value. Doesn't actually do anything in this implementation.
func (this *BinaryEncoder) WriteNull(_ interface{}) {
	//do nothing
}

// Writes a boolean value.
func (this *BinaryEncoder) WriteBoolean(x bool) {
	if x {
		this.buffer.Write([]byte{0x01})
	} else {
		this.buffer.Write([]byte{0x00})
	}
}

// Writes an int value.
func (this *BinaryEncoder) WriteInt(x int32) {
	this.buffer.Write(this.encodeVarint32(x))
}

// Writes a long value.
func (this *BinaryEncoder) WriteLong(x int64) {
	this.buffer.Write(this.encodeVarint64(x))
}

// Writes a float value.
func (this *BinaryEncoder) WriteFloat(x float32) {
	bytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(bytes, math.Float32bits(x))
	this.buffer.Write(bytes)
}

// Writes a double value.
func (this *BinaryEncoder) WriteDouble(x float64) {
	bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytes, math.Float64bits(x))
	this.buffer.Write(bytes)
}

// Writes raw bytes to this Encoder.
func (this *BinaryEncoder) WriteRaw(x []byte) {
	this.buffer.Write(x)
}

// Writes a bytes value.
func (this *BinaryEncoder) WriteBytes(x []byte) {
	this.WriteLong(int64(len(x)))
	this.buffer.Write(x)
}

// Writes a string value.
func (this *BinaryEncoder) WriteString(x string) {
	this.WriteLong(int64(len(x)))
	this.buffer.Write([]byte(x))
}

// WriteArrayNext should be called after finishing writing an array block either passing it the number of items in
// next block or 0 indicating the end of array.
func (this *BinaryEncoder) WriteArrayStart(count int64) {
	this.writeItemCount(count)
}

// WriteArrayNext should be called after finishing writing an array block either passing it the number of items in
// next block or 0 indicating the end of array.
func (this *BinaryEncoder) WriteArrayNext(count int64) {
	this.writeItemCount(count)
}

// WriteMapStart should be called when starting to serialize a map providing it with a number of items in
// map block.
func (this *BinaryEncoder) WriteMapStart(count int64) {
	this.writeItemCount(count)
}

// WriteMapNext should be called after finishing writing a map block either passing it the number of items in
// next block or 0 indicating the end of map.
func (this *BinaryEncoder) WriteMapNext(count int64) {
	this.writeItemCount(count)
}

func (this *BinaryEncoder) writeItemCount(count int64) {
	this.WriteLong(count)
}

func (this *BinaryEncoder) encodeVarint32(n int32) []byte {
	var buf [5]byte
	ux := uint32(n) << 1
	if n < 0 {
		ux = ^ux
	}
	i := 0
	for ux >= 0x80 {
		buf[i] = byte(ux) | 0x80
		ux >>= 7
		i++
	}
	buf[i] = byte(ux)

	return buf[0 : i+1]
}

func (this *BinaryEncoder) encodeVarint64(x int64) []byte {
	var buf [10]byte
	ux := uint64(x) << 1
	if x < 0 {
		ux = ^ux
	}
	i := 0
	for ux >= 0x80 {
		buf[i] = byte(ux) | 0x80
		ux >>= 7
		i++
	}
	buf[i] = byte(ux)

	return buf[0 : i+1]
}
