package avro

import (
	"encoding/binary"
	"math"
)

// Decoder is an interface that provides low-level support for deserializing Avro values.
type Decoder interface {
	// Reads a null value. Returns a decoded value and an error if it occurs.
	ReadNull() (interface{}, error)

	// Reads a boolean value. Returns a decoded value and an error if it occurs.
	ReadBoolean() (bool, error)

	// Reads an in value. Returns a decoded value and an error if it occurs.
	ReadInt() (int32, error)

	// Reads a long value. Returns a decoded value and an error if it occurs.
	ReadLong() (int64, error)

	// Reads a float value. Returns a decoded value and an error if it occurs.
	ReadFloat() (float32, error)

	// Reads a double value. Returns a decoded value and an error if it occurs.
	ReadDouble() (float64, error)

	// Reads a bytes value. Returns a decoded value and an error if it occurs.
	ReadBytes() ([]byte, error)

	// Reads a string value. Returns a decoded value and an error if it occurs.
	ReadString() (string, error)

	// Reads an enum value (which is an Avro int value). Returns a decoded value and an error if it occurs.
	ReadEnum() (int32, error)

	// Reads and returns the size of the first block of an array. If call to this return non-zero, then the caller
	// should read the indicated number of items and then call ArrayNext() to find out the number of items in the
	// next block. Returns a decoded value and an error if it occurs.
	ReadArrayStart() (int64, error)

	// Processes the next block of an array and returns the number of items in the block.
	// Returns a decoded value and an error if it occurs.
	ArrayNext() (int64, error)

	// Reads and returns the size of the first block of map entries. If call to this return non-zero, then the caller
	// should read the indicated number of items and then call MapNext() to find out the number of items in the
	// next block. Usage is similar to ReadArrayStart(). Returns a decoded value and an error if it occurs.
	ReadMapStart() (int64, error)

	// Processes the next block of map entries and returns the number of items in the block.
	// Returns a decoded value and an error if it occurs.
	MapNext() (int64, error)

	// Reads fixed sized binary object into the provided buffer.
	// Returns an error if it occurs.
	ReadFixed([]byte) error

	// Reads fixed sized binary object into the provided buffer.
	// The second parameter is the position where the data needs to be written, the third is the size of binary object.
	// Returns an error if it occurs.
	ReadFixedWithBounds([]byte, int, int) error

	// SetBlock is used for Avro Object Container Files where the data is split in blocks and sets a data block
	// for this decoder and sets the position to the start of this block.
	SetBlock(*DataBlock)

	// Seek sets the reading position of this Decoder to a given value allowing to skip items etc.
	Seek(int64)

	// Tell returns the current reading position of this Decoder.
	Tell() int64
}

// DataBlock is a structure that holds a certain amount of entries and the actual buffer to read from.
type DataBlock struct {
	// Actual data
	Data []byte

	// Number of entries encoded in Data.
	NumEntries int64

	// Size of data buffer in bytes.
	BlockSize int

	// Number of unread entries in this DataBlock.
	BlockRemaining int64
}

var max_int_buf_size = 5
var max_long_buf_size = 10

// BinaryDecoder implements Decoder and provides low-level support for deserializing Avro values.
type BinaryDecoder struct {
	buf []byte
	pos int64
}

// Creates a new BinaryDecoder to read from a given buffer.
func NewBinaryDecoder(buf []byte) *BinaryDecoder {
	return &BinaryDecoder{buf, 0}
}

// Reads a null value. Returns a decoded value and an error if it occurs.
func (this *BinaryDecoder) ReadNull() (interface{}, error) {
	return nil, nil
}

// Reads an int value. Returns a decoded value and an error if it occurs.
func (this *BinaryDecoder) ReadInt() (int32, error) {
	if err := checkEOF(this.buf, this.pos, 1); err != nil {
		return 0, EOF
	}
	var value uint32
	var b uint8
	var offset int
	bufLen := int64(len(this.buf))

	for {
		if offset == max_int_buf_size {
			return 0, IntOverflow
		}

		if this.pos >= bufLen {
			return 0, InvalidInt
		}

		b = this.buf[this.pos]
		value |= uint32(b&0x7F) << uint(7*offset)
		this.pos++
		offset++
		if b&0x80 == 0 {
			break
		}
	}
	return int32((value >> 1) ^ -(value & 1)), nil
}

// Reads a long value. Returns a decoded value and an error if it occurs.
func (this *BinaryDecoder) ReadLong() (int64, error) {
	var value uint64
	var b uint8
	var offset int
	bufLen := int64(len(this.buf))

	for {
		if offset == max_long_buf_size {
			return 0, LongOverflow
		}

		if this.pos >= bufLen {
			return 0, InvalidLong
		}

		b = this.buf[this.pos]
		value |= uint64(b&0x7F) << uint(7*offset)
		this.pos++
		offset++

		if b&0x80 == 0 {
			break
		}
	}
	return int64((value >> 1) ^ -(value & 1)), nil
}

// Reads a string value. Returns a decoded value and an error if it occurs.
func (this *BinaryDecoder) ReadString() (string, error) {
	if err := checkEOF(this.buf, this.pos, 1); err != nil {
		return "", err
	}
	length, err := this.ReadLong()
	if err != nil || length < 0 {
		return "", InvalidStringLength
	}
	if err := checkEOF(this.buf, this.pos, int(length)); err != nil {
		return "", err
	}
	value := string(this.buf[this.pos : this.pos+length])
	this.pos += length
	return value, nil
}

// Reads a boolean value. Returns a decoded value and an error if it occurs.
func (this *BinaryDecoder) ReadBoolean() (bool, error) {
	b := this.buf[this.pos] & 0xFF
	this.pos++
	var err error = nil
	if b != 0 && b != 1 {
		err = InvalidBool
	}
	return b == 1, err
}

// Reads a bytes value. Returns a decoded value and an error if it occurs.
func (this *BinaryDecoder) ReadBytes() ([]byte, error) {
	//TODO make something with these if's!!
	if err := checkEOF(this.buf, this.pos, 1); err != nil {
		return nil, EOF
	}
	length, err := this.ReadLong()
	if err != nil {
		return nil, err
	}
	if length < 0 {
		return nil, NegativeBytesLength
	}
	if err := checkEOF(this.buf, this.pos, int(length)); err != nil {
		return nil, EOF
	}

	bytes := make([]byte, length)
	copy(bytes[:], this.buf[this.pos:this.pos+length])
	this.pos += length
	return bytes, err
}

// Reads a float value. Returns a decoded value and an error if it occurs.
func (this *BinaryDecoder) ReadFloat() (float32, error) {
	var float float32
	if err := checkEOF(this.buf, this.pos, 4); err != nil {
		return float, err
	}
	bits := binary.LittleEndian.Uint32(this.buf[this.pos : this.pos+4])
	float = math.Float32frombits(bits)
	this.pos += 4
	return float, nil
}

// Reads a double value. Returns a decoded value and an error if it occurs.
func (this *BinaryDecoder) ReadDouble() (float64, error) {
	var double float64
	if err := checkEOF(this.buf, this.pos, 8); err != nil {
		return double, err
	}
	bits := binary.LittleEndian.Uint64(this.buf[this.pos : this.pos+8])
	double = math.Float64frombits(bits)
	this.pos += 8
	return double, nil
}

// Reads an enum value (which is an Avro int value). Returns a decoded value and an error if it occurs.
func (this *BinaryDecoder) ReadEnum() (int32, error) {
	return this.ReadInt()
}

// Reads and returns the size of the first block of an array. If call to this return non-zero, then the caller
// should read the indicated number of items and then call ArrayNext() to find out the number of items in the
// next block. Returns a decoded value and an error if it occurs.
func (this *BinaryDecoder) ReadArrayStart() (int64, error) {
	return this.readItemCount()
}

// Processes the next block of an array and returns the number of items in the block.
// Returns a decoded value and an error if it occurs.
func (this *BinaryDecoder) ArrayNext() (int64, error) {
	return this.readItemCount()
}

// Reads and returns the size of the first block of map entries. If call to this return non-zero, then the caller
// should read the indicated number of items and then call MapNext() to find out the number of items in the
// next block. Usage is similar to ReadArrayStart(). Returns a decoded value and an error if it occurs.
func (this *BinaryDecoder) ReadMapStart() (int64, error) {
	return this.readItemCount()
}

// Processes the next block of map entries and returns the number of items in the block.
// Returns a decoded value and an error if it occurs.
func (this *BinaryDecoder) MapNext() (int64, error) {
	return this.readItemCount()
}

// Reads fixed sized binary object into the provided buffer.
// Returns an error if it occurs.
func (this *BinaryDecoder) ReadFixed(bytes []byte) error {
	return this.readBytes(bytes, 0, len(bytes))
}

// Reads fixed sized binary object into the provided buffer.
// The second parameter is the position where the data needs to be written, the third is the size of binary object.
// Returns an error if it occurs.
func (this *BinaryDecoder) ReadFixedWithBounds(bytes []byte, start int, length int) error {
	return this.readBytes(bytes, start, length)
}

// SetBlock is used for Avro Object Container Files where the data is split in blocks and sets a data block
// for this decoder and sets the position to the start of this block.
func (this *BinaryDecoder) SetBlock(block *DataBlock) {
	this.buf = block.Data
	this.Seek(0)
}

// Seek sets the reading position of this Decoder to a given value allowing to skip items etc.
func (this *BinaryDecoder) Seek(pos int64) {
	this.pos = pos
}

// Tell returns the current reading position of this Decoder.
func (this *BinaryDecoder) Tell() int64 {
	return this.pos
}

func checkEOF(buf []byte, pos int64, length int) error {
	if int64(len(buf)) < pos+int64(length) {
		return EOF
	}
	return nil
}

func (this *BinaryDecoder) readItemCount() (int64, error) {
	if count, err := this.ReadLong(); err != nil {
		return 0, err
	} else {
		if count < 0 {
			this.ReadLong()
			count = -count
		}
		return count, err
	}
}

func (this *BinaryDecoder) readBytes(bytes []byte, start int, length int) error {
	if length < 0 {
		return NegativeBytesLength
	}
	if err := checkEOF(this.buf, this.pos, int(start+length)); err != nil {
		return EOF
	}
	copy(bytes[:], this.buf[this.pos+int64(start):this.pos+int64(start)+int64(length)])
	this.pos += int64(length)

	return nil
}
