package avro

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"math"
)

const (
	version    byte = 1
	sync_size       = 16
	schema_key      = "avro.schema"
	codec_key       = "avro.codec"
)

var magic []byte = []byte{'O', 'b', 'j', version}

var syncBuffer = make([]byte, sync_size)

// DataFileReader is a reader for Avro Object Container Files. More here: https://avro.apache.org/docs/current/spec.html#Object+Container+Files
type DataFileReader struct {
	data         []byte
	header       *header
	block        *DataBlock
	dec          Decoder
	blockDecoder Decoder
	datum        DatumReader
}

type header struct {
	meta map[string][]byte
	sync []byte
}

func newHeader() *header {
	header := &header{}
	header.meta = make(map[string][]byte)
	header.sync = make([]byte, sync_size)

	return header
}

// Creates a new DataFileReader for a given file and using the given DatumReader to read the data from that file.
// May return an error if the file contains invalid data or is just missing.
func NewDataFileReader(filename string, datumReader DatumReader) (*DataFileReader, error) {
	if buf, err := ioutil.ReadFile(filename); err != nil {
		return nil, err
	} else {
		if len(buf) < len(magic) || !bytes.Equal(magic, buf[0:4]) {
			return nil, NotAvroFile
		}

		dec := NewBinaryDecoder(buf)
		blockDecoder := NewBinaryDecoder(nil)
		reader := &DataFileReader{
			data:         buf,
			dec:          dec,
			blockDecoder: blockDecoder,
			datum:        datumReader,
		}
		reader.Seek(4) //skip the magic bytes

		reader.header = newHeader()
		if metaLength, err := dec.ReadMapStart(); err != nil {
			return nil, err
		} else {
			for {
				var i int64 = 0
				for i < metaLength {
					key, err := dec.ReadString()
					if err != nil {
						return nil, err
					}

					value, err := dec.ReadBytes()
					if err != nil {
						return nil, err
					}
					reader.header.meta[key] = value
					i++
				}
				metaLength, err = dec.MapNext()
				if err != nil {
					return nil, err
				} else if metaLength == 0 {
					break
				}
			}
		}
		dec.ReadFixed(reader.header.sync)
		//TODO codec?

		schema, err := ParseSchema(string(reader.header.meta[schema_key]))
		if err != nil {
			return nil, err
		}
		reader.datum.SetSchema(schema)
		reader.block = &DataBlock{}

		if reader.hasNextBlock() {
			if err := reader.NextBlock(); err != nil {
				return nil, err
			}
		}

		return reader, nil
	}
}

// Switches the reading position in this DataFileReader to a provided value.
func (this *DataFileReader) Seek(pos int64) {
	this.dec.Seek(pos)
}

func (this *DataFileReader) hasNext() (bool, error) {
	if this.block.BlockRemaining == 0 {
		if int64(this.block.BlockSize) != this.blockDecoder.Tell() {
			return false, BlockNotFinished
		}
		if this.hasNextBlock() {
			if err := this.NextBlock(); err != nil {
				return false, err
			}
		} else {
			return false, nil
		}
	}
	return true, nil
}

func (this *DataFileReader) hasNextBlock() bool {
	return int64(len(this.data)) > this.dec.Tell()
}

// Reads the next value from file and fills the given value with data.
// First return value indicates whether the read was successful.
// Second return value indicates whether there was an error while reading data.
// Returns (false, nil) when no more data left to read.
func (this *DataFileReader) Next(v interface{}) (bool, error) {
	if hasNext, err := this.hasNext(); err != nil {
		return false, err
	} else {
		if hasNext {
			err := this.datum.Read(v, this.blockDecoder)
			if err != nil {
				return false, err
			}
			this.block.BlockRemaining--
			return true, nil
		} else {
			return false, nil
		}
	}
	return false, nil
}

// Tells this DataFileReader to skip current block and move to next one.
// May return an error if the block is malformed or no more blocks left to read.
func (this *DataFileReader) NextBlock() error {
	if blockCount, err := this.dec.ReadLong(); err != nil {
		return err
	} else {
		if blockSize, err := this.dec.ReadLong(); err != nil {
			return err
		} else {
			if blockSize > math.MaxInt32 || blockSize < 0 {
				return errors.New(fmt.Sprintf("Block size invalid or too large: %d", blockSize))
			}

			block := this.block
			if block.Data == nil || int64(len(block.Data)) < blockSize {
				block.Data = make([]byte, blockSize)
			}
			block.BlockRemaining = blockCount
			block.NumEntries = blockCount
			block.BlockSize = int(blockSize)
			this.dec.ReadFixedWithBounds(block.Data, 0, int(block.BlockSize))
			this.dec.ReadFixed(syncBuffer)
			if !bytes.Equal(syncBuffer, this.header.sync) {
				return InvalidSync
			}
			this.blockDecoder.SetBlock(this.block)
		}
	}
	return nil
}
