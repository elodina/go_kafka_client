package avro

import (
	"bytes"
	"testing"
)

func TestDataFileWriter(t *testing.T) {
	schema := MustParseSchema(primitiveSchemaRaw)
	datumWriter := NewSpecificDatumWriter()
	datumWriter.SetSchema(schema)
	buf := &bytes.Buffer{}
	dfw, err := NewDataFileWriter(buf, schema, datumWriter)
	if err != nil {
		t.Fatal(err)
	}

	d := 5.0

	// test size growth of underlying file with respect to flushes
	var sizes = []int{
		884, 884, 936, 936, 988, 988,
		1040, 1040, 1092, 1092,
	}
	for i, size := range sizes {
		p := primitive{
			LongField:   int64(i),
			DoubleField: d,
		}
		if err = dfw.Write(&p); err != nil {
			t.Fatalf("Write failed %v", err)
		}
		if i%2 == 0 {
			if err = dfw.Flush(); err != nil {
				t.Fatal(err)
			}
		}
		assert(t, buf.Len(), size)
		d *= 7
	}

	if err = dfw.Close(); err != nil {
		t.Fatal(err)
	}
	encoded := buf.Bytes()
	assert(t, len(encoded), 1145)

	// now make sure we can decode again
	datumReader := NewSpecificDatumReader()
	dfr, err := newDataFileReaderBytes(encoded, datumReader)
	if err != nil {
		t.Fatal(err)
	}
	var p primitive
	dfr.Next(&p)
	assert(t, p.LongField, int64(0))
	dfr.Next(&p)
	assert(t, p.LongField, int64(1))
}
