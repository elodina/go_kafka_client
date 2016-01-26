package avro

import (
	"bytes"
	"math/rand"
	"testing"
)

const primitiveSchemaRaw = `{"type":"record","name":"Primitive","namespace":"example.avro","fields":[{"name":"booleanField","type":"boolean"},{"name":"intField","type":"int"},{"name":"longField","type":"long"},{"name":"floatField","type":"float"},{"name":"doubleField","type":"double"},{"name":"bytesField","type":"bytes"},{"name":"stringField","type":"string"},{"name":"nullField","type":"null"}]}`

func TestSpecificDatumWriterPrimitives(t *testing.T) {
	sch, err := ParseSchema(primitiveSchemaRaw)
	assert(t, err, nil)

	buffer := &bytes.Buffer{}
	enc := NewBinaryEncoder(buffer)

	w := NewSpecificDatumWriter()
	w.SetSchema(sch)

	in := randomPrimitiveObject()

	err = w.Write(in, enc)
	assert(t, err, nil)
	dec := NewBinaryDecoder(buffer.Bytes())
	r := NewSpecificDatumReader()
	r.SetSchema(sch)

	out := &primitive{}
	r.Read(out, dec)

	assert(t, out.BooleanField, in.BooleanField)
	assert(t, out.IntField, in.IntField)
	assert(t, out.LongField, in.LongField)
	assert(t, out.FloatField, in.FloatField)
	assert(t, out.DoubleField, in.DoubleField)
	assert(t, out.BytesField, in.BytesField)
	assert(t, out.StringField, in.StringField)
	assert(t, out.NullField, in.NullField)
}

func TestSpecificDatumWriterComplex(t *testing.T) {
	complex := newComplex()
	complex.StringArray = []string{"asd", "zxc", "qwe"}
	complex.LongArray = []int64{0, 1, 2, 3, 4}
	complex.MapOfInts = map[string]int32{"a": 0, "b": 1}
	complex.UnionField = "hello world"
	complex.FixedField = []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	complex.EnumField.SetIndex(Foo_B)
	complex.RecordField.FloatRecordField = 12.3
	complex.RecordField.IntRecordField = 5
	complex.RecordField.LongRecordField = 12345678
	complex.RecordField.StringRecordField = "i am groot"

	buffer := &bytes.Buffer{}
	enc := NewBinaryEncoder(buffer)

	w := NewSpecificDatumWriter()
	w.SetSchema(complex.Schema())

	err := w.Write(complex, enc)
	assert(t, err, nil)
	dec := NewBinaryDecoder(buffer.Bytes())
	r := NewSpecificDatumReader()
	r.SetSchema(complex.Schema())

	decodedComplex := newComplex()
	err = r.Read(decodedComplex, dec)
	assert(t, err, nil)

	assert(t, decodedComplex.StringArray, complex.StringArray)
	assert(t, decodedComplex.LongArray, complex.LongArray)
	assert(t, decodedComplex.MapOfInts, complex.MapOfInts)
	assert(t, decodedComplex.UnionField, complex.UnionField)
	assert(t, decodedComplex.FixedField, complex.FixedField)
	assert(t, decodedComplex.EnumField, complex.EnumField)
	assert(t, decodedComplex.RecordField.FloatRecordField, complex.RecordField.FloatRecordField)
	assert(t, decodedComplex.RecordField.IntRecordField, complex.RecordField.IntRecordField)
	assert(t, decodedComplex.RecordField.LongRecordField, complex.RecordField.LongRecordField)
	assert(t, decodedComplex.RecordField.StringRecordField, complex.RecordField.StringRecordField)
}

func TestSpecificDatumWriterRecursive(t *testing.T) {
	employee1 := newEmployee()
	employee1.Name = "Employee 1"
	employee2 := newEmployee()
	employee2.Name = "Employee 2"
	employee3 := newEmployee()
	employee3.Name = "Employee 3"
	employee1.Boss = employee2
	employee2.Boss = employee3

	buffer := &bytes.Buffer{}
	enc := NewBinaryEncoder(buffer)

	w := NewSpecificDatumWriter()
	w.SetSchema(employee1.Schema())

	err := w.Write(employee1, enc)
	assert(t, err, nil)
	dec := NewBinaryDecoder(buffer.Bytes())
	r := NewSpecificDatumReader()
	r.SetSchema(employee1.Schema())

	decodedEmployee := newEmployee()
	err = r.Read(decodedEmployee, dec)
	assert(t, err, nil)

	assert(t, decodedEmployee.Name, employee1.Name)
	assert(t, decodedEmployee.Boss.Name, employee1.Boss.Name)
	assert(t, decodedEmployee.Boss.Boss.Name, employee1.Boss.Boss.Name)
}

func TestSpecificDatumTags(t *testing.T) {
	type Tagged struct {
		Bool   bool              `avro:"booleanField"`
		Int    int32             `avro:"intField"`
		Long   int64             `avro:"longField"`
		Float  float32           `avro:"floatField"`
		Double float64           `avro:"doubleField"`
		Bytes  []byte            `avro:"bytesField"`
		String string            `avro:"stringField"`
		Null   interface{}       `avro:"nullField"`
		Array  []string          `avro:"arrayField"`
		Map    map[string]string `avro:"mapField"`
	}

	sch, err := ParseSchema(`{"type":"record","name":"Tagged","namespace":"example.avro","fields":[{"name":"booleanField","type":"boolean"},{"name":"intField","type":"int"},{"name":"longField","type":"long"},{"name":"floatField","type":"float"},{"name":"doubleField","type":"double"},{"name":"bytesField","type":"bytes"},{"name":"stringField","type":"string"},{"name":"nullField","type":"null"},{"name":"arrayField","type":{"type":"array","items":"string"}},{"name":"mapField","type":{"type":"map","values":"string"}}]}`)
	assert(t, err, nil)

	buffer := &bytes.Buffer{}
	enc := NewBinaryEncoder(buffer)

	w := NewSpecificDatumWriter()
	w.SetSchema(sch)

	in := &Tagged{
		Bool:   true,
		Int:    123,
		Long:   1234567890,
		Float:  13.5,
		Double: 56783456.12736,
		Bytes:  []byte{0, 1, 2, 3, 4},
		String: "hello world",
		Null:   nil,
		Array:  []string{"the", "quick", "brown", "fox", "jumps", "over", "the", "lazy", "dog"},
		Map:    map[string]string{"a": "A", "b": "B"},
	}

	err = w.Write(in, enc)
	assert(t, err, nil)
	dec := NewBinaryDecoder(buffer.Bytes())
	r := NewSpecificDatumReader()
	r.SetSchema(sch)

	out := &Tagged{}
	r.Read(out, dec)

	assert(t, out.Bool, in.Bool)
	assert(t, out.Int, in.Int)
	assert(t, out.Long, in.Long)
	assert(t, out.Float, in.Float)
	assert(t, out.Double, in.Double)
	assert(t, out.Bytes, in.Bytes)
	assert(t, out.String, in.String)
	assert(t, out.Null, in.Null)
	assert(t, out.Array, in.Array)
	assert(t, out.Map, in.Map)
}

func TestGenericDatumWriterEmptyMap(t *testing.T) {
	sch, err := ParseSchema(`{
    "type": "record",
    "name": "Rec",
    "fields": [
        {
            "name": "map1",
            "type": {
                "type": "map",
                "values": "string"
            }
        }
    ]
}`)
	if err != nil {
		t.Fatal(err)
	}

	buffer := &bytes.Buffer{}
	enc := NewBinaryEncoder(buffer)
	w := NewGenericDatumWriter()
	w.SetSchema(sch)

	rec := NewGenericRecord(sch)
	rec.Set("map1", map[string]string{})

	err = w.Write(rec, enc)
	if err != nil {
		t.Fatal(err)
	}

	assert(t, buffer.Bytes(), []byte{0x00})
}

func TestGenericDatumWriterEmptyArray(t *testing.T) {
	sch, err := ParseSchema(`{
    "type": "record",
    "name": "Rec",
    "fields": [
        {
            "name": "arr",
            "type": {
                "type": "array",
                "items": "string"
            }
        }
    ]
}`)
	if err != nil {
		t.Fatal(err)
	}

	buffer := &bytes.Buffer{}
	enc := NewBinaryEncoder(buffer)
	w := NewGenericDatumWriter()
	w.SetSchema(sch)

	rec := NewGenericRecord(sch)
	rec.Set("arr", []string{})

	err = w.Write(rec, enc)
	if err != nil {
		t.Fatal(err)
	}

	assert(t, buffer.Bytes(), []byte{0x00})
}

func randomPrimitiveObject() *primitive {
	p := &primitive{}
	p.BooleanField = rand.Int()%2 == 0

	p.IntField = rand.Int31()
	if p.IntField%3 == 0 {
		p.IntField = -p.IntField
	}

	p.LongField = rand.Int63()
	if p.LongField%3 == 0 {
		p.LongField = -p.LongField
	}

	p.FloatField = rand.Float32()
	if p.BooleanField {
		p.FloatField = -p.FloatField
	}

	p.DoubleField = rand.Float64()
	if !p.BooleanField {
		p.DoubleField = -p.DoubleField
	}

	p.BytesField = randomBytes(rand.Intn(99) + 1)
	p.StringField = randomString(rand.Intn(99) + 1)

	return p
}

func BenchmarkEncodeVarint32(b *testing.B) {
	enc := NewBinaryEncoder(nil)
	for i := 0; i < b.N; i++ {
		enc.encodeVarint32(int32(i))
	}
}

func BenchmarkEncodeVarint64(b *testing.B) {
	enc := NewBinaryEncoder(nil)
	for i := 0; i < b.N; i++ {
		enc.encodeVarint64(int64(i))
	}
}

func BenchmarkSpecificDatumWriter(b *testing.B) {
	var c = newComplex()
	c.FixedField = []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	w := NewSpecificDatumWriter()
	w.SetSchema(c.Schema())
	var buf bytes.Buffer
	buf.Grow(10000)
	err := w.Write(c, NewBinaryEncoder(&buf))
	if err != nil {
		panic(err)
	}
	buf.Reset()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w.Write(c, NewBinaryEncoder(&buf))
		buf.Reset()
	}
}

type _complex struct {
	StringArray []string
	LongArray   []int64
	EnumField   *GenericEnum
	MapOfInts   map[string]int32
	UnionField  interface{}
	FixedField  []byte
	RecordField *_testRecord
}

func newComplex() *_complex {
	return &_complex{
		StringArray: make([]string, 0),
		LongArray:   make([]int64, 0),
		EnumField:   NewGenericEnum([]string{"A", "B", "C", "D"}),
		MapOfInts:   make(map[string]int32),
		RecordField: newTestRecord(),
	}
}

func (this *_complex) Schema() Schema {
	if _Complex_schema_err != nil {
		panic(_Complex_schema_err)
	}
	return _Complex_schema
}

// Enum values for Foo
const (
	Foo_A int32 = 0
	Foo_B int32 = 1
	Foo_C int32 = 2
	Foo_D int32 = 3
)

type _testRecord struct {
	LongRecordField   int64
	StringRecordField string
	IntRecordField    int32
	FloatRecordField  float32
}

func newTestRecord() *_testRecord {
	return &_testRecord{}
}

func (this *_testRecord) Schema() Schema {
	if _TestRecord_schema_err != nil {
		panic(_TestRecord_schema_err)
	}
	return _TestRecord_schema
}

// Generated by codegen. Please do not modify.
var _Complex_schema, _Complex_schema_err = ParseSchema(`{
    "type": "record",
    "namespace": "example.avro",
    "name": "Complex",
    "fields": [
        {
            "name": "stringArray",
            "type": {
                "type": "array",
                "items": "string"
            }
        },
        {
            "name": "longArray",
            "type": {
                "type": "array",
                "items": "long"
            }
        },
        {
            "name": "enumField",
            "type": {
                "type": "enum",
                "name": "foo",
                "symbols": [
                    "A",
                    "B",
                    "C",
                    "D"
                ]
            }
        },
        {
            "name": "mapOfInts",
            "type": {
                "type": "map",
                "values": "int"
            }
        },
        {
            "name": "unionField",
            "type": [
                "null",
                "string"
            ]
        },
        {
            "name": "fixedField",
            "type": {
                "type": "fixed",
                "size": 16,
                "name": "md5"
            }
        },
        {
            "name": "recordField",
            "type": {
                "type": "record",
                "name": "TestRecord",
                "fields": [
                    {
                        "name": "longRecordField",
                        "type": "long"
                    },
                    {
                        "name": "stringRecordField",
                        "type": "string"
                    },
                    {
                        "name": "intRecordField",
                        "type": "int"
                    },
                    {
                        "name": "floatRecordField",
                        "type": "float"
                    }
                ]
            }
        }
    ]
}`)

// Generated by codegen. Please do not modify.
var _TestRecord_schema, _TestRecord_schema_err = ParseSchema(`{
    "type": "record",
    "name": "TestRecord",
    "fields": [
        {
            "name": "longRecordField",
            "type": "long"
        },
        {
            "name": "stringRecordField",
            "type": "string"
        },
        {
            "name": "intRecordField",
            "type": "int"
        },
        {
            "name": "floatRecordField",
            "type": "float"
        }
    ]
}`)

type _employee struct {
	Name string
	Boss *_employee
}

func newEmployee() *_employee {
	return &_employee{}
}

func (this *_employee) Schema() Schema {
	if _Employee_schema_err != nil {
		panic(_Employee_schema_err)
	}
	return _Employee_schema
}

// Generated by codegen. Please do not modify.
var _Employee_schema, _Employee_schema_err = ParseSchema(`{
    "type": "record",
    "namespace": "avro",
    "name": "Employee",
    "fields": [
        {
            "name": "name",
            "type": "string"
        },
        {
            "name": "boss",
            "type": [
                "Employee",
                "null"
            ]
        }
    ]
}`)
