package avro

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
)

// Reader is an interface that may be implemented to avoid using runtime reflection during deserialization.
// Implementing it is optional and may be used as an optimization. Falls back to using reflection if not implemented.
type Reader interface {
	Read(dec Decoder) error
}

// DatumReader is an interface that is responsible for reading structured data according to schema from a decoder
type DatumReader interface {
	// Reads a single structured entry using this DatumReader according to provided Schema.
	// Accepts a value to fill with data and a Decoder to read from. Given value MUST be of pointer type.
	// May return an error indicating a read failure.
	Read(interface{}, Decoder) error

	// Sets the schema for this DatumReader to know the data structure.
	// Note that it must be called before calling Read.
	SetSchema(Schema)
}

var enumSymbolsToIndexCache map[string]map[string]int32 = make(map[string]map[string]int32)
var enumSymbolsToIndexCacheLock sync.Mutex

// Generic Avro enum representation. This is still subject to change and may be rethought.
type GenericEnum struct {
	// Avro enum symbols.
	Symbols        []string
	symbolsToIndex map[string]int32
	index          int32
}

// Returns a new GenericEnum that uses provided enum symbols.
func NewGenericEnum(symbols []string) *GenericEnum {
	symbolsToIndex := make(map[string]int32)
	for index, symbol := range symbols {
		symbolsToIndex[symbol] = int32(index)
	}

	return &GenericEnum{
		Symbols:        symbols,
		symbolsToIndex: symbolsToIndex,
	}
}

// Gets the numeric value for this enum.
func (this *GenericEnum) GetIndex() int32 {
	return this.index
}

// Gets the string value for this enum (e.g. symbol).
func (this *GenericEnum) Get() string {
	return this.Symbols[this.index]
}

// Sets the numeric value for this enum.
func (this *GenericEnum) SetIndex(index int32) {
	this.index = index
}

// Sets the string value for this enum (e.g. symbol).
// Panics if the given symbol does not exist in this enum.
func (this *GenericEnum) Set(symbol string) {
	if index, exists := this.symbolsToIndex[symbol]; !exists {
		panic("Unknown enum symbol")
	} else {
		this.index = index
	}
}

// SpecificDatumReader implements DatumReader and is used for filling Go structs with data.
// Each value passed to Read is expected to be a pointer.
type SpecificDatumReader struct {
	schema Schema
}

// Creates a new SpecificDatumReader.
func NewSpecificDatumReader() *SpecificDatumReader {
	return &SpecificDatumReader{}
}

// Sets the schema for this SpecificDatumReader to know the data structure.
// Note that it must be called before calling Read.
func (this *SpecificDatumReader) SetSchema(schema Schema) {
	this.schema = schema
}

// Reads a single structured entry using this SpecificDatumReader.
// Accepts a Go struct with exported fields to fill with data and a Decoder to read from. Given value MUST be of
// pointer type. Field names should match field names in Avro schema but be exported (e.g. "some_value" in Avro
// schema is expected to be Some_value in struct) or you may provide Go struct tags to explicitly show how
// to map fields (e.g. if you want to map "some_value" field of type int to SomeValue in Go struct you should define
// your struct field as follows: SomeValue int32 `avro:"some_field"`).
// May return an error indicating a read failure.
func (this *SpecificDatumReader) Read(v interface{}, dec Decoder) error {
	if reader, ok := v.(Reader); ok {
		return reader.Read(dec)
	}

	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return errors.New("Not applicable for non-pointer types or nil")
	}
	if this.schema == nil {
		return SchemaNotSet
	}

	sch := this.schema.(*RecordSchema)
	for i := 0; i < len(sch.Fields); i++ {
		field := sch.Fields[i]
		this.findAndSet(v, field, dec)
	}

	return nil
}

func (this *SpecificDatumReader) findAndSet(v interface{}, field *SchemaField, dec Decoder) error {
	structField, err := findField(reflect.ValueOf(v), field.Name)
	if err != nil {
		return err
	}

	value, err := this.readValue(field.Type, structField, dec)
	if err != nil {
		return err
	}

	this.setValue(field, structField, value)

	return nil
}

func (this *SpecificDatumReader) readValue(field Schema, reflectField reflect.Value, dec Decoder) (reflect.Value, error) {
	switch field.Type() {
	case Null:
		return reflect.ValueOf(nil), nil
	case Boolean:
		return this.mapPrimitive(func() (interface{}, error) { return dec.ReadBoolean() })
	case Int:
		return this.mapPrimitive(func() (interface{}, error) { return dec.ReadInt() })
	case Long:
		return this.mapPrimitive(func() (interface{}, error) { return dec.ReadLong() })
	case Float:
		return this.mapPrimitive(func() (interface{}, error) { return dec.ReadFloat() })
	case Double:
		return this.mapPrimitive(func() (interface{}, error) { return dec.ReadDouble() })
	case Bytes:
		return this.mapPrimitive(func() (interface{}, error) { return dec.ReadBytes() })
	case String:
		return this.mapPrimitive(func() (interface{}, error) { return dec.ReadString() })
	case Array:
		return this.mapArray(field, reflectField, dec)
	case Enum:
		return this.mapEnum(field, dec)
	case Map:
		return this.mapMap(field, reflectField, dec)
	case Union:
		return this.mapUnion(field, reflectField, dec)
	case Fixed:
		return this.mapFixed(field, dec)
	case Record:
		return this.mapRecord(field, reflectField, dec)
	case Recursive:
		return this.mapRecord(field.(*RecursiveSchema).Actual, reflectField, dec)
	}

	return reflect.ValueOf(nil), fmt.Errorf("Unknown field type: %d", field.Type())
}

func (this *SpecificDatumReader) setValue(field *SchemaField, where reflect.Value, what reflect.Value) {
	zero := reflect.Value{}
	if zero != what {
		where.Set(what)
	}
}

func (this *SpecificDatumReader) mapPrimitive(reader func() (interface{}, error)) (reflect.Value, error) {
	if value, err := reader(); err != nil {
		return reflect.ValueOf(value), err
	} else {
		return reflect.ValueOf(value), nil
	}
}

func (this *SpecificDatumReader) mapArray(field Schema, reflectField reflect.Value, dec Decoder) (reflect.Value, error) {
	if arrayLength, err := dec.ReadArrayStart(); err != nil {
		return reflect.ValueOf(arrayLength), err
	} else {
		array := reflect.MakeSlice(reflectField.Type(), 0, 0)
		pointer := reflectField.Type().Elem().Kind() == reflect.Ptr
		for {
			if arrayLength == 0 {
				break
			}

			arrayPart := reflect.MakeSlice(reflectField.Type(), int(arrayLength), int(arrayLength))
			var i int64 = 0
			for ; i < arrayLength; i++ {
				current := arrayPart.Index(int(i))
				val, err := this.readValue(field.(*ArraySchema).Items, current, dec)
				if err != nil {
					return reflect.ValueOf(arrayLength), err
				}

				if pointer && val.Kind() != reflect.Ptr {
					val = val.Addr()
				} else if !pointer && val.Kind() == reflect.Ptr {
					val = val.Elem()
				}
				current.Set(val)
			}
			//concatenate arrays
			if array.Len() == 0 {
				array = arrayPart
			} else {
				array = reflect.AppendSlice(array, arrayPart)
			}
			arrayLength, err = dec.ArrayNext()
			if err != nil {
				return reflect.ValueOf(arrayLength), err
			}
		}
		return array, nil
	}
}

func (this *SpecificDatumReader) mapMap(field Schema, reflectField reflect.Value, dec Decoder) (reflect.Value, error) {
	if mapLength, err := dec.ReadMapStart(); err != nil {
		return reflect.ValueOf(mapLength), err
	} else {
		resultMap := reflect.MakeMap(reflectField.Type())
		for {
			if mapLength == 0 {
				break
			}

			var i int64 = 0
			for ; i < mapLength; i++ {
				key, err := this.readValue(&StringSchema{}, reflectField, dec)
				if err != nil {
					return reflect.ValueOf(mapLength), err
				}
				val, err := this.readValue(field.(*MapSchema).Values, reflectField, dec)
				if err != nil {
					return reflect.ValueOf(mapLength), nil
				}
				if val.Kind() == reflect.Ptr {
					resultMap.SetMapIndex(key, val.Elem())
				} else {
					resultMap.SetMapIndex(key, val)
				}
			}

			mapLength, err = dec.MapNext()
			if err != nil {
				return reflect.ValueOf(mapLength), err
			}
		}
		return resultMap, nil
	}
}

func (this *SpecificDatumReader) mapEnum(field Schema, dec Decoder) (reflect.Value, error) {
	if enumIndex, err := dec.ReadEnum(); err != nil {
		return reflect.ValueOf(enumIndex), err
	} else {
		schema := field.(*EnumSchema)
		fullName := GetFullName(schema)

		var symbolsToIndex map[string]int32
		enumSymbolsToIndexCacheLock.Lock()
		if symbolsToIndex = enumSymbolsToIndexCache[fullName]; symbolsToIndex == nil {
			symbolsToIndex = NewGenericEnum(schema.Symbols).symbolsToIndex
			enumSymbolsToIndexCache[fullName] = symbolsToIndex
		}
		enumSymbolsToIndexCacheLock.Unlock()

		enum := &GenericEnum{
			Symbols:        schema.Symbols,
			symbolsToIndex: symbolsToIndex,
			index:          enumIndex,
		}
		return reflect.ValueOf(enum), nil
	}
}

func (this *SpecificDatumReader) mapUnion(field Schema, reflectField reflect.Value, dec Decoder) (reflect.Value, error) {
	if unionType, err := dec.ReadInt(); err != nil {
		return reflect.ValueOf(unionType), err
	} else {
		union := field.(*UnionSchema).Types[unionType]
		return this.readValue(union, reflectField, dec)
	}
}

func (this *SpecificDatumReader) mapFixed(field Schema, dec Decoder) (reflect.Value, error) {
	fixed := make([]byte, field.(*FixedSchema).Size)
	if err := dec.ReadFixed(fixed); err != nil {
		return reflect.ValueOf(fixed), err
	}
	return reflect.ValueOf(fixed), nil
}

func (this *SpecificDatumReader) mapRecord(field Schema, reflectField reflect.Value, dec Decoder) (reflect.Value, error) {
	var t reflect.Type
	switch reflectField.Kind() {
	case reflect.Ptr, reflect.Array, reflect.Map, reflect.Slice, reflect.Chan:
		t = reflectField.Type().Elem()
	default:
		t = reflectField.Type()
	}
	record := reflect.New(t).Interface()

	recordSchema := field.(*RecordSchema)
	for i := 0; i < len(recordSchema.Fields); i++ {
		this.findAndSet(record, recordSchema.Fields[i], dec)
	}

	return reflect.ValueOf(record), nil
}

// GenericDatumReader implements DatumReader and is used for filling GenericRecords or other Avro supported types
// (full list is: interface{}, bool, int32, int64, float32, float64, string, slices of any type, maps with string keys
// and any values, GenericEnums) with data.
// Each value passed to Read is expected to be a pointer.
type GenericDatumReader struct {
	schema Schema
}

// Creates a new GenericDatumReader.
func NewGenericDatumReader() *GenericDatumReader {
	return &GenericDatumReader{}
}

// Sets the schema for this GenericDatumReader to know the data structure.
// Note that it must be called before calling Read.
func (this *GenericDatumReader) SetSchema(schema Schema) {
	this.schema = schema
}

// Reads a single entry using this GenericDatumReader.
// Accepts a value to fill with data and a Decoder to read from. Given value MUST be of  pointer type.
// May return an error indicating a read failure.
func (this *GenericDatumReader) Read(v interface{}, dec Decoder) error {
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return errors.New("Not applicable for non-pointer types or nil")
	}
	rv = rv.Elem()
	if this.schema == nil {
		return SchemaNotSet
	}

	//read the value
	value, err := this.readValue(this.schema, dec)
	if err != nil {
		return err
	}

	newValue := reflect.ValueOf(value)
	// dereference the value if needed
	if newValue.Kind() == reflect.Ptr {
		newValue = newValue.Elem()
	}

	//set the new value
	rv.Set(newValue)

	return nil
}

func (this *GenericDatumReader) findAndSet(record *GenericRecord, field *SchemaField, dec Decoder) error {
	value, err := this.readValue(field.Type, dec)
	if err != nil {
		return err
	}

	switch typedValue := value.(type) {
	case *GenericEnum:
		if typedValue.GetIndex() >= int32(len(typedValue.Symbols)) {
			return errors.New("Enum index invalid!")
		}
		record.Set(field.Name, typedValue.Symbols[typedValue.GetIndex()])

	default:
		record.Set(field.Name, value)
	}

	return nil
}

func (this *GenericDatumReader) readValue(field Schema, dec Decoder) (interface{}, error) {
	switch field.Type() {
	case Null:
		return nil, nil
	case Boolean:
		return dec.ReadBoolean()
	case Int:
		return dec.ReadInt()
	case Long:
		return dec.ReadLong()
	case Float:
		return dec.ReadFloat()
	case Double:
		return dec.ReadDouble()
	case Bytes:
		return dec.ReadBytes()
	case String:
		return dec.ReadString()
	case Array:
		return this.mapArray(field, dec)
	case Enum:
		return this.mapEnum(field, dec)
	case Map:
		return this.mapMap(field, dec)
	case Union:
		return this.mapUnion(field, dec)
	case Fixed:
		return this.mapFixed(field, dec)
	case Record:
		return this.mapRecord(field, dec)
	case Recursive:
		return this.mapRecord(field.(*RecursiveSchema).Actual, dec)
	}

	return nil, fmt.Errorf("Unknown field type: %d", field.Type())
}

func (this *GenericDatumReader) mapArray(field Schema, dec Decoder) ([]interface{}, error) {
	if arrayLength, err := dec.ReadArrayStart(); err != nil {
		return nil, err
	} else {
		array := make([]interface{}, 0)
		for {
			if arrayLength == 0 {
				break
			}
			arrayPart := make([]interface{}, arrayLength, arrayLength)
			var i int64 = 0
			for ; i < arrayLength; i++ {
				val, err := this.readValue(field.(*ArraySchema).Items, dec)
				if err != nil {
					return nil, err
				}
				arrayPart[i] = val
			}
			//concatenate arrays
			concatArray := make([]interface{}, len(array)+int(arrayLength), cap(array)+int(arrayLength))
			copy(concatArray, array)
			copy(concatArray, arrayPart)
			array = concatArray
			arrayLength, err = dec.ArrayNext()
			if err != nil {
				return nil, err
			}
		}
		return array, nil
	}
}

func (this *GenericDatumReader) mapEnum(field Schema, dec Decoder) (*GenericEnum, error) {
	if enumIndex, err := dec.ReadEnum(); err != nil {
		return nil, err
	} else {
		schema := field.(*EnumSchema)
		fullName := GetFullName(schema)

		var symbolsToIndex map[string]int32
		enumSymbolsToIndexCacheLock.Lock()
		if symbolsToIndex = enumSymbolsToIndexCache[fullName]; symbolsToIndex == nil {
			symbolsToIndex = NewGenericEnum(schema.Symbols).symbolsToIndex
			enumSymbolsToIndexCache[fullName] = symbolsToIndex
		}
		enumSymbolsToIndexCacheLock.Unlock()

		enum := &GenericEnum{
			Symbols:        schema.Symbols,
			symbolsToIndex: symbolsToIndex,
			index:          enumIndex,
		}
		return enum, nil
	}
}

func (this *GenericDatumReader) mapMap(field Schema, dec Decoder) (map[string]interface{}, error) {
	if mapLength, err := dec.ReadMapStart(); err != nil {
		return nil, err
	} else {
		resultMap := make(map[string]interface{})
		for {
			if mapLength == 0 {
				break
			}
			var i int64 = 0
			for ; i < mapLength; i++ {
				key, err := this.readValue(&StringSchema{}, dec)
				if err != nil {
					return nil, err
				}
				val, err := this.readValue(field.(*MapSchema).Values, dec)
				if err != nil {
					return nil, err
				}
				resultMap[key.(string)] = val
			}

			mapLength, err = dec.MapNext()
			if err != nil {
				return nil, err
			}
		}
		return resultMap, nil
	}
}

func (this *GenericDatumReader) mapUnion(field Schema, dec Decoder) (interface{}, error) {
	if unionType, err := dec.ReadInt(); err != nil {
		return nil, err
	} else {
		if unionType >= 0 && unionType < int32(len(field.(*UnionSchema).Types)) {
			union := field.(*UnionSchema).Types[unionType]
			return this.readValue(union, dec)
		} else {
			return nil, errors.New("Union type overflow")
		}
	}
}

func (this *GenericDatumReader) mapFixed(field Schema, dec Decoder) ([]byte, error) {
	fixed := make([]byte, field.(*FixedSchema).Size)
	if err := dec.ReadFixed(fixed); err != nil {
		return nil, err
	}
	return fixed, nil
}

func (this *GenericDatumReader) mapRecord(field Schema, dec Decoder) (*GenericRecord, error) {
	record := NewGenericRecord(field)

	recordSchema := field.(*RecordSchema)
	for i := 0; i < len(recordSchema.Fields); i++ {
		err := this.findAndSet(record, recordSchema.Fields[i], dec)
		if err != nil {
			return nil, err
		}
	}

	return record, nil
}
