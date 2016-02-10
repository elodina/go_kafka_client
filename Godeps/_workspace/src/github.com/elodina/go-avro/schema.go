package avro

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"reflect"
)

const (
	// Record schema type constant
	Record int = iota

	// Enum schema type constant
	Enum

	// Array schema type constant
	Array

	// Map schema type constant
	Map

	// Union schema type constant
	Union

	// Fixed schema type constant
	Fixed

	// String schema type constant
	String

	// Bytes schema type constant
	Bytes

	// Int schema type constant
	Int

	// Long schema type constant
	Long

	// Float schema type constant
	Float

	// Double schema type constant
	Double

	// Boolean schema type constant
	Boolean

	// Null schema type constant
	Null

	// Recursive schema type constant. Recursive is an artificial type that means a Record schema without its definition
	// that should be looked up in some registry.
	Recursive
)

const (
	type_record  = "record"
	type_union   = "union"
	type_enum    = "enum"
	type_array   = "array"
	type_map     = "map"
	type_fixed   = "fixed"
	type_string  = "string"
	type_bytes   = "bytes"
	type_int     = "int"
	type_long    = "long"
	type_float   = "float"
	type_double  = "double"
	type_boolean = "boolean"
	type_null    = "null"
)

const (
	schema_aliasesField   = "aliases"
	schema_defaultField   = "default"
	schema_docField       = "doc"
	schema_fieldsField    = "fields"
	schema_itemsField     = "items"
	schema_nameField      = "name"
	schema_namespaceField = "namespace"
	schema_sizeField      = "size"
	schema_symbolsField   = "symbols"
	schema_typeField      = "type"
	schema_valuesField    = "values"
)

// Schema is an interface representing a single Avro schema (both primitive and complex).
type Schema interface {
	// Returns an integer constant representing this schema type.
	Type() int

	// If this is a record, enum or fixed, returns its name, otherwise the name of the primitive type.
	GetName() string

	// Gets a custom non-reserved string property from this schema and a bool representing if it exists.
	Prop(key string) (string, bool)

	// Converts this schema to its JSON representation.
	String() string

	// Checks whether the given value is writeable to this schema.
	Validate(v reflect.Value) bool
}

// StringSchema implements Schema and represents Avro string type.
type StringSchema struct{}

// Returns a JSON representation of StringSchema.
func (*StringSchema) String() string {
	return `{"type": "string"}`
}

// Returns a type constant for this StringSchema.
func (*StringSchema) Type() int {
	return String
}

// Returns a type name for this StringSchema.
func (*StringSchema) GetName() string {
	return type_string
}

// Doesn't return anything valuable for StringSchema.
func (*StringSchema) Prop(key string) (string, bool) {
	return "", false
}

// Checks whether the given value is writeable to this schema.
func (*StringSchema) Validate(v reflect.Value) bool {
	_, ok := dereference(v).Interface().(string)
	return ok
}

func (this *StringSchema) MarshalJSON() ([]byte, error) {
	return []byte(`"string"`), nil
}

// BytesSchema implements Schema and represents Avro bytes type.
type BytesSchema struct{}

// Returns a JSON representation of BytesSchema.
func (*BytesSchema) String() string {
	return `{"type": "bytes"}`
}

// Returns a type constant for this BytesSchema.
func (*BytesSchema) Type() int {
	return Bytes
}

// Returns a type name for this BytesSchema.
func (*BytesSchema) GetName() string {
	return type_bytes
}

// Doesn't return anything valuable for BytesSchema.
func (*BytesSchema) Prop(key string) (string, bool) {
	return "", false
}

// Checks whether the given value is writeable to this schema.
func (*BytesSchema) Validate(v reflect.Value) bool {
	v = dereference(v)

	return v.Kind() == reflect.Slice && v.Type().Elem().Kind() == reflect.Uint8
}

func (this *BytesSchema) MarshalJSON() ([]byte, error) {
	return []byte(`"bytes"`), nil
}

// IntSchema implements Schema and represents Avro int type.
type IntSchema struct{}

// Returns a JSON representation of IntSchema.
func (*IntSchema) String() string {
	return `{"type": "int"}`
}

// Returns a type constant for this IntSchema.
func (*IntSchema) Type() int {
	return Int
}

// Returns a type name for this IntSchema.
func (*IntSchema) GetName() string {
	return type_int
}

// Doesn't return anything valuable for IntSchema.
func (*IntSchema) Prop(key string) (string, bool) {
	return "", false
}

// Checks whether the given value is writeable to this schema.
func (*IntSchema) Validate(v reflect.Value) bool {
	return reflect.TypeOf(dereference(v).Interface()).Kind() == reflect.Int32
}

func (this *IntSchema) MarshalJSON() ([]byte, error) {
	return []byte(`"int"`), nil
}

// LongSchema implements Schema and represents Avro long type.
type LongSchema struct{}

// Returns a JSON representation of LongSchema.
func (*LongSchema) String() string {
	return `{"type": "long"}`
}

// Returns a type constant for this LongSchema.
func (*LongSchema) Type() int {
	return Long
}

// Returns a type name for this LongSchema.
func (*LongSchema) GetName() string {
	return type_long
}

// Doesn't return anything valuable for LongSchema.
func (*LongSchema) Prop(key string) (string, bool) {
	return "", false
}

// Checks whether the given value is writeable to this schema.
func (*LongSchema) Validate(v reflect.Value) bool {
	return reflect.TypeOf(dereference(v).Interface()).Kind() == reflect.Int64
}

func (this *LongSchema) MarshalJSON() ([]byte, error) {
	return []byte(`"long"`), nil
}

// FloatSchema implements Schema and represents Avro float type.
type FloatSchema struct{}

// Returns a JSON representation of FloatSchema.
func (*FloatSchema) String() string {
	return `{"type": "float"}`
}

// Returns a type constant for this FloatSchema.
func (*FloatSchema) Type() int {
	return Float
}

// Returns a type name for this FloatSchema.
func (*FloatSchema) GetName() string {
	return type_float
}

// Doesn't return anything valuable for FloatSchema.
func (*FloatSchema) Prop(key string) (string, bool) {
	return "", false
}

// Checks whether the given value is writeable to this schema.
func (*FloatSchema) Validate(v reflect.Value) bool {
	return reflect.TypeOf(dereference(v).Interface()).Kind() == reflect.Float32
}

func (this *FloatSchema) MarshalJSON() ([]byte, error) {
	return []byte(`"float"`), nil
}

// DoubleSchema implements Schema and represents Avro double type.
type DoubleSchema struct{}

// Returns a JSON representation of DoubleSchema.
func (*DoubleSchema) String() string {
	return `{"type": "double"}`
}

// Returns a type constant for this DoubleSchema.
func (*DoubleSchema) Type() int {
	return Double
}

// Returns a type name for this DoubleSchema.
func (*DoubleSchema) GetName() string {
	return type_double
}

// Doesn't return anything valuable for DoubleSchema.
func (*DoubleSchema) Prop(key string) (string, bool) {
	return "", false
}

// Checks whether the given value is writeable to this schema.
func (*DoubleSchema) Validate(v reflect.Value) bool {
	return reflect.TypeOf(dereference(v).Interface()).Kind() == reflect.Float64
}

func (this *DoubleSchema) MarshalJSON() ([]byte, error) {
	return []byte(`"double"`), nil
}

// BooleanSchema implements Schema and represents Avro boolean type.
type BooleanSchema struct{}

// Returns a JSON representation of BooleanSchema.
func (*BooleanSchema) String() string {
	return `{"type": "boolean"}`
}

// Returns a type constant for this BooleanSchema.
func (*BooleanSchema) Type() int {
	return Boolean
}

// Returns a type name for this BooleanSchema.
func (*BooleanSchema) GetName() string {
	return type_boolean
}

// Doesn't return anything valuable for BooleanSchema.
func (*BooleanSchema) Prop(key string) (string, bool) {
	return "", false
}

// Checks whether the given value is writeable to this schema.
func (*BooleanSchema) Validate(v reflect.Value) bool {
	return dereference(v).Kind() == reflect.Bool
}

func (this *BooleanSchema) MarshalJSON() ([]byte, error) {
	return []byte(`"boolean"`), nil
}

// NullSchema implements Schema and represents Avro null type.
type NullSchema struct{}

// Returns a JSON representation of NullSchema.
func (*NullSchema) String() string {
	return `{"type": "null"}`
}

// Returns a type constant for this NullSchema.
func (*NullSchema) Type() int {
	return Null
}

// Returns a type name for this NullSchema.
func (*NullSchema) GetName() string {
	return type_null
}

// Doesn't return anything valuable for NullSchema.
func (*NullSchema) Prop(key string) (string, bool) {
	return "", false
}

// Checks whether the given value is writeable to this schema.
func (*NullSchema) Validate(v reflect.Value) bool {
	// Check if the value is something that can be null
	switch v.Kind() {
	case reflect.Interface:
		return v.IsNil()
	case reflect.Array:
		return v.Cap() == 0
	case reflect.Slice:
		return v.IsNil() || v.Cap() == 0
	case reflect.Map:
		return len(v.MapKeys()) == 0
	case reflect.String:
		return len(v.String()) == 0
	case reflect.Float32:
		// Should NaN floats be treated as null?
		return math.IsNaN(v.Float())
	case reflect.Float64:
		// Should NaN floats be treated as null?
		return math.IsNaN(v.Float())
	case reflect.Ptr:
		return v.IsNil()
	case reflect.Invalid:
		return true
	}

	// Nothing else in particular, so this should not validate?
	return false
}

func (this *NullSchema) MarshalJSON() ([]byte, error) {
	return []byte(`"null"`), nil
}

// RecordSchema implements Schema and represents Avro record type.
type RecordSchema struct {
	Name       string   `json:"name,omitempty"`
	Namespace  string   `json:"namespace,omitempty"`
	Doc        string   `json:"doc,omitempty"`
	Aliases    []string `json:"aliases,omitempty"`
	Properties map[string]string
	Fields     []*SchemaField `json:"fields"`
}

// Returns a JSON representation of RecordSchema.
func (this *RecordSchema) String() string {
	bytes, err := json.MarshalIndent(this, "", "    ")
	if err != nil {
		panic(err)
	}

	return string(bytes)
}

func (this *RecordSchema) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type      string         `json:"type,omitempty"`
		Namespace string         `json:"namespace,omitempty"`
		Name      string         `json:"name,omitempty"`
		Doc       string         `json:"doc,omitempty"`
		Aliases   []string       `json:"aliases,omitempty"`
		Fields    []*SchemaField `json:"fields"`
	}{
		Type:      "record",
		Namespace: this.Namespace,
		Name:      this.Name,
		Doc:       this.Doc,
		Aliases:   this.Aliases,
		Fields:    this.Fields,
	})
}

// Returns a type constant for this RecordSchema.
func (*RecordSchema) Type() int {
	return Record
}

// Returns a record name for this RecordSchema.
func (this *RecordSchema) GetName() string {
	return this.Name
}

// Gets a custom non-reserved string property from this schema and a bool representing if it exists.
func (this *RecordSchema) Prop(key string) (string, bool) {
	if this.Properties != nil {
		if prop, ok := this.Properties[key]; ok {
			return prop, true
		}
	}

	return "", false
}

// Checks whether the given value is writeable to this schema.
func (rs *RecordSchema) Validate(v reflect.Value) bool {
	v = dereference(v)
	if v.Kind() != reflect.Struct || !v.CanAddr() || !v.CanInterface() {
		return false
	}
	rec, ok := v.Interface().(GenericRecord)
	if !ok {
		// This is not a generic record and is likely a specific record. Hence
		// use the basic check.
		return v.Kind() == reflect.Struct
	}

	field_count := 0
	for key, val := range rec.fields {
		for idx := range rs.Fields {
			// key.Name must have rs.Fields[idx].Name as a suffix
			if len(rs.Fields[idx].Name) <= len(key) {
				lhs := key[len(key)-len(rs.Fields[idx].Name) : len(key)]
				if lhs == rs.Fields[idx].Name {
					if !rs.Fields[idx].Type.Validate(reflect.ValueOf(val)) {
						return false
					}
					field_count++
					break
				}
			}
		}
	}

	// All of the fields set must be accounted for in the union.
	if field_count < len(rec.fields) {
		return false
	}

	return true
}

// RecursiveSchema implements Schema and represents Avro record type without a definition (e.g. that should be looked up).
type RecursiveSchema struct {
	Actual *RecordSchema
}

func newRecursiveSchema(parent *RecordSchema) *RecursiveSchema {
	return &RecursiveSchema{
		Actual: parent,
	}
}

// Returns a JSON representation of RecursiveSchema.
func (this *RecursiveSchema) String() string {
	return fmt.Sprintf(`{"type": "%s"}`, this.Actual.GetName())
}

// Returns a type constant for this RecursiveSchema.
func (*RecursiveSchema) Type() int {
	return Recursive
}

// Returns a record name for enclosed RecordSchema.
func (this *RecursiveSchema) GetName() string {
	return this.Actual.GetName()
}

// Doesn't return anything valuable for RecursiveSchema.
func (*RecursiveSchema) Prop(key string) (string, bool) {
	return "", false
}

// Checks whether the given value is writeable to this schema.
func (this *RecursiveSchema) Validate(v reflect.Value) bool {
	return this.Actual.Validate(v)
}

func (this *RecursiveSchema) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`"%s"`, this.Actual.GetName())), nil
}

// SchemaField represents a schema field for Avro record.
type SchemaField struct {
	Name    string      `json:"name,omitempty"`
	Doc     string      `json:"doc,omitempty"`
	Default interface{} `json:"default"`
	Type    Schema      `json:"type,omitempty"`
}

func (this *SchemaField) MarshalJSON() ([]byte, error) {
	if this.Type.Type() == Null || (this.Type.Type() == Union && this.Type.(*UnionSchema).Types[0].Type() == Null) {
		return json.Marshal(struct {
			Name    string      `json:"name,omitempty"`
			Doc     string      `json:"doc,omitempty"`
			Default interface{} `json:"default"`
			Type    Schema      `json:"type,omitempty"`
		}{
			Name:    this.Name,
			Doc:     this.Doc,
			Default: this.Default,
			Type:    this.Type,
		})
	} else {
		return json.Marshal(struct {
			Name    string      `json:"name,omitempty"`
			Doc     string      `json:"doc,omitempty"`
			Default interface{} `json:"default,omitempty"`
			Type    Schema      `json:"type,omitempty"`
		}{
			Name:    this.Name,
			Doc:     this.Doc,
			Default: this.Default,
			Type:    this.Type,
		})
	}
}

// Returns a JSON representation of SchemaField.
func (this *SchemaField) String() string {
	return fmt.Sprintf("[SchemaField: Name: %s, Doc: %s, Default: %v, Type: %s]", this.Name, this.Doc, this.Default, this.Type)
}

// EnumSchema implements Schema and represents Avro enum type.
type EnumSchema struct {
	Name       string
	Namespace  string
	Aliases    []string
	Doc        string
	Symbols    []string
	Properties map[string]string
}

// Returns a JSON representation of EnumSchema.
func (this *EnumSchema) String() string {
	bytes, err := json.MarshalIndent(this, "", "    ")
	if err != nil {
		panic(err)
	}

	return string(bytes)
}

// Returns a type constant for this EnumSchema.
func (*EnumSchema) Type() int {
	return Enum
}

// Returns an enum name for this EnumSchema.
func (this *EnumSchema) GetName() string {
	return this.Name
}

// Gets a custom non-reserved string property from this schema and a bool representing if it exists.
func (this *EnumSchema) Prop(key string) (string, bool) {
	if this.Properties != nil {
		if prop, ok := this.Properties[key]; ok {
			return prop, true
		}
	}

	return "", false
}

// Checks whether the given value is writeable to this schema.
func (this *EnumSchema) Validate(v reflect.Value) bool {
	//TODO implement
	return true
}

func (this *EnumSchema) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type      string   `json:"type,omitempty"`
		Namespace string   `json:"namespace,omitempty"`
		Name      string   `json:"name,omitempty"`
		Doc       string   `json:"doc,omitempty"`
		Symbols   []string `json:"symbols,omitempty"`
	}{
		Type:      "enum",
		Namespace: this.Namespace,
		Name:      this.Name,
		Doc:       this.Doc,
		Symbols:   this.Symbols,
	})
}

// ArraySchema implements Schema and represents Avro array type.
type ArraySchema struct {
	Items      Schema
	Properties map[string]string
}

// Returns a JSON representation of ArraySchema.
func (this *ArraySchema) String() string {
	bytes, err := json.MarshalIndent(this, "", "    ")
	if err != nil {
		panic(err)
	}

	return string(bytes)
}

// Returns a type constant for this ArraySchema.
func (*ArraySchema) Type() int {
	return Array
}

// Returns a type name for this ArraySchema.
func (*ArraySchema) GetName() string {
	return type_array
}

// Gets a custom non-reserved string property from this schema and a bool representing if it exists.
func (this *ArraySchema) Prop(key string) (string, bool) {
	if this.Properties != nil {
		if prop, ok := this.Properties[key]; ok {
			return prop, true
		}
	}

	return "", false
}

// Checks whether the given value is writeable to this schema.
func (this *ArraySchema) Validate(v reflect.Value) bool {
	v = dereference(v)

	// This needs to be a slice
	return v.Kind() == reflect.Slice || v.Kind() == reflect.Array
}

func (this *ArraySchema) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type  string `json:"type,omitempty"`
		Items Schema `json:"items,omitempty"`
	}{
		Type:  "array",
		Items: this.Items,
	})
}

// MapSchema implements Schema and represents Avro map type.
type MapSchema struct {
	Values     Schema
	Properties map[string]string
}

// Returns a JSON representation of MapSchema.
func (this *MapSchema) String() string {
	bytes, err := json.MarshalIndent(this, "", "    ")
	if err != nil {
		panic(err)
	}

	return string(bytes)
}

// Returns a type constant for this MapSchema.
func (*MapSchema) Type() int {
	return Map
}

// Returns a type name for this MapSchema.
func (*MapSchema) GetName() string {
	return type_map
}

// Gets a custom non-reserved string property from this schema and a bool representing if it exists.
func (this *MapSchema) Prop(key string) (string, bool) {
	if this.Properties != nil {
		if prop, ok := this.Properties[key]; ok {
			return prop, true
		}
	}
	return "", false
}

// Checks whether the given value is writeable to this schema.
func (this *MapSchema) Validate(v reflect.Value) bool {
	v = dereference(v)

	return v.Kind() == reflect.Map && v.Type().Key().Kind() == reflect.String
}

func (this *MapSchema) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type   string `json:"type,omitempty"`
		Values Schema `json:"values,omitempty"`
	}{
		Type:   "map",
		Values: this.Values,
	})
}

// UnionSchema implements Schema and represents Avro union type.
type UnionSchema struct {
	Types []Schema
}

// Returns a JSON representation of UnionSchema.
func (this *UnionSchema) String() string {
	bytes, err := json.MarshalIndent(this, "", "    ")
	if err != nil {
		panic(err)
	}

	return fmt.Sprintf(`{"type": %s}`, string(bytes))
}

// Returns a type constant for this UnionSchema.
func (*UnionSchema) Type() int {
	return Union
}

// Returns a type name for this UnionSchema.
func (*UnionSchema) GetName() string {
	return type_union
}

// Doesn't return anything valuable for UnionSchema.
func (*UnionSchema) Prop(key string) (string, bool) {
	return "", false
}

func (this *UnionSchema) GetType(v reflect.Value) int {
	if this.Types != nil {
		for i := range this.Types {
			if t := this.Types[i]; t.Validate(v) {
				return i
			}
		}
	}

	return -1
}

// Checks whether the given value is writeable to this schema.
func (this *UnionSchema) Validate(v reflect.Value) bool {
	v = dereference(v)
	for i := range this.Types {
		if t := this.Types[i]; t.Validate(v) {
			return true
		}
	}

	return false
}

func (this *UnionSchema) MarshalJSON() ([]byte, error) {
	return json.Marshal(this.Types)
}

// FixedSchema implements Schema and represents Avro fixed type.
type FixedSchema struct {
	Namespace  string
	Name       string
	Size       int
	Properties map[string]string
}

// Returns a JSON representation of FixedSchema.
func (this *FixedSchema) String() string {
	bytes, err := json.MarshalIndent(this, "", "    ")
	if err != nil {
		panic(err)
	}

	return string(bytes)
}

// Returns a type constant for this FixedSchema.
func (*FixedSchema) Type() int {
	return Fixed
}

// Returns a fixed name for this FixedSchema.
func (this *FixedSchema) GetName() string {
	return this.Name
}

// Gets a custom non-reserved string property from this schema and a bool representing if it exists.
func (this *FixedSchema) Prop(key string) (string, bool) {
	if this.Properties != nil {
		if prop, ok := this.Properties[key]; ok {
			return prop, true
		}
	}
	return "", false
}

// Checks whether the given value is writeable to this schema.
func (this *FixedSchema) Validate(v reflect.Value) bool {
	v = dereference(v)

	return (v.Kind() == reflect.Array || v.Kind() == reflect.Slice) && v.Type().Elem().Kind() == reflect.Uint8 && v.Len() == this.Size
}

func (this *FixedSchema) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type string `json:"type,omitempty"`
		Size int    `json:"size,omitempty"`
		Name string `json:"name,omitempty"`
	}{
		Type: "fixed",
		Size: this.Size,
		Name: this.Name,
	})
}

func GetFullName(schema Schema) string {
	switch sch := schema.(type) {
	case *RecordSchema:
		{
			if sch.Namespace == "" {
				return sch.GetName()
			} else {
				return fmt.Sprintf("%s.%s", sch.Namespace, sch.GetName())
			}
		}
	case *EnumSchema:
		{
			if sch.Namespace == "" {
				return sch.GetName()
			} else {
				return fmt.Sprintf("%s.%s", sch.Namespace, sch.GetName())
			}
		}
	case *FixedSchema:
		{
			if sch.Namespace == "" {
				return sch.GetName()
			} else {
				return fmt.Sprintf("%s.%s", sch.Namespace, sch.GetName())
			}
		}
	default:
		return schema.GetName()
	}
}

// Parses a given file.
// May return an error if schema is not parsable or file does not exist.
func ParseSchemaFile(file string) (Schema, error) {
	fileContents, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, err
	}

	return ParseSchema(string(fileContents))
}

// Parses a given schema without provided schemas to reuse.
// Equivalent to call ParseSchemaWithResistry(rawSchema, make(map[string]Schema))
// May return an error if schema is not parsable or has insufficient information about any type.
func ParseSchema(rawSchema string) (Schema, error) {
	return ParseSchemaWithRegistry(rawSchema, make(map[string]Schema))
}

// Parses a given schema using the provided registry for type lookup.
// Registry will be filled up during parsing.
// May return an error if schema is not parsable or has insufficient information about any type.
func ParseSchemaWithRegistry(rawSchema string, schemas map[string]Schema) (Schema, error) {
	var schema interface{}
	if err := json.Unmarshal([]byte(rawSchema), &schema); err != nil {
		schema = rawSchema
	}

	return schemaByType(schema, schemas, "")
}

// MustParseSchema is like ParseSchema, but panics if the given schema
// cannot be parsed.
func MustParseSchema(rawSchema string) Schema {
	s, err := ParseSchema(rawSchema)
	if err != nil {
		panic(err)
	}
	return s
}

func schemaByType(i interface{}, registry map[string]Schema, namespace string) (Schema, error) {
	switch v := i.(type) {
	case nil:
		return new(NullSchema), nil
	case string:
		switch v {
		case type_null:
			return new(NullSchema), nil
		case type_boolean:
			return new(BooleanSchema), nil
		case type_int:
			return new(IntSchema), nil
		case type_long:
			return new(LongSchema), nil
		case type_float:
			return new(FloatSchema), nil
		case type_double:
			return new(DoubleSchema), nil
		case type_bytes:
			return new(BytesSchema), nil
		case type_string:
			return new(StringSchema), nil
		default:
			schema, ok := registry[getFullName(v, namespace)]
			if !ok {
				return nil, fmt.Errorf("Unknown type name: %s", v)
			}

			return schema, nil
		}
	case map[string][]interface{}:
		return parseUnionSchema(v[schema_typeField], registry, namespace)
	case map[string]interface{}:
		switch v[schema_typeField] {
		case type_null:
			return new(NullSchema), nil
		case type_boolean:
			return new(BooleanSchema), nil
		case type_int:
			return new(IntSchema), nil
		case type_long:
			return new(LongSchema), nil
		case type_float:
			return new(FloatSchema), nil
		case type_double:
			return new(DoubleSchema), nil
		case type_bytes:
			return new(BytesSchema), nil
		case type_string:
			return new(StringSchema), nil
		case type_array:
			items, err := schemaByType(v[schema_itemsField], registry, namespace)
			if err != nil {
				return nil, err
			}
			return &ArraySchema{Items: items, Properties: getProperties(v)}, nil
		case type_map:
			values, err := schemaByType(v[schema_valuesField], registry, namespace)
			if err != nil {
				return nil, err
			}
			return &MapSchema{Values: values, Properties: getProperties(v)}, nil
		case type_enum:
			return parseEnumSchema(v, registry, namespace)
		case type_fixed:
			return parseFixedSchema(v, registry, namespace)
		case type_record:
			return parseRecordSchema(v, registry, namespace)
		}
	case []interface{}:
		return parseUnionSchema(v, registry, namespace)
	}

	return nil, InvalidSchema
}

func parseEnumSchema(v map[string]interface{}, registry map[string]Schema, namespace string) (Schema, error) {
	symbols := make([]string, len(v[schema_symbolsField].([]interface{})))
	for i, symbol := range v[schema_symbolsField].([]interface{}) {
		symbols[i] = symbol.(string)
	}

	schema := &EnumSchema{Name: v[schema_nameField].(string), Symbols: symbols}
	setOptionalField(&schema.Namespace, v, schema_namespaceField)
	setOptionalField(&schema.Doc, v, schema_docField)
	schema.Properties = getProperties(v)

	return addSchema(getFullName(v[schema_nameField].(string), namespace), schema, registry)
}

func parseFixedSchema(v map[string]interface{}, registry map[string]Schema, namespace string) (Schema, error) {
	if size, ok := v[schema_sizeField].(float64); !ok {
		return nil, InvalidFixedSize
	} else {
		schema := &FixedSchema{Name: v[schema_nameField].(string), Size: int(size), Properties: getProperties(v)}
		setOptionalField(&schema.Namespace, v, schema_namespaceField)
		return addSchema(getFullName(v[schema_nameField].(string), namespace), schema, registry)
	}
}

func parseUnionSchema(v []interface{}, registry map[string]Schema, namespace string) (Schema, error) {
	types := make([]Schema, len(v))
	var err error
	for i := range v {
		types[i], err = schemaByType(v[i], registry, namespace)
		if err != nil {
			return nil, err
		}
	}
	return &UnionSchema{Types: types}, nil
}

func parseRecordSchema(v map[string]interface{}, registry map[string]Schema, namespace string) (Schema, error) {
	schema := &RecordSchema{Name: v[schema_nameField].(string)}
	setOptionalField(&schema.Namespace, v, schema_namespaceField)
	setOptionalField(&namespace, v, schema_namespaceField)
	setOptionalField(&schema.Doc, v, schema_docField)
	addSchema(getFullName(v[schema_nameField].(string), namespace), newRecursiveSchema(schema), registry)
	fields := make([]*SchemaField, len(v[schema_fieldsField].([]interface{})))
	for i := range fields {
		field, err := parseSchemaField(v[schema_fieldsField].([]interface{})[i], registry, namespace)
		if err != nil {
			return nil, err
		}
		fields[i] = field
	}
	schema.Fields = fields
	schema.Properties = getProperties(v)

	return schema, nil
}

func parseSchemaField(i interface{}, registry map[string]Schema, namespace string) (*SchemaField, error) {
	switch v := i.(type) {
	case map[string]interface{}:
		name, ok := v[schema_nameField].(string)
		if !ok {
			return nil, fmt.Errorf("Schema field name missing")
		}
		schemaField := &SchemaField{Name: name}
		setOptionalField(&schemaField.Doc, v, schema_docField)
		fieldType, err := schemaByType(v[schema_typeField], registry, namespace)
		if err != nil {
			return nil, err
		}
		schemaField.Type = fieldType
		if def, exists := v[schema_defaultField]; exists {
			switch def.(type) {
			case float64:
				// JSON treats all numbers as float64 by default
				switch schemaField.Type.Type() {
				case Int:
					var converted int32 = int32(def.(float64))
					schemaField.Default = converted
				case Long:
					var converted int64 = int64(def.(float64))
					schemaField.Default = converted
				case Float:
					var converted float32 = float32(def.(float64))
					schemaField.Default = converted

				default:
					schemaField.Default = def
				}
			default:
				schemaField.Default = def
			}
		}

		return schemaField, nil
	}

	return nil, InvalidSchema
}

func setOptionalField(where *string, v map[string]interface{}, fieldName string) {
	if field, exists := v[fieldName]; exists {
		*where = field.(string)
	}
}

func addSchema(name string, schema Schema, schemas map[string]Schema) (Schema, error) {
	if schemas != nil {
		if sch, ok := schemas[name]; ok {
			return sch, nil
		} else {
			schemas[name] = schema
		}
	}

	return schema, nil
}

func getFullName(name string, namespace string) string {
	if len(namespace) > 0 {
		return namespace + "." + name
	} else {
		return name
	}
}

// gets custom string properties from a given schema
func getProperties(v map[string]interface{}) map[string]string {
	props := make(map[string]string)

	for name, value := range v {
		if !isReserved(name) {
			if val, ok := value.(string); ok {
				props[name] = val
			}
		}
	}

	return props
}

func isReserved(name string) bool {
	switch name {
	case schema_aliasesField, schema_docField, schema_fieldsField, schema_itemsField, schema_nameField,
		schema_namespaceField, schema_sizeField, schema_symbolsField, schema_typeField, schema_valuesField:
		return true
	}

	return false
}

func dereference(v reflect.Value) reflect.Value {
	if v.Kind() == reflect.Ptr {
		return v.Elem()
	}

	return v
}
