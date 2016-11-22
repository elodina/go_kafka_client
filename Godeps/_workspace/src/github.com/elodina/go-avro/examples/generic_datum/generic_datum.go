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

package main

import (
	"bytes"
	"fmt"
	"github.com/elodina/go-avro"
)

var rawSchema = `{
   "type":"record",
   "name":"TestRecord",
   "fields":[
      {
         "name":"value",
         "type":"int"
      },
      {
         "name":"defval1",
         "type":"int",
         "default":1234
      },
      {
         "name":"defval2",
         "type":"boolean",
         "default":true
      },
      {
         "name":"defval3",
         "type":"double",
         "default":567.89
      },
      {
         "name":"defval4",
         "type":"long",
         "default":2345
      },
      {
         "name":"union",
         "type":[ {
		    "name":"union1",
			"type":"string"
         },
         {
            "name":"union2",
            "type":"boolean"
         },
         {
            "name":"union3",
            "type":"int"
         }],
         "default":"null"
	  },
      {
         "name":"rec",
         "type":{
            "type":"array",
            "items":{
               "type":"record",
               "name":"TestRecord2",
               "fields":[
                  {
                     "name":"stringValue",
                     "type":"string"
                  },
                  {
                     "name":"intValue",
                     "type":"int"
                  },
                  {
                     "name" : "fruits",
                     "type" : {
                        "type" : "enum",
                        "name" : "FruitNames",
                        "symbols" : [ "apple", "banana", "pear", "plum" ]
                     }
                  }
               ]
            }
         }
      }
   ]
}`

var rawPrimitiveSchema = `{"type" : "string"}`

func main() {
	// Parse the schema first
	schema := avro.MustParseSchema(rawSchema)

	// Create a record for a given schema
	record := avro.NewGenericRecord(schema)
	value := int32(3)
	record.Set("value", value)

	var unionValue int32 = 1234
	record.Set("union", unionValue)

	subRecords := make([]*avro.GenericRecord, 2)
	subRecord0 := avro.NewGenericRecord(schema)
	subRecord0.Set("stringValue", "Hello")
	subRecord0.Set("intValue", int32(1))
	subRecord0.Set("fruits", "apple")
	subRecords[0] = subRecord0

	subRecord1 := avro.NewGenericRecord(schema)
	subRecord1.Set("stringValue", "World")
	subRecord1.Set("intValue", int32(2))
	subRecord1.Set("fruits", "pear")
	subRecords[1] = subRecord1

	record.Set("rec", subRecords)

	writer := avro.NewGenericDatumWriter()
	// SetSchema must be called before calling Write
	writer.SetSchema(schema)

	// Create a new Buffer and Encoder to write to this Buffer
	buffer := new(bytes.Buffer)
	encoder := avro.NewBinaryEncoder(buffer)

	// Write the record
	err := writer.Write(record, encoder)
	if err != nil {
		panic(err)
	}

	reader := avro.NewGenericDatumReader()
	// SetSchema must be called before calling Read
	reader.SetSchema(schema)

	// Create a new Decoder with a given buffer
	decoder := avro.NewBinaryDecoder(buffer.Bytes())

	decodedRecord := avro.NewGenericRecord(schema)
	// Read data into given GenericRecord with a given Decoder. The first parameter to Read should be something to read into
	err = reader.Read(decodedRecord, decoder)
	if err != nil {
		panic(err)
	}

	decodedValue := decodedRecord.Get("value").(int32)
	if value != decodedValue {
		panic("Something went terribly wrong!")
	}
	fmt.Printf("Read a value: %d\n", decodedValue)

	decodedUnionValue := decodedRecord.Get("union").(int32)
	if unionValue != decodedUnionValue {
		panic("Something went terribly wrong!")
	}
	fmt.Printf("Read a union value: %d\n", decodedUnionValue)

	defVal1, ok := decodedRecord.Get("defval1").(int32)
	if !ok {
		panic("Something went terribly wrong!")
	}
	defVal2, ok := decodedRecord.Get("defval2").(bool)
	if !ok {
		panic("Something went terribly wrong!")
	}
	defVal3, ok := decodedRecord.Get("defval3").(float64)
	if !ok {
		panic("Something went terribly wrong!")
	}
	defVal4, ok := decodedRecord.Get("defval4").(int64)
	if !ok {
		panic("Something went terribly wrong!")
	}
	fmt.Println("Read the first default value: ", defVal1)
	fmt.Println("Read the second default value: ", defVal2)
	fmt.Println("Read the third default value: ", defVal3)
	fmt.Println("Read the fourth default value: ", defVal4)

	decodedArray := decodedRecord.Get("rec").([]interface{})
	if len(decodedArray) != 2 {
		panic("Something went terribly wrong!")
	}

	for index, decodedSubRecord := range decodedArray {
		r := decodedSubRecord.(*avro.GenericRecord)
		fmt.Printf("Read a subrecord %d string value: %s\n", index, r.Get("stringValue"))
		fmt.Printf("Read a subrecord %d int value: %d\n", index, r.Get("intValue"))
		fmt.Println("Read a subrecord:", index, "value:", r.Get("fruits"))
	}

	// The same should work for primitives
	primitiveSchema := avro.MustParseSchema(rawPrimitiveSchema)

	// Define a primitive value to encode
	primitiveValue := "hello world"

	// Create a new Buffer and Encoder to write to this Buffer
	buffer = new(bytes.Buffer)
	encoder = avro.NewBinaryEncoder(buffer)

	primitiveWriter := avro.NewGenericDatumWriter()
	// SetSchema must be called before calling Write
	primitiveWriter.SetSchema(primitiveSchema)

	// Write the primitive
	err = primitiveWriter.Write(primitiveValue, encoder)
	if err != nil {
		panic(err)
	}

	reader = avro.NewGenericDatumReader()
	// SetSchema must be called before calling Read
	reader.SetSchema(primitiveSchema)

	// Create a new Decoder with a given buffer
	decoder = avro.NewBinaryDecoder(buffer.Bytes())

	decodedPrimitive := ""
	// Read data into given GenericRecord with a given Decoder. The first parameter to Read should be something to read into
	err = reader.Read(&decodedPrimitive, decoder)
	if err != nil {
		panic(err)
	}

	if primitiveValue != decodedPrimitive {
		panic("Something went terribly wrong!")
	}
	fmt.Printf("Read a primitive value: %s\n", decodedPrimitive)

}
