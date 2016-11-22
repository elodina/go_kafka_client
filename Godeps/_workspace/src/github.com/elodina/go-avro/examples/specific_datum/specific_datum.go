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

// Define the schema to read
var rawSchema = `{
     "type": "record",
     "name": "TestRecord",
     "fields": [
       { "name": "value", "type": "int" }
     ]
}`

// Define a struct that will match a schema definition. Some fields may be omitted, but all fields to map should be exported
type TestRecord struct {
	Value int32
}

func main() {
	// Parse the schema first
	schema, err := avro.ParseSchema(rawSchema)
	if err != nil {
		// Should not happen if the schema is valid
		panic(err)
	}

	// Create a test record to encode
	record := new(TestRecord)
	record.Value = 3

	writer := avro.NewSpecificDatumWriter()
	// SetSchema must be called before calling Write
	writer.SetSchema(schema)

	// Create a new Buffer and Encoder to write to this Buffer
	buffer := new(bytes.Buffer)
	encoder := avro.NewBinaryEncoder(buffer)

	// Write the record
	writer.Write(record, encoder)

	reader := avro.NewSpecificDatumReader()
	// SetSchema must be called before calling Read
	reader.SetSchema(schema)

	// Create a new Decoder with a given buffer
	decoder := avro.NewBinaryDecoder(buffer.Bytes())

	// Create a new TestRecord to decode data into
	decodedRecord := new(TestRecord)

	// Read data into a given record with a given Decoder.
	err = reader.Read(decodedRecord, decoder)
	if err != nil {
		panic(err)
	}

	if record.Value != decodedRecord.Value {
		panic("Something went terribly wrong!")
	}

	fmt.Printf("Read a value: %d\n", decodedRecord.Value)
}
