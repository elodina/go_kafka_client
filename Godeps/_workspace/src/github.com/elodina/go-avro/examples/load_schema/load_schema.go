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

func main() {
	// Define a schema that does not contain information about type "User"
	dependentSchema := `{ "type": "record",
 "namespace": "example.avro",
 "name": "System",
 "fields": [
     {"name": "user", "type": "User"}
 ]
}`

	// Load schemas in a given folder, where the information about type "User" is provided
	schemas := avro.LoadSchemas("schemas/")

	// Now parse the dependent schema providing it loaded schemas
	// It should now be able to resolve type "User" as it is in the same namespace as type "System"
	schema, err := avro.ParseSchemaWithRegistry(dependentSchema, schemas)
	if err != nil {
		panic(err)
	}

	// Try it out and write a GenericRecord to verify everything works fine
	user := avro.NewGenericRecord(schema)
	user.Set("name", "Some User")

	record := avro.NewGenericRecord(schema)
	record.Set("user", user)

	writer := avro.NewGenericDatumWriter()
	// SetSchema must be called before calling Write
	writer.SetSchema(schema)

	// Create a new Buffer and Encoder to write to this Buffer
	buffer := new(bytes.Buffer)
	encoder := avro.NewBinaryEncoder(buffer)

	// Write the record
	writer.Write(record, encoder)

	reader := avro.NewGenericDatumReader()
	// SetSchema must be called before calling Read
	reader.SetSchema(schema)

	// Create a new Decoder with a given buffer
	decoder := avro.NewBinaryDecoder(buffer.Bytes())

	// Read a new GenericRecord with a given Decoder. The first parameter to Read should be nil for GenericDatumReader
	decodedRecord := avro.NewGenericRecord(schema)
	err = reader.Read(decodedRecord, decoder)
	if err != nil {
		panic(err)
	}

	// Check whether the user got decoded correctly
	decodedUser := decodedRecord.Get("user")
	decodedUserName := decodedUser.(*avro.GenericRecord).Get("name")
	if decodedUserName.(string) != "Some User" {
		panic("Something went terribly wrong!")
	}

	fmt.Printf("Got a user name back: %s\n", decodedUserName)
}
