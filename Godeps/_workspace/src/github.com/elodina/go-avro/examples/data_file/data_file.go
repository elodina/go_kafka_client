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
	"fmt"
	"github.com/elodina/go-avro"
)

// Fields to map should be exported
type SomeComplexType struct {
	StringArray []string
	LongArray   []int64
	EnumField   *avro.GenericEnum
	MapOfInts   map[string]int32
	UnionField  string
	FixedField  []byte
	RecordField *SomeAnotherType
}

// Fields to map should be exported here as well
type SomeAnotherType struct {
	LongRecordField   int64
	StringRecordField string
	IntRecordField    int32
	FloatRecordField  float32
}

func main() {
	// Provide a filename to read and a DatumReader to manage the reading itself
	specificReader, err := avro.NewDataFileReader("complex.avro", avro.NewSpecificDatumReader())
	if err != nil {
		// Should not actually happen
		panic(err)
	}

	for {
		// Note: should ALWAYS pass in a pointer, e.g. specificReader.Next(SomeComplexType{}) will NOT work
		obj := new(SomeComplexType)
		ok, err := specificReader.Next(obj)
		if !ok {
			if err != nil {
				panic(err)
			}
			break
		} else {
			fmt.Printf("%#v\n", obj)
		}
	}
}
