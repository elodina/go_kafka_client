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

package avro

import (
	"github.com/elodina/go-avro"
	"reflect"
	"testing"
)

const (
	schemaRepositoryUrl = "http://localhost:8081"
	rawMetricsSchema    = `{"namespace": "net.elodina.kafka.metrics","type": "record","name": "Timings","fields": [{"name": "id", "type": "long"},{"name": "timings",  "type": {"type":"array", "items": "long"} }]}`
)

func TestAvroKafkaEncoderDecoder(t *testing.T) {
	encoder := NewKafkaAvroEncoder(schemaRepositoryUrl)

	schema, err := avro.ParseSchema(rawMetricsSchema)
	assert(t, err, nil)

	record := avro.NewGenericRecord(schema)
	record.Set("id", int64(3))
	record.Set("timings", []int64{123456, 654321})

	bytes, err := encoder.Encode(record)
	assert(t, err, nil)

	decoder := NewKafkaAvroDecoder(schemaRepositoryUrl)
	decoded, err := decoder.Decode(bytes)
	assert(t, err, nil)

	decodedRecord, ok := decoded.(*avro.GenericRecord)
	assert(t, ok, true)

	assert(t, decodedRecord.Get("id"), record.Get("id"))
	assert(t, decodedRecord.Get("timings"), []interface{}{int64(123456), int64(654321)})
}

func assert(t *testing.T, actual interface{}, expected interface{}) {
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expected %v, actual %v", expected, actual)
	}
}

func assertNot(t *testing.T, actual interface{}, expected interface{}) {
	if reflect.DeepEqual(actual, expected) {
		t.Errorf("%v should not be %v", actual, expected)
	}
}
