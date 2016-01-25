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
//    "github.com/stealthly/go-avro/sch/interop"
    "bytes"
    "github.com/stealthly/go-avro"
    "encoding/hex"
    "fmt"
)

func main() {
//    rec := interop.NewSubRecord1()
//    rec.Map1 = map[string]string{}
//
//    buffer := &bytes.Buffer{}
//    enc := avro.NewBinaryEncoder(buffer)
//
//    w := avro.NewSpecificDatumWriter()
//    w.SetSchema(rec.Schema())
//
//    err := w.Write(rec, enc)
//    if err != nil {
//        panic(err)
//    }
//
//    fmt.Println(hex.Dump(buffer.Bytes()))

    schema := avro.MustParseSchema(`{
    "type": "record",
    "namespace": "test.public.avro.interop",
    "name": "SubRecord1",
    "fields": [
        {
            "name": "map1",
            "type": {
                "type": "array",
                "items": "string"
            }
        }
    ]
}`)

    buffer := &bytes.Buffer{}
    enc := avro.NewBinaryEncoder(buffer)
    w := avro.NewGenericDatumWriter()
    w.SetSchema(schema)

    rec := avro.NewGenericRecord(schema)
    rec.Set("map1", []string{})

    err := w.Write(rec, enc)
    if err != nil {
        panic(err)
    }

    fmt.Println(hex.Dump(buffer.Bytes()))
}
