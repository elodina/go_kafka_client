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

package go_kafka_client

import "encoding/binary"

type Encoder interface {
	Encode(interface{}) ([]byte, error)
}

type Decoder interface {
	Decode([]byte) (interface{}, error)
}

type StringEncoder struct{}

func (this *StringEncoder) Encode(what interface{}) ([]byte, error) {
	if what == nil {
		return nil, nil
	}
	return []byte(what.(string)), nil
}

type StringDecoder struct{}

func (this *StringDecoder) Decode(bytes []byte) (interface{}, error) {
	return string(bytes), nil
}

type Int32Encoder struct{}

func (this *Int32Encoder) Encode(what interface{}) ([]byte, error) {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, what.(uint32))
	return buf, nil
}

type Int32Decoder struct{}

func (this *Int32Decoder) Decode(bytes []byte) (interface{}, error) {
	return binary.LittleEndian.Uint32(bytes), nil
}

type ByteEncoder struct{}

func (this *ByteEncoder) Encode(what interface{}) ([]byte, error) {
	if what == nil {
		return nil, nil
	}
	return what.([]byte), nil
}

type ByteDecoder struct{}

func (this *ByteDecoder) Decode(bytes []byte) (interface{}, error) {
	return bytes, nil
}
