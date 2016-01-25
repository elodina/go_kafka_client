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
	"fmt"
	"reflect"
	"strings"
)

func findField(where reflect.Value, name string) (reflect.Value, error) {
	if where.Kind() == reflect.Ptr {
		where = where.Elem()
	}
	t := where.Type()
	rm := reflectMap[t]
	if rm == nil {
		rm = reflectBuildRi(t)
	}
	if rf, ok := rm.names[name]; ok {
		return where.FieldByIndex(rf), nil
	}
	return reflect.Value{}, fmt.Errorf("Field %s does not exist in %s", name, t.Name())
}

func reflectBuildRi(t reflect.Type) *reflectInfo {
	rm := &reflectInfo{
		names: make(map[string][]int),
	}
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if strings.ToLower(f.Name[:1]) != f.Name[:1] {
			tag := f.Tag.Get("avro")
			if tag != "" {
				rm.names[tag] = f.Index
			} else {
				rm.names[f.Name] = f.Index
				rm.names[strings.ToLower(f.Name[:1])+f.Name[1:]] = f.Index
			}
		}
	}

	// copy the map instead of dealing with locking
	m := make(map[reflect.Type]*reflectInfo, len(reflectMap)+1)
	for k, v := range reflectMap {
		m[k] = v
	}
	m[t] = rm
	reflectMap = m
	return rm
}

var reflectMap map[reflect.Type]*reflectInfo

type reflectInfo struct {
	names map[string][]int
}
