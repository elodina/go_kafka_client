/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package go_kafka_client

import (
	"github.com/jimlawless/cfg"
	"os"
	"log"
	"testing"
	"reflect"
	"math/rand"
	"sync"
	"container/ring"
	"hash/fnv"
)

var Logger = log.New(os.Stdout, "[stealthly] ", log.LstdFlags)

func LoadConfiguration(path string) (map[string]string, error) {
	cfgMap := make(map[string]string)
	err := cfg.Load(path, cfgMap)

	return cfgMap, err
}

func Assert(t *testing.T, actual interface{}, expected interface{}) {
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expected %v, actual %v", expected, actual)
	}
}

func AssertNot(t *testing.T, actual interface{}, expected interface{}) {
	if reflect.DeepEqual(actual, expected) {
		t.Errorf("%v should not be %v", actual, expected)
	}
}

func InLock(lock *sync.Mutex, fun func()) {
	lock.Lock()
	defer lock.Unlock()

	fun()
}

func ShuffleArray(src interface{}, dest interface{}) {
	rSrc := reflect.ValueOf(src).Elem()
	rDest := reflect.ValueOf(dest).Elem()

	perm := rand.Perm(rSrc.Len())
	for i, v := range perm {
		rDest.Index(v).Set(rSrc.Index(i))
	}
}

func CircularIterator(src interface{}) *ring.Ring {
	arr := reflect.ValueOf(src).Elem()
	circle := ring.New(arr.Len())
	for i := 0; i < arr.Len(); i++ {
		circle.Value = arr.Index(i).Interface()
		circle = circle.Next()
	}

	return circle
}

func Position(haystack interface {}, needle interface {}) int {
	rSrc := reflect.ValueOf(haystack).Elem()
	for position := 0; position < rSrc.Len(); position++ {
		if (reflect.DeepEqual(rSrc.Index(position).Interface(), needle)) {
			return position
		}
	}

	return -1
}

func Hash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32())
}
