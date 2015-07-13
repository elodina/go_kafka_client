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

import (
	"container/ring"
	crand "crypto/rand"
	"fmt"
	"github.com/jimlawless/cfg"
	"hash/fnv"
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Loads a property file located at Path. Returns a map[string]string or error.
func LoadConfiguration(Path string) (map[string]string, error) {
	cfgMap := make(map[string]string)
	err := cfg.Load(Path, cfgMap)

	return cfgMap, err
}

func inLock(lock *sync.Mutex, fun func()) {
	lock.Lock()
	defer lock.Unlock()

	fun()
}

func inReadLock(lock *sync.RWMutex, fun func()) {
	lock.RLock()
	defer lock.RUnlock()

	fun()
}

func inWriteLock(lock *sync.RWMutex, fun func()) {
	lock.Lock()
	defer lock.Unlock()

	fun()
}

func shuffleArray(src interface{}, dest interface{}) {
	rSrc := reflect.ValueOf(src).Elem()
	rDest := reflect.ValueOf(dest).Elem()

	perm := rand.Perm(rSrc.Len())
	for i, v := range perm {
		rDest.Index(v).Set(rSrc.Index(i))
	}
}

func circularIterator(src interface{}) *ring.Ring {
	arr := reflect.ValueOf(src).Elem()
	circle := ring.New(arr.Len())
	for i := 0; i < arr.Len(); i++ {
		circle.Value = arr.Index(i).Interface()
		circle = circle.Next()
	}

	return circle
}

func position(haystack interface{}, needle interface{}) int {
	rSrc := reflect.ValueOf(haystack).Elem()
	for position := 0; position < rSrc.Len(); position++ {
		if reflect.DeepEqual(rSrc.Index(position).Interface(), needle) {
			return position
		}
	}

	return -1
}

func hash(s string) int32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int32(h.Sum32())
}

func notifyWhenThresholdIsReached(inputChannels interface{}, outputChannel interface{}, threshold int) chan bool {
	input := reflect.ValueOf(inputChannels)
	output := reflect.ValueOf(outputChannel)
	killChannel := make(chan bool)

	if input.Kind() != reflect.Array && input.Kind() != reflect.Slice {
		panic("Incorrect input channels type")
	}

	if output.Kind() != reflect.Chan {
		panic("Incorrect output channel type")
	}

	cases := make([]reflect.SelectCase, input.Len())
	for i := 0; i < input.Len(); i++ {
		if input.Index(i).Kind() == reflect.Ptr {
			cases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(input.Index(i).Elem().Interface())}
		} else {
			cases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(input.Index(i).Interface())}
		}
	}

	count := threshold
	go func() {
		for {
			remaining := len(cases)
			for remaining > 0 {
				chosen, _, ok := reflect.Select(cases)
				if !ok {
					// The chosen channel has been closed, so zero out the channel to disable the case
					cases[chosen].Chan = reflect.ValueOf(nil)
					remaining -= 1
					continue
				}

				if cases[chosen].Chan.Interface() == killChannel {
					return
				} else {
					count--
				}

				if count == 0 {
					output.Send(reflect.ValueOf(true))
					return
				}
			}
		}
	}()

	return killChannel
}

func redirectChannelsTo(inputChannels interface{}, outputChannel interface{}) chan bool {
	killChannel, _ := redirectChannelsToWithTimeout(inputChannels, outputChannel, 0*time.Second)
	return killChannel
}

func pipe(from interface{}, to interface{}) chan bool {
	return redirectChannelsTo([]interface{}{from}, to)
}

func redirectChannelsToWithTimeout(inputChannels interface{}, outputChannel interface{}, timeout time.Duration) (chan bool, <-chan time.Time) {
	input := reflect.ValueOf(inputChannels)
	var timeoutInputChannel <-chan time.Time
	if timeout.Seconds() == 0 {
		timeoutInputChannel = nil
	} else {
		timeoutInputChannel = time.After(timeout)
	}
	output := reflect.ValueOf(outputChannel)
	timeoutOutputChannel := make(chan time.Time)
	killChannel := make(chan bool)

	if input.Kind() != reflect.Array && input.Kind() != reflect.Slice {
		panic("Incorrect input channels type")
	}

	if output.Kind() != reflect.Chan {
		panic("Incorrect output channel type")
	}

	cases := make([]reflect.SelectCase, input.Len())
	for i := 0; i < input.Len(); i++ {
		if input.Index(i).Kind() == reflect.Ptr {
			cases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(input.Index(i).Elem().Interface())}
		} else {
			cases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(input.Index(i).Interface())}
		}
	}
	cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(killChannel)})
	if timeoutInputChannel != nil {
		cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(timeoutInputChannel)})
	}

	go func() {
		for {
			remaining := len(cases)
			for remaining > 0 {
				chosen, value, ok := reflect.Select(cases)
				if !ok {
					// The chosen channel has been closed, so zero out the channel to disable the case
					cases[chosen].Chan = reflect.ValueOf(nil)
					remaining -= 1
					continue
				}

				if cases[chosen].Chan.Interface() == killChannel {
					return
				}

				if cases[chosen].Chan.Interface() == timeoutInputChannel {
					timeoutOutputChannel <- value.Interface().(time.Time)
				}

				output.Send(value)
			}
		}
	}()

	return killChannel, timeoutOutputChannel
}

func uuid() string {
	b := make([]byte, 16)
	crand.Read(b)
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

type barrier struct {
	size               int32
	watchers           int32
	barrierReachedLock sync.Mutex
	barrierReachedCond *sync.Cond
	callback           func()
}

func newBarrier(size int32, callback func()) *barrier {
	if size == 0 {
		panic("Cannot create barrier of size 0")
	}

	barrier := &barrier{}
	barrier.size = size
	barrier.watchers = 0
	barrier.barrierReachedCond = sync.NewCond(&barrier.barrierReachedLock)
	barrier.callback = callback

	return barrier
}

func (b *barrier) await() {
	inLock(&b.barrierReachedLock, func() {
		if b.watchers == b.size {
			panic("Barrier has been broken")
		}

		b.watchers++
		if b.size != b.watchers {
			for b.size != b.watchers {
				b.barrierReachedCond.Wait()
			}
			return
		}

		b.callback()
		b.barrierReachedCond.Broadcast()
	})
}

func (b *barrier) reset(size int32) {
	inLock(&b.barrierReachedLock, func() {
		if b.watchers > 0 && b.watchers != b.size {
			panic("Barrier is not broken yet")
		}
		b.size = size
		b.watchers = 0
	})
}

type hashArray []*TopicAndPartition

func (s hashArray) Len() int      { return len(s) }
func (s hashArray) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s hashArray) Less(i, j int) bool {
	return hash(s[i].String()) < hash(s[j].String())
}

func setStringConfig(where *string, what string) {
	if what != "" {
		*where = what
	}
}

func setStringSliceConfig(where *[]string, what string, delimiter string) {
	if what != "" {
		splitted := strings.Split(what, delimiter)
		if len(splitted) > 0 {
			*where = splitted
		}
	}
}

func setBoolConfig(where *bool, what string) {
	if what != "" {
		*where = what == "true"
	}
}

func setDurationConfig(where *time.Duration, what string) error {
	if what != "" {
		value, err := time.ParseDuration(what)
		if err == nil {
			*where = value
		}
		return err
	}
	return nil
}

func setIntConfig(where *int, what string) error {
	if what != "" {
		value, err := strconv.Atoi(what)
		if err == nil {
			*where = value
		}
		return err
	}
	return nil
}

func setInt32Config(where *int32, what string) error {
	if what != "" {
		value, err := strconv.Atoi(what)
		if err == nil {
			*where = int32(value)
		}
		return err
	}
	return nil
}
