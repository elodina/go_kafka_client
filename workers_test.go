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
	"testing"
	"time"
)

func TestFailureCounter(t *testing.T) {
	threshold := int32(5)
	failTimeWindow := 2 * time.Second

	counter := NewFailureCounter(threshold, failTimeWindow)

	failed := false
	for i := 0; i < int(threshold); i++ {
		failed = failed || counter.Failed()
	}

	if !failed {
		t.Error("Failure counter should fail when threshold is reached")
	}

	counter = NewFailureCounter(threshold, failTimeWindow)
	failed = false
	for i := 0; i < int(threshold) - 1; i++ {
		failed = failed || counter.Failed()
	}
	time.Sleep(failTimeWindow + (100 * time.Millisecond))
	failed = failed || counter.Failed()

	if failed {
		t.Error("Failure counter should not fail when threshold is not reached within a given time window")
	}
}

func TestWorker(t *testing.T) {
	outChannel := make(chan WorkerResult)
	taskTimeout := 1 * time.Second

	task := &Task{
		Msg: &Message{},
	}

	goodStrategy := func (_ *Worker, _ *Message, id TaskId) WorkerResult { return NewSuccessfulResult(id) }
	failStrategy := func (_ *Worker, _ *Message, id TaskId) WorkerResult { return NewProcessingFailedResult(id) }
	slowStrategy := func (_ *Worker, _ *Message, id TaskId) WorkerResult { time.Sleep(5 * time.Second); return NewSuccessfulResult(id) }

	//test good case
	worker := &Worker{
		OutputChannel: outChannel,
		TaskTimeout: taskTimeout,
	}
	worker.Start(task, goodStrategy)

	result := <-outChannel
	if !result.Success() {
		t.Error("Worker result with good strategy should be successful")
	}

	//test fail case
	worker2 := &Worker{
		OutputChannel: outChannel,
		TaskTimeout: taskTimeout,
	}
	worker2.Start(task, failStrategy)
	result = <-outChannel
	if result.Success() {
		t.Error("Worker result with fail strategy should be unsuccessful")
	}

	//test timeout
	worker3 := &Worker{
		OutputChannel: outChannel,
		TaskTimeout: taskTimeout,
	}
	worker3.Start(task, slowStrategy)
	result = <-outChannel
	if _, ok := result.(*TimedOutResult); !ok {
		t.Error("Worker with slow strategy should time out")
	}

	select {
		case <-outChannel: {
			t.Error("Worker should not produce any result after timeout")
		}
		case <-time.After(taskTimeout + time.Second):
	}
}
