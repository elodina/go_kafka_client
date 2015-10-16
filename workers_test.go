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
	"testing"
	"time"
)

var goodStrategy = func(_ *Worker, _ *Message, id TaskId) WorkerResult { return NewSuccessfulResult(id) }
var failStrategy = func(_ *Worker, _ *Message, id TaskId) WorkerResult { return NewProcessingFailedResult(id) }
var slowStrategy = func(_ *Worker, _ *Message, id TaskId) WorkerResult {
	time.Sleep(5 * time.Second)
	return NewSuccessfulResult(id)
}

func sleepStrategy(sleepfor time.Duration) func(_ *Worker, _ *Message, id TaskId) WorkerResult {
	return func(_ *Worker, _ *Message, id TaskId) WorkerResult {
		time.Sleep(sleepfor)
		return NewSuccessfulResult(id)
	}
}

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
	for i := 0; i < int(threshold)-1; i++ {
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

	//test good case
	worker := &Worker{
		InputChannel:         make(chan *TaskAndStrategy),
		OutputChannel:        outChannel,
		HandlerInputChannel:  make(chan *TaskAndStrategy),
		HandlerOutputChannel: make(chan WorkerResult),
		TaskTimeout:          taskTimeout,
	}
	worker.Start()
	worker.InputChannel <- &TaskAndStrategy{task, goodStrategy}

	result := <-outChannel
	if !result.Success() {
		t.Error("Worker result with good strategy should be successful")
	}

	//test fail case
	worker2 := &Worker{
		InputChannel:         make(chan *TaskAndStrategy),
		OutputChannel:        outChannel,
		HandlerInputChannel:  make(chan *TaskAndStrategy),
		HandlerOutputChannel: make(chan WorkerResult),
		TaskTimeout:          taskTimeout,
	}
	worker2.Start()
	worker2.InputChannel <- &TaskAndStrategy{task, failStrategy}
	result = <-outChannel
	if result.Success() {
		t.Error("Worker result with fail strategy should be unsuccessful")
	}

	//test timeout
	worker3 := &Worker{
		InputChannel:         make(chan *TaskAndStrategy),
		OutputChannel:        outChannel,
		HandlerInputChannel:  make(chan *TaskAndStrategy),
		HandlerOutputChannel: make(chan WorkerResult),
		TaskTimeout:          taskTimeout,
	}
	worker3.Start()
	worker3.InputChannel <- &TaskAndStrategy{task, slowStrategy}
	result = <-outChannel
	if _, ok := result.(*TimedOutResult); !ok {
		t.Error("Worker with slow strategy should time out")
	}

	select {
	case <-outChannel:
		{
			t.Error("Worker should not produce any result after timeout")
		}
	case <-time.After(taskTimeout + time.Second):
	}
}

func TestWorkerManager(t *testing.T) {
	wmid := "test-WM"
	config := DefaultConsumerConfig()
	config.NumWorkers = 3
	config.Strategy = goodStrategy
	mockZk := newMockZookeeperCoordinator()
	config.Coordinator = mockZk
	config.OffsetStorage = mockZk
	topicPartition := TopicAndPartition{"fakeTopic", int32(0)}

	metrics := newConsumerMetrics(wmid, "")
	closeConsumer := make(chan bool)
	manager := NewWorkerManager(wmid, config, topicPartition, metrics, closeConsumer)

	go manager.Start()

	if len(manager.workers) != config.NumWorkers {
		t.Errorf("Number of workers of worker manager should be %d, actual: %d", config.NumWorkers, len(manager.workers))
	}

	checkAllWorkersAvailable(t, manager)

	batch := []*Message{
		&Message{Offset: 0},
		&Message{Offset: 1},
		&Message{Offset: 2},
		&Message{Offset: 3},
		&Message{Offset: 4},
		&Message{Offset: 5},
	}

	manager.inputChannel <- batch

	time.Sleep(1 * time.Second)
	checkAllWorkersAvailable(t, manager)

	<-manager.Stop()

	//make sure we don't lose our offsets
	if len(mockZk.commitHistory) != 1 {
		t.Errorf("Worker manager should commit offset only once")
	}
	if mockZk.commitHistory[topicPartition] != 5 {
		t.Errorf("Worker manager should commit offset 5")
	}
}

func checkAllWorkersAvailable(t *testing.T, wm *WorkerManager) {
	Trace("test", "Checking all workers availability")
	//if all workers are available we shouldn't be able to insert one more available worker
	select {
	case wm.availableWorkers <- &Worker{}:
		t.Error("Not all workers are available")
	case <-time.After(1 * time.Second):
	}
}

func benchmarkWorkerManager(b *testing.B, numWorkers int, msgsPerBatch int, sleepFor time.Duration) {
	wmid := "test-WM"
	config := DefaultConsumerConfig()
	config.NumWorkers = numWorkers
	config.Strategy = sleepStrategy(sleepFor)
	mockZk := newMockZookeeperCoordinator()
	config.Coordinator = mockZk
	config.OffsetStorage = mockZk
	topicPartition := TopicAndPartition{"fakeTopic", int32(0)}

	metrics := newConsumerMetrics(wmid, "")
	closeConsumer := make(chan bool)
	manager := NewWorkerManager(wmid, config, topicPartition, metrics, closeConsumer)

	go manager.Start()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch := make([]*Message, msgsPerBatch)
		for j := range batch {
			batch[j] = &Message{Offset: int64(i*msgsPerBatch + j)}
		}
		manager.inputChannel <- batch
	}
	<-manager.Stop()

}

func BenchmarkWorkerManager_1worker_1msg_10us(b *testing.B) {
	benchmarkWorkerManager(b, 1, 1, 10*time.Microsecond)
}

func BenchmarkWorkerManager_1worker_1msg_100us(b *testing.B) {
	benchmarkWorkerManager(b, 1, 1, 100*time.Microsecond)
}

func BenchmarkWorkerManager_1worker_1msg_1000us(b *testing.B) {
	benchmarkWorkerManager(b, 1, 1, 1000*time.Microsecond)
}

func BenchmarkWorkerManager_1worker_10msg_10us(b *testing.B) {
	benchmarkWorkerManager(b, 1, 10, 10*time.Microsecond)
}

func BenchmarkWorkerManager_1worker_10msg_100us(b *testing.B) {
	benchmarkWorkerManager(b, 1, 10, 100*time.Microsecond)
}

func BenchmarkWorkerManager_1worker_10msg_1000us(b *testing.B) {
	benchmarkWorkerManager(b, 1, 10, 1000*time.Microsecond)
}

func BenchmarkWorkerManager_1worker_100msg_10us(b *testing.B) {
	benchmarkWorkerManager(b, 1, 10, 10*time.Microsecond)
}

func BenchmarkWorkerManager_1worker_100msg_100us(b *testing.B) {
	benchmarkWorkerManager(b, 1, 10, 100*time.Microsecond)
}

func BenchmarkWorkerManager_1worker_100msg_1000us(b *testing.B) {
	benchmarkWorkerManager(b, 1, 10, 1000*time.Microsecond)
}

func BenchmarkWorkerManager_5worker_1msg_10us(b *testing.B) {
	benchmarkWorkerManager(b, 5, 1, 10*time.Microsecond)
}

func BenchmarkWorkerManager_5worker_1msg_100us(b *testing.B) {
	benchmarkWorkerManager(b, 5, 1, 100*time.Microsecond)
}

func BenchmarkWorkerManager_5worker_1msg_1000us(b *testing.B) {
	benchmarkWorkerManager(b, 5, 1, 1000*time.Microsecond)
}

func BenchmarkWorkerManager_5worker_10msg_10us(b *testing.B) {
	benchmarkWorkerManager(b, 5, 10, 10*time.Microsecond)
}

func BenchmarkWorkerManager_5worker_10msg_100us(b *testing.B) {
	benchmarkWorkerManager(b, 5, 10, 100*time.Microsecond)
}

func BenchmarkWorkerManager_5worker_10msg_1000us(b *testing.B) {
	benchmarkWorkerManager(b, 5, 10, 1000*time.Microsecond)
}

func BenchmarkWorkerManager_5worker_100msg_10us(b *testing.B) {
	benchmarkWorkerManager(b, 5, 10, 10*time.Microsecond)
}

func BenchmarkWorkerManager_5worker_100msg_100us(b *testing.B) {
	benchmarkWorkerManager(b, 5, 10, 100*time.Microsecond)
}

func BenchmarkWorkerManager_5worker_100msg_1000us(b *testing.B) {
	benchmarkWorkerManager(b, 5, 10, 1000*time.Microsecond)
}

func BenchmarkWorkerManager_25worker_1msg_10us(b *testing.B) {
	benchmarkWorkerManager(b, 25, 1, 10*time.Microsecond)
}

func BenchmarkWorkerManager_25worker_1msg_100us(b *testing.B) {
	benchmarkWorkerManager(b, 25, 1, 100*time.Microsecond)
}

func BenchmarkWorkerManager_25worker_1msg_1000us(b *testing.B) {
	benchmarkWorkerManager(b, 25, 1, 1000*time.Microsecond)
}

func BenchmarkWorkerManager_25worker_10msg_10us(b *testing.B) {
	benchmarkWorkerManager(b, 25, 10, 10*time.Microsecond)
}

func BenchmarkWorkerManager_25worker_10msg_100us(b *testing.B) {
	benchmarkWorkerManager(b, 25, 10, 100*time.Microsecond)
}

func BenchmarkWorkerManager_25worker_10msg_1000us(b *testing.B) {
	benchmarkWorkerManager(b, 25, 10, 1000*time.Microsecond)
}

func BenchmarkWorkerManager_25worker_100msg_10us(b *testing.B) {
	benchmarkWorkerManager(b, 25, 10, 10*time.Microsecond)
}

func BenchmarkWorkerManager_25worker_100msg_100us(b *testing.B) {
	benchmarkWorkerManager(b, 25, 10, 100*time.Microsecond)
}

func BenchmarkWorkerManager_25worker_100msg_1000us(b *testing.B) {
	benchmarkWorkerManager(b, 25, 10, 1000*time.Microsecond)
}
