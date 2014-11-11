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
	"time"
	"reflect"
	"math"
	"fmt"
	"sync"
)

type WorkerManager struct {
	Config *ConsumerConfig
	Strategy         WorkerStrategy
	FailureCallback FailedCallback
	FailedAttemptCallback FailedCallback
	Workers          []*Worker
	AvailableWorkers chan *Worker
	CurrentBatch map[TaskId]*Task
	InputChannel     chan [] *Message
	OutputChannel    chan map[TopicAndPartition]int64
	LargestOffsets map[TopicAndPartition]int64
	FailCounter *FailureCounter
}

func (wm *WorkerManager) String() string {
	return fmt.Sprintf("%s-workerManager", wm.Config.ConsumerId)
}

func (wm *WorkerManager) Start() {
	for {
		batch := <-wm.InputChannel
		for _, message := range batch {
			wm.CurrentBatch[TaskId{ TopicAndPartition{ message.Topic, message.Partition }, message.Offset }] = &Task{
				Msg: message,
			}
		}

		for _, task := range wm.CurrentBatch {
			worker := <-wm.AvailableWorkers
			worker.Start(task, wm.Strategy)
		}

		wm.awaitResult()

		wm.OutputChannel <- wm.LargestOffsets
		wm.LargestOffsets = make(map[TopicAndPartition]int64)
	}
}

func (wm *WorkerManager) Stop() chan bool {
	finished := make(chan bool)
	go func() {
		for _, worker := range wm.Workers {
			worker.Close()
		}
		finished <- true
	}()

	return finished
}

func (wm *WorkerManager) IsBatchProcessed() bool {
	return len(wm.CurrentBatch) == 0
}

func (wm *WorkerManager) awaitResult() {
	outputChannels := make([]chan WorkerResult, wm.Config.NumWorkers)
	for i, worker := range wm.Workers {
		outputChannels[i] = worker.OutputChannel
	}

	Loop:
	for {
		cases := make([]reflect.SelectCase, len(outputChannels))
		for i, ch := range outputChannels {
			cases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ch)}
		}
		cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(time.After(wm.Config.WorkerBatchTimeout))})

		remaining := len(cases)
		for remaining > 0 {
			chosen, value, ok := reflect.Select(cases)
			if !ok {
				// The chosen channel has been closed, so zero out the channel to disable the case
				cases[chosen].Chan = reflect.ValueOf(nil)
				remaining -= 1
				continue
			}

			result, ok := value.Interface().(WorkerResult)
			if !ok {
				if _, ok := value.Interface().(time.Time); ok {
					Errorf(wm, "Worker batch has timed out")
					outputChannels = make([] chan WorkerResult, 0)
					for _, task := range wm.CurrentBatch {
						channel := make(chan WorkerResult)
						outputChannels = append(outputChannels, channel)
						task.Callee.OutputChannel = channel
						wm.AvailableWorkers <- task.Callee
					}
					cases = make([]reflect.SelectCase, len(outputChannels))
					for i, ch := range outputChannels {
						cases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ch)}
					}
					cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(time.After(wm.Config.WorkerBatchTimeout))})
					wm.CurrentBatch = make(map[TaskId]*Task)
					break Loop
				} else {
					panic("Incorrect result has been returned from worker")
				}
			}

			task := wm.CurrentBatch[result.Id]
			if result.Success {
				wm.taskIsDone(result)
			} else {
				Warnf(wm, "Worker task %s has failed", result.Id)
				task.Retries++
				if task.Retries >= wm.Config.MaxWorkerRetries {
					Errorf(wm, "Worker task %s has failed after %d retries", result.Id, wm.Config.MaxWorkerRetries)

					wm.taskIsDone(result)
					if wm.FailCounter.Failed() {
						wm.FailureCallback(wm)
					} else {
						wm.FailedAttemptCallback(wm)
					}
				} else {
					Warnf(wm, "Retrying worker task %s %dth time", result.Id, task.Retries)
					time.Sleep(wm.Config.WorkerBackoff)
					go task.Callee.Start(task, wm.Strategy)
				}
			}

			if wm.IsBatchProcessed() { break Loop }
		}
	}
}

func (wm *WorkerManager) taskIsDone(result WorkerResult) {
	key := result.Id.TopicPartition
	wm.LargestOffsets[key] = int64(math.Max(float64(wm.LargestOffsets[key]), float64(result.Id.Offset)))
	wm.AvailableWorkers <- wm.CurrentBatch[result.Id].Callee
	delete(wm.CurrentBatch, result.Id)
}

func NewWorkerManager(config *ConsumerConfig, batchOutputChannel chan map[TopicAndPartition]int64) *WorkerManager {
	workers := make([]*Worker, config.NumWorkers)
	availableWorkers := make(chan *Worker, config.NumWorkers)
	for i := 0; i < config.NumWorkers; i++ {
		workers[i] = &Worker{
			OutputChannel: make(chan WorkerResult),
			CloseTimeout: config.WorkerCloseTimeout,
		}
		availableWorkers <- workers[i]
	}

	return &WorkerManager {
		Config: config,
		Strategy: config.Strategy,
		FailureCallback: config.WorkerFailureCallback,
		FailedAttemptCallback: config.WorkerFailedAttemptCallback,
		AvailableWorkers: availableWorkers,
		InputChannel: make(chan [] *Message),
		OutputChannel: batchOutputChannel,
		CurrentBatch: make(map[TaskId]*Task),
		LargestOffsets: make(map[TopicAndPartition]int64),
		FailCounter: NewFailureCounter(config.WorkerRetryThreshold, config.WorkerConsideredFailedTimeWindow),
	}
}

type Worker struct {
	OutputChannel chan WorkerResult
	CloseTimeout time.Duration
	Closed bool
}

func (w *Worker) Start(task *Task, strategy WorkerStrategy) {
	task.Callee = w
	go strategy(w, task.Msg, w.OutputChannel)
}

func (w *Worker) Close() *WorkerResult {
	if !w.Closed {
		w.Closed = true
		select {
		case result := <- w.OutputChannel: {
			return &result
		}
		case <- time.After(w.CloseTimeout): {
			Warnf(w, "Worker failed to close within %f seconds", w.CloseTimeout.Seconds())
			return nil
		}
		}
	}

	return nil
}

type WorkerStrategy func(*Worker, *Message, chan WorkerResult)

type FailedCallback func(*WorkerManager)

type FailureCounter struct {
	count int32
	failed bool
	countLock sync.Mutex
	failedThreshold int32
}

func NewFailureCounter(FailedThreshold int32, WorkerConsideredFailedTimeWindow time.Duration) *FailureCounter {
	counter := &FailureCounter {
		failedThreshold : FailedThreshold,
	}
	go func() {
		for {
			select {
			case <- time.After(WorkerConsideredFailedTimeWindow): {
				if counter.count >= FailedThreshold {
					counter.failed = true
					return
				} else {
					InLock(&counter.countLock, func() {
						counter.count = 0
					})
				}
			}
			}
		}
	}()
	return counter
}

func (f *FailureCounter) Failed() bool {
	InLock(&f.countLock, func() { f.count++ })
	return f.count >= f.failedThreshold || f.failed
}

type Task struct {
	Msg *Message
	Retries int
	Callee *Worker
}

type WorkerResult struct {
	Id      TaskId
	Success bool
}

type TaskId struct {
	TopicPartition TopicAndPartition
	Offset         int64
}

func (tid *TaskId) String() string {
	return fmt.Sprintf("%s, Offset: %d", tid.TopicPartition, tid.Offset)
}
