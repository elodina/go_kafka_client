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
)

type WorkerManager struct {
	Config *ConsumerConfig
	Strategy WorkerStrategy
	Workers []*Worker
	AvailableWorkers chan *Worker
	CurrentBatch map[TaskId]*Task
	InputChannel chan[] *Message
	OutputChannel chan map[TopicAndPartition]int64
	LargestOffsets map[TopicAndPartition]int64
	CurrentBatchCreated time.Time
	IsTimedOut bool
}

func (wm *WorkerManager) String() string {
	return fmt.Sprintf("%s-workerManager", wm.Config.ConsumerId)
}

func (wm *WorkerManager) Start() {
	go wm.startResultDispatcher()
	for {
		batch := <-wm.InputChannel
		wm.IsTimedOut = false
		for _, message := range batch {
			wm.CurrentBatch[TaskId{ TopicAndPartition{ message.Topic, int(message.Partition) }, message.Offset }] = &Task{
				Msg: message,
				Retries: 0,
			}
		}

		for _, task := range wm.CurrentBatch {
			if wm.IsTimedOut { break }

			worker := <-wm.AvailableWorkers
			go worker.Start(task)
		}

		wm.OutputChannel <- wm.LargestOffsets
	}
}

func(wm *WorkerManager) IsBatchProcessed() bool {
	return len(wm.CurrentBatch) == 0
}

func (wm *WorkerManager) startResultDispatcher() {
	outputChannels := make([]chan WorkerResult, wm.Config.NumWorkers)
	for i, worker := range wm.Workers {
		outputChannels[i] = worker.OutputChannel
	}

	for {
		cases := make([]reflect.SelectCase, len(outputChannels))
		for i, ch := range outputChannels {
			cases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ch)}
		}
		cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(time.After(wm.Config.BatchTimeout))})

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
					cases := make([]reflect.SelectCase, len(outputChannels))
					for i, ch := range outputChannels {
						cases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ch)}
					}
					cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(time.After(wm.Config.BatchTimeout))})
					wm.IsTimedOut = true
					continue
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
				} else {
					Warnf(wm, "Retrying worker task %s %dth time", result.Id, task.Retries)
					time.Sleep(wm.Config.WorkerBackoff)
					go task.Callee.Start(task)
				}
			}
		}
	}
}

func (wm *WorkerManager) taskIsDone(result WorkerResult) {
	key := result.Id.TopicPartition
	wm.LargestOffsets[key] = int64(math.Max(float64(wm.LargestOffsets[key]), float64(result.Id.Offset)))
	wm.AvailableWorkers <- wm.CurrentBatch[result.Id].Callee
}

func NewWorkerManager(config *ConsumerConfig, batchOutputChannel chan map[TopicAndPartition]int64) *WorkerManager {
	workers := make([]*Worker, config.NumWorkers)
	availableWorkers := make(chan *Worker, config.NumWorkers)
	for i := 0; i < config.NumWorkers; i++ {
		workers[i] = &Worker{
			InputChannel: make(chan *Message),
			OutputChannel: make(chan WorkerResult),
		}
		availableWorkers <- workers[i]
	}

	return &WorkerManager {
		Config: config,
		Strategy: config.Strategy,
		Workers: workers,
		AvailableWorkers: availableWorkers,
		InputChannel: make(chan[] *Message),
		OutputChannel: batchOutputChannel,
		CurrentBatch: make(map[TaskId]*Task),
		LargestOffsets: make(map[TopicAndPartition]int64),
	}
}

type Worker struct {
	InputChannel chan *Message
	OutputChannel chan WorkerResult
}

func (w *Worker) Start(task *Task) {
	task.Callee = w
	w.InputChannel <- task.Msg
}

type WorkerStrategy func(*Message, chan WorkerResult)

type Task struct {
	Msg *Message
	Retries int
	Callee *Worker
}

type WorkerResult struct {
	Id TaskId
	Success bool
}

type TaskId struct {
	TopicPartition TopicAndPartition
	Offset int64
}

func (tid *TaskId) String() string {
	return fmt.Sprintf("%s, Offset: %d", tid.TopicPartition, tid.Offset)
}
