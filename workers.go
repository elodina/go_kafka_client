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
	"fmt"
	metrics "github.com/rcrowley/go-metrics"
	"math"
	"sync"
	"time"
	"sync/atomic"
)

// WorkerManager is responsible for splitting the incomming batches of messages between a configured amount of workers.
// It also keeps track of highest processed offsets and commits them to offset storage with a configurable frequency.
type WorkerManager struct {
	id                  string
	config              *ConsumerConfig
	workers             []*Worker
	availableWorkers    chan *Worker
	currentBatch        map[TaskId]*Task //TODO inspect for race conditions
	inputChannel        chan []*Message
	topicPartition      TopicAndPartition
	largestOffset       int64
	lastCommittedOffset int64
	failCounter         *FailureCounter
	batchProcessed      chan bool
	stopLock            sync.Mutex
	managerStop         chan bool
	processingStop      chan bool
	commitStop          chan bool

	activeWorkersCounter metrics.Counter
	pendingTasksCounter  metrics.Counter
	batchDurationTimer   metrics.Timer
	idleTimer            metrics.Timer
}

// Creates a new WorkerManager with given id using a given ConsumerConfig and responsible for managing given TopicAndPartition.
func NewWorkerManager(id string, config *ConsumerConfig, topicPartition TopicAndPartition,
	wmsIdleTimer metrics.Timer, batchDurationTimer metrics.Timer, activeWorkersCounter metrics.Counter,
	pendingWMsTasksCounter metrics.Counter) *WorkerManager {
	workers := make([]*Worker, config.NumWorkers)
	availableWorkers := make(chan *Worker, config.NumWorkers)
	for i := 0; i < config.NumWorkers; i++ {
		workers[i] = &Worker{
			OutputChannel: make(chan WorkerResult),
			TaskTimeout:   config.WorkerTaskTimeout,
		}
		availableWorkers <- workers[i]
	}

	return &WorkerManager{
		id:                   id,
		config:               config,
		availableWorkers:     availableWorkers,
		workers:              workers,
		inputChannel:         make(chan []*Message),
		currentBatch:         make(map[TaskId]*Task),
		topicPartition:       topicPartition,
		largestOffset:        InvalidOffset,
		failCounter:          NewFailureCounter(config.WorkerRetryThreshold, config.WorkerThresholdTimeWindow),
		batchProcessed:       make(chan bool),
		managerStop:          make(chan bool),
		processingStop:       make(chan bool),
		commitStop:           make(chan bool),
		activeWorkersCounter: activeWorkersCounter,
		pendingTasksCounter:  pendingWMsTasksCounter,
		batchDurationTimer:   batchDurationTimer,
		idleTimer:            wmsIdleTimer,
	}
}

func (wm *WorkerManager) String() string {
	return wm.id
}

// Starts processing incoming batches with this WorkerManager. Processing is possible only in batch-at-once mode.
// It also launches an offset committer routine.
// Call to this method blocks.
func (wm *WorkerManager) Start() {
	go wm.processBatch()
	go wm.commitBatch()
	for {
		startIdle := time.Now()
		select {
		case batch := <-wm.inputChannel:
			{
				wm.idleTimer.Update(time.Since(startIdle))
				Debug(wm, "WorkerManager got batch")
				wm.batchDurationTimer.Time(func() {
					wm.startBatch(batch)
				})
				Debug(wm, "WorkerManager got batch processed")
			}
		case <-wm.managerStop:
			return
		}
	}
}

// Tells this WorkerManager to finish processing current batch, stop accepting new work and shut down.
// This method returns immediately and returns a channel which will get the value once the shut down is finished.
func (wm *WorkerManager) Stop() chan bool {
	finished := make(chan bool)
	go func() {
		Debugf(wm, "Trying to stop workerManager")
		inLock(&wm.stopLock, func() {
			Debug(wm, "Stopping manager")
			wm.managerStop <- true
			Debug(wm, "Stopping processor")
			wm.processingStop <- true
			Debug(wm, "Successful manager stop")
			Debug(wm, "Stopping committer")
			wm.commitStop <- true
			Debug(wm, "Successful committer stop")
			finished <- true
			Debug(wm, "Leaving manager stop")
		})
		Debugf(wm, "Stopped workerManager")
	}()

	return finished
}

func (wm *WorkerManager) startBatch(batch []*Message) {
	inLock(&wm.stopLock, func() {
		for _, message := range batch {
			topicPartition := TopicAndPartition{message.Topic, message.Partition}
			wm.currentBatch[TaskId{topicPartition, message.Offset}] = &Task{Msg: message}
		}
		wm.pendingTasksCounter.Inc(int64(len(wm.currentBatch)))
		for _, task := range wm.currentBatch {
			worker := <-wm.availableWorkers
			wm.activeWorkersCounter.Inc(1)
			wm.pendingTasksCounter.Dec(1)
			worker.Start(task, wm.config.Strategy)
		}

		<-wm.batchProcessed
	})
}

func (wm *WorkerManager) commitBatch() {
	for {
		select {
		case <-wm.commitStop:
			{
				wm.commitOffset()
				return
			}
		case <-time.After(wm.config.OffsetCommitInterval):
			{
				wm.commitOffset()
			}
		}
	}
}

func (wm *WorkerManager) commitOffset() {
	largestOffset := wm.GetLargestOffset()
	Tracef(wm, "Inside commit offset with largest %d and last %d", largestOffset, wm.lastCommittedOffset)
	if largestOffset <= wm.lastCommittedOffset || isOffsetInvalid(largestOffset) {
		return
	}

	success := false
	for i := 0; i <= wm.config.OffsetsCommitMaxRetries; i++ {
		err := wm.config.Coordinator.CommitOffset(wm.config.Groupid, &wm.topicPartition, largestOffset)
		if err == nil {
			success = true
			Debugf(wm, "Successfully committed offset %d for %s", largestOffset, wm.topicPartition)
			break
		} else {
			Infof(wm, "Failed to commit offset %d for %s. Retying...", largestOffset, &wm.topicPartition)
		}
	}

	if !success {
		Errorf(wm, "Failed to commit offset %d for %s after %d retries", largestOffset, &wm.topicPartition, wm.config.OffsetsCommitMaxRetries)
		//TODO: what to do next?
	} else {
		wm.lastCommittedOffset = largestOffset
	}
}

// Asks this WorkerManager whether the current batch is fully processed. Returns true if so, false otherwise.
func (wm *WorkerManager) IsBatchProcessed() bool {
	return len(wm.currentBatch) == 0
}

func (wm *WorkerManager) processBatch() {
	outputChannels := make([]*chan WorkerResult, wm.config.NumWorkers)
	for i, worker := range wm.workers {
		outputChannels[i] = &worker.OutputChannel
	}

	resultsChannel := make(chan WorkerResult)
	for {
		stopRedirecting := redirectChannelsTo(outputChannels, resultsChannel)
		select {
		case result := <-resultsChannel:
			{
				go func() {
					stopRedirecting <- true
				}()

				task := wm.currentBatch[result.Id()]
				if result.Success() {
					wm.taskIsDone(result)
				} else {
					if _, ok := result.(*TimedOutResult); ok {
						task.Callee.OutputChannel = make(chan WorkerResult)
					}

					Warnf(wm, "Worker task %s has failed", result.Id())
					task.Retries++
					if task.Retries > wm.config.MaxWorkerRetries {
						Errorf(wm, "Worker task %s has failed after %d retries", result.Id(), wm.config.MaxWorkerRetries)

						var decision FailedDecision
						if wm.failCounter.Failed() {
							decision = wm.config.WorkerFailureCallback(wm)
						} else {
							decision = wm.config.WorkerFailedAttemptCallback(task, result)
						}
						switch decision {
						case CommitOffsetAndContinue:
							{
								wm.taskIsDone(result)
							}
						case DoNotCommitOffsetAndContinue:
							{
								wm.availableWorkers <- wm.currentBatch[result.Id()].Callee
								delete(wm.currentBatch, result.Id())
							}
						case CommitOffsetAndStop:
							{
								wm.taskIsDone(result)
								wm.stopBatch()
							}
						case DoNotCommitOffsetAndStop:
							{
								wm.stopBatch()
							}
						}
					} else {
						Warnf(wm, "Retrying worker task %s %dth time", result.Id(), task.Retries)
						time.Sleep(wm.config.WorkerBackoff)
						go task.Callee.Start(task, wm.config.Strategy)
					}
				}

				if wm.IsBatchProcessed() {
					Debug(wm, "Sending batch processed")
					wm.batchProcessed <- true
					Debug(wm, "Received batch processed")
				}
			}
		case <-wm.processingStop:
			{
				go func() {
					stopRedirecting <- true
				}()
				return
			}
		}

	}
}

func (wm *WorkerManager) stopBatch() {
	wm.currentBatch = make(map[TaskId]*Task)
	for _, worker := range wm.workers {
		worker.OutputChannel = make(chan WorkerResult)
	}
}

func (wm *WorkerManager) taskIsDone(result WorkerResult) {
	Tracef(wm, "Task is done: %d", result.Id().Offset)
	wm.UpdateLargestOffset(result.Id().Offset)
	wm.availableWorkers <- wm.currentBatch[result.Id()].Callee
	wm.activeWorkersCounter.Dec(1)
	delete(wm.currentBatch, result.Id())
}

// Gets the highest offset that has been processed by this WorkerManager.
func (wm *WorkerManager) GetLargestOffset() int64 {
	return atomic.LoadInt64(&wm.largestOffset)
}

// Updates the highest offset that has been processed by this WorkerManager with a new value.
func (wm *WorkerManager) UpdateLargestOffset(offset int64) {
	atomic.StoreInt64(&wm.largestOffset, int64(math.Max(float64(wm.largestOffset), float64(offset))))
}

// Represents a worker that is able to process a single message.
type Worker struct {
	// Channel to write processing results to.
	OutputChannel chan WorkerResult

	// Timeout for a single worker task.
	TaskTimeout   time.Duration

	// Indicates whether this worker is closed and cannot accept new work.
	Closed        bool
}

func (w *Worker) String() string {
	return "worker"
}

// Starts processing a given task using given strategy with this worker.
// Call to this method blocks until the task is done or timed out.
func (w *Worker) Start(task *Task, strategy WorkerStrategy) {
	task.Callee = w
	go func() {
		resultChannel := make(chan WorkerResult)
		go func() { resultChannel <- strategy(w, task.Msg, task.Id()) }()
		select {
		case result := <-resultChannel:
			{
				w.OutputChannel <- result
			}
		case <-time.After(w.TaskTimeout):
			{
				w.OutputChannel <- &TimedOutResult{task.Id()}
			}
		}
	}()
}

// Defines what to do with a single Kafka message. Returns a WorkerResult to distinguish successful and unsuccessful processings.
type WorkerStrategy func(*Worker, *Message, TaskId) WorkerResult

// A callback that is triggered when a worker fails to process ConsumerConfig.WorkerRetryThreshold messages within ConsumerConfig.WorkerThresholdTimeWindow
type FailedCallback func(*WorkerManager) FailedDecision

// A callback that is triggered when a worker fails to process a single message.
type FailedAttemptCallback func(*Task, WorkerResult) FailedDecision

// A counter used to track whether we reached the configurable threshold of failed messages within a given time window.
type FailureCounter struct {
	count           int32
	failed          bool
	countLock       sync.Mutex
	failedThreshold int32
}

// Creates a new FailureCounter with threshold FailedThreshold and time window WorkerThresholdTimeWindow.
func NewFailureCounter(FailedThreshold int32, WorkerThresholdTimeWindow time.Duration) *FailureCounter {
	counter := &FailureCounter{
		failedThreshold: FailedThreshold,
	}
	go func() {
		for {
			select {
			case <-time.After(WorkerThresholdTimeWindow):
				{
					if counter.count >= FailedThreshold {
						counter.failed = true
						return
					} else {
						inLock(&counter.countLock, func() {
							counter.count = 0
						})
					}
				}
			}
		}
	}()
	return counter
}

// Tells this FailureCounter to increment a number of failures by one.
// Returns true if threshold is reached, false otherwise.
func (f *FailureCounter) Failed() bool {
	inLock(&f.countLock, func() { f.count++ })
	return f.count >= f.failedThreshold || f.failed
}

// Represents a single task for a worker.
type Task struct {
	// A message that should be processed.
	Msg     *Message

	// Number of retries used for this task.
	Retries int

	// A worker that is responsible for processing this task.
	Callee  *Worker
}

// Returns an id for this Task.
func (t *Task) Id() TaskId {
	return TaskId{TopicAndPartition{t.Msg.Topic, t.Msg.Partition}, t.Msg.Offset}
}

// Interface that represents a result of processing incoming message.
type WorkerResult interface {
	// Returns an id of task that was processed.
	Id() TaskId

	// Returns true if processing succeeded, false otherwise.
	Success() bool
}

// An implementation of WorkerResult interface representing a successfully processed incoming message.
type SuccessfulResult struct {
	id TaskId
}

// Creates a new SuccessfulResult for given TaskId.
func NewSuccessfulResult(id TaskId) *SuccessfulResult {
	return &SuccessfulResult{id}
}

func (sr *SuccessfulResult) String() string {
	return fmt.Sprintf("{Success: %s}", sr.Id())
}

// Returns an id of task that was processed.
func (wr *SuccessfulResult) Id() TaskId {
	return wr.id
}

// Always returns true for SuccessfulResult.
func (wr *SuccessfulResult) Success() bool {
	return true
}

// An implementation of WorkerResult interface representing a failure to process incoming message.
type ProcessingFailedResult struct {
	id TaskId
}

// Creates a new ProcessingFailedResult for given TaskId.
func NewProcessingFailedResult(id TaskId) *ProcessingFailedResult {
	return &ProcessingFailedResult{id}
}

func (sr *ProcessingFailedResult) String() string {
	return fmt.Sprintf("{Failed: %s}", sr.Id())
}

// Returns an id of task that was processed.
func (wr *ProcessingFailedResult) Id() TaskId {
	return wr.id
}

// Always returns false for ProcessingFailedResult.
func (wr *ProcessingFailedResult) Success() bool {
	return false
}

// An implementation of WorkerResult interface representing a timeout to process incoming message.
type TimedOutResult struct {
	id TaskId
}

func (sr *TimedOutResult) String() string {
	return fmt.Sprintf("{Timed out: %s}", sr.Id())
}

// Returns an id of task that was processed.
func (wr *TimedOutResult) Id() TaskId {
	return wr.id
}

// Always returns false for TimedOutResult.
func (wr *TimedOutResult) Success() bool {
	return false
}

// Type representing a task id. Consists from topic, partition and offset of a message being processed.
type TaskId struct {
	// Message's topic and partition
	TopicPartition TopicAndPartition

	// Message's offset
	Offset         int64
}

func (tid TaskId) String() string {
	return fmt.Sprintf("%s, Offset: %d", &tid.TopicPartition, tid.Offset)
}

// Defines what to do when worker fails to process a message.
type FailedDecision int32

const (
	// Tells the worker manager to ignore the failure and continue normally.
	CommitOffsetAndContinue FailedDecision = iota

	// Tells the worker manager to continue processing new messages but not to commit offset that failed.
	DoNotCommitOffsetAndContinue

	// Tells the worker manager to commit offset and stop processing the current batch.
	CommitOffsetAndStop

	// Tells the worker manager not to commit offset and stop processing the current batch.
	DoNotCommitOffsetAndStop
)
