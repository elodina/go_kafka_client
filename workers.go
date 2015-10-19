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
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// WorkerManager is responsible for splitting the incomming batches of messages between a configured amount of workers.
// It also keeps track of highest processed offsets and commits them to offset storage with a configurable frequency.
type WorkerManager struct {
	id                  string
	config              *ConsumerConfig
	workers             []*Worker
	availableWorkers    chan *Worker
	currentBatch        *taskBatch
	batchOrder          []TaskId
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
	closeConsumer       chan bool
	shutdownDecision    *FailedDecision

	metrics *ConsumerMetrics
}

// Creates a new WorkerManager with given id using a given ConsumerConfig and responsible for managing given TopicAndPartition.
func NewWorkerManager(id string, config *ConsumerConfig, topicPartition TopicAndPartition, metrics *ConsumerMetrics, closeConsumer chan bool) *WorkerManager {
	workers := make([]*Worker, config.NumWorkers)
	availableWorkers := make(chan *Worker, config.NumWorkers)
	for i := 0; i < config.NumWorkers; i++ {
		workers[i] = &Worker{
			InputChannel:         make(chan *TaskAndStrategy),
			OutputChannel:        make(chan WorkerResult),
			HandlerInputChannel:  make(chan *TaskAndStrategy),
			HandlerOutputChannel: make(chan WorkerResult),
			TaskTimeout:          config.WorkerTaskTimeout,
		}
		workers[i].Start()
		availableWorkers <- workers[i]
	}

	return &WorkerManager{
		id:                  id,
		config:              config,
		availableWorkers:    availableWorkers,
		workers:             workers,
		inputChannel:        make(chan []*Message),
		currentBatch:        newTaskBatch(),
		batchOrder:          make([]TaskId, 0),
		topicPartition:      topicPartition,
		largestOffset:       InvalidOffset,
		lastCommittedOffset: InvalidOffset,
		failCounter:         NewFailureCounter(config.WorkerRetryThreshold, config.WorkerThresholdTimeWindow),
		batchProcessed:      make(chan bool),
		managerStop:         make(chan bool),
		processingStop:      make(chan bool),
		commitStop:          make(chan bool),
		metrics:             metrics,
		closeConsumer:       closeConsumer,
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
		// force manager stop to be checked first
		select {
		case <-wm.managerStop:
			return
		default:
			select {
			case batch := <-wm.inputChannel:
				{
					wm.metrics.wMsIdle().Update(time.Since(startIdle))
					if Logger.IsAllowed(TraceLevel) {
						Trace(wm, "WorkerManager got batch")
					}
					wm.metrics.wMsBatchDuration().Time(func() {
						wm.startBatch(batch)
					})
					if Logger.IsAllowed(TraceLevel) {
						Trace(wm, "WorkerManager got batch processed")
					}
				}
			case <-wm.managerStop:
				return
			}
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
			wm.failCounter.Close()
			Debug(wm, "Stopped failure counter")
			finished <- true
			Debug(wm, "Leaving manager stop")

			Debug(wm, "Stopping workers")
			for _, worker := range wm.workers {
				worker.Stop()
			}
			Debug(wm, "Stopped all workers")
		})
		Debugf(wm, "Stopped workerManager")
	}()

	return finished
}

func (wm *WorkerManager) startBatch(batch []*Message) {
	inLock(&wm.stopLock, func() {
		wm.currentBatch = newTaskBatch()
		wm.batchOrder = make([]TaskId, 0)
		for _, message := range batch {
			topicPartition := TopicAndPartition{message.Topic, message.Partition}
			id := TaskId{topicPartition, message.Offset}
			wm.batchOrder = append(wm.batchOrder, id)
			wm.currentBatch.add(id, &Task{Msg: message})
		}
		wm.metrics.pendingWMsTasks().Inc(int64(wm.currentBatch.numOutstanding()))
		for _, id := range wm.batchOrder {
			task := wm.currentBatch.get(id)
			worker := <-wm.availableWorkers

			if wm.shutdownDecision == nil {
				wm.metrics.activeWorkers().Inc(1)
				wm.metrics.pendingWMsTasks().Dec(1)
				worker.InputChannel <- &TaskAndStrategy{task, wm.config.Strategy}
			} else {
				return
			}
		}

		<-wm.batchProcessed
	})
}

func (wm *WorkerManager) commitBatch() {
	for {
		timeout := time.NewTimer(wm.config.OffsetCommitInterval)
		select {
		case <-wm.commitStop:
			{
				timeout.Stop()
				wm.commitOffset()
				return
			}
		case <-timeout.C:
			{
				wm.commitOffset()
			}
		}
	}
}

func (wm *WorkerManager) commitOffset() {
	largestOffset := wm.GetLargestOffset()
	if Logger.IsAllowed(TraceLevel) {
		Tracef(wm, "Inside commit offset with largest %d and last %d", largestOffset, wm.lastCommittedOffset)
	}
	if largestOffset <= wm.lastCommittedOffset || isOffsetInvalid(largestOffset) {
		return
	}

	success := false
	for i := 0; i <= wm.config.OffsetsCommitMaxRetries; i++ {
		err := wm.config.OffsetStorage.CommitOffset(wm.config.Groupid, wm.topicPartition.Topic, wm.topicPartition.Partition, largestOffset)
		if err == nil {
			success = true
			if Logger.IsAllowed(TraceLevel) {
				Tracef(wm, "Successfully committed offset %d for %s", largestOffset, wm.topicPartition)
			}
			break
		} else {
			Debugf(wm, "Failed to commit offset %d for %s; error: %s. Retrying...", largestOffset, &wm.topicPartition, err)
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
	return wm.currentBatch.done()
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

				if wm.shutdownDecision != nil && *wm.shutdownDecision == DoNotCommitOffsetAndStop {
					wm.taskIsDone(result)
					continue
				}

				if result.Success() {
					wm.taskSucceeded(result)
				} else {
					task := wm.currentBatch.get(result.Id())
					if _, ok := result.(*TimedOutResult); ok {
						wm.metrics.taskTimeouts().Inc(1)
						task.Callee.OutputChannel = make(chan WorkerResult)
					}

					Debugf(wm, "Worker task %s has failed", result.Id())
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
								wm.taskSucceeded(result)
							}
						case DoNotCommitOffsetAndContinue:
							{
								wm.taskIsDone(result)
							}
						case CommitOffsetAndStop:
							{
								wm.taskSucceeded(result)
								wm.triggerShutdownIfRequired(&decision)
							}
						case DoNotCommitOffsetAndStop:
							{
								Debug(wm, "Setting task as done")
								wm.taskIsDone(result)
								Debug(wm, "Triggering shutdown")
								wm.triggerShutdownIfRequired(&decision)
							}
						}
					} else {
						Debugf(wm, "Retrying worker task %s %dth time", result.Id(), task.Retries)
						time.Sleep(wm.config.WorkerBackoff)
						go func() {
							task.Callee.InputChannel <- &TaskAndStrategy{task, wm.config.Strategy}
						}()
					}
				}

				if wm.IsBatchProcessed() {
					if Logger.IsAllowed(TraceLevel) {
						Trace(wm, "Sending batch processed")
					}
					wm.batchProcessed <- true
					if Logger.IsAllowed(TraceLevel) {
						Trace(wm, "Received batch processed")
					}
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

func (wm *WorkerManager) triggerShutdownIfRequired(decision *FailedDecision) {
	if wm.shutdownDecision == nil {
		wm.shutdownDecision = decision
		go func() {
			wm.closeConsumer <- true
		}()
	}
}

func (wm *WorkerManager) taskSucceeded(result WorkerResult) {
	if Logger.IsAllowed(TraceLevel) {
		Tracef(wm, "Task is done: %d", result.Id().Offset)
	}
	wm.UpdateLargestOffset(result.Id().Offset)
	wm.taskIsDone(result)
	wm.metrics.activeWorkers().Dec(1)
}

func (wm *WorkerManager) taskIsDone(result WorkerResult) {
	wm.availableWorkers <- wm.currentBatch.get(result.Id()).Callee
	wm.currentBatch.markDone(result.Id())
}

// Gets the highest offset that has been processed by this WorkerManager.
func (wm *WorkerManager) GetLargestOffset() int64 {
	return atomic.LoadInt64(&wm.largestOffset)
}

// Updates the highest offset that has been processed by this WorkerManager with a new value.
func (wm *WorkerManager) UpdateLargestOffset(offset int64) {
	atomic.StoreInt64(&wm.largestOffset, int64(math.Max(float64(wm.GetLargestOffset()), float64(offset))))
}

// Represents a worker that is able to process a single message.
type Worker struct {
	// Channel to write tasks to.
	InputChannel chan *TaskAndStrategy

	// Channel to write processing results to.
	OutputChannel chan WorkerResult

	// Intermediate channel for pushing result to strategy handler
	HandlerInputChannel chan *TaskAndStrategy

	// Intermediate channel for pushing result from strategy handler
	HandlerOutputChannel chan WorkerResult

	// Timeout for a single worker task.
	TaskTimeout time.Duration

	// Indicates whether this worker is closed and cannot accept new work.
	Closed bool
}

func (w *Worker) String() string {
	return "worker"
}

// Starts processing a given task using given strategy with this worker.
// Call to this method blocks until the task is done or timed out.
func (w *Worker) Start() {
	handlerInterrupted := false
	go func() {
		for taskAndStrategy := range w.HandlerInputChannel {
			result := taskAndStrategy.Strategy(w, taskAndStrategy.WorkerTask.Msg, taskAndStrategy.WorkerTask.Id())
		Loop:
			for !handlerInterrupted {
				timeout := time.NewTimer(5 * time.Second)
				select {
				case w.HandlerOutputChannel <- result:
					timeout.Stop()
					break Loop
				case <-timeout.C:
				}
			}
			handlerInterrupted = false
		}
	}()

	go func() {
		for taskAndStrategy := range w.InputChannel {
			taskAndStrategy.WorkerTask.Callee = w
			w.HandlerInputChannel <- taskAndStrategy
			timeout := time.NewTimer(w.TaskTimeout)
			select {
			case result := <-w.HandlerOutputChannel:
				{
					w.OutputChannel <- result
				}
			case <-timeout.C:
				{
					handlerInterrupted = true
					w.OutputChannel <- &TimedOutResult{taskAndStrategy.WorkerTask.Id()}
				}
			}
			timeout.Stop()
		}
	}()
}

func (w *Worker) Stop() {
	close(w.InputChannel)
	close(w.HandlerInputChannel)
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
	stop            chan bool
}

// Creates a new FailureCounter with threshold FailedThreshold and time window WorkerThresholdTimeWindow.
func NewFailureCounter(FailedThreshold int32, WorkerThresholdTimeWindow time.Duration) *FailureCounter {
	counter := &FailureCounter{
		failedThreshold: FailedThreshold,
		stop:            make(chan bool),
	}
	go func() {
		for {
			timeout := time.NewTimer(WorkerThresholdTimeWindow)
			select {
			case <-timeout.C:
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
			case <-counter.stop:
				timeout.Stop()
				return
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

// Stops this failure counter
func (f *FailureCounter) Close() {
	f.stop <- true
}

// Represents a single task for a worker.
type Task struct {
	// A message that should be processed.
	Msg *Message

	// Number of retries used for this task.
	Retries int

	// A worker that is responsible for processing this task.
	Callee *Worker
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
	Offset int64
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

// taskBatch represents a batch of tasks which must be processed by workers
type taskBatch struct {
	tasks  map[TaskId]*Task
	nTasks *int64
	nDone  *int64
}

func newTaskBatch() *taskBatch {
	var t, d int64 = 0, 0
	return &taskBatch{
		tasks:  make(map[TaskId]*Task),
		nTasks: &t,
		nDone:  &d,
	}
}

func (b *taskBatch) add(id TaskId, task *Task) {
	b.tasks[id] = task
	atomic.AddInt64(b.nTasks, 1)
}

func (b *taskBatch) get(id TaskId) *Task {
	return b.tasks[id]
}

func (b *taskBatch) markDone(id TaskId) {
	atomic.AddInt64(b.nDone, 1)
}

func (b *taskBatch) numOutstanding() int {
	return int(atomic.LoadInt64(b.nTasks) - atomic.LoadInt64(b.nDone))
}

func (b *taskBatch) done() bool {
	return b.numOutstanding() == 0
}

type TaskAndStrategy struct {
	WorkerTask *Task
	Strategy   WorkerStrategy
}
