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

package utils

import (
	mesos "github.com/mesos/mesos-go/mesosproto"
	"sync"
	"time"
)

type Tasks interface {
	Exists(id string) bool
	Add(task Task)
	Remove(id string)
	Get(id string) Task
	GetAll() []Task
	GetWithFilter(filter func(Task) bool) []Task
	IsReconciling() bool
}

func NewTasks() Tasks {
	return &tasks{
		tasks: make(map[string]Task),
	}
}

type tasks struct {
	tasks map[string]Task
}

func (t *tasks) Exists(id string) bool {
	_, exists := t.tasks[id]
	return exists
}

func (t *tasks) Add(task Task) {
	t.tasks[task.Data().ID] = task
	Logger.Info("Added task:\n%s", task)
}

func (t *tasks) Remove(id string) {

	delete(t.tasks, id)
	Logger.Info("Removed task %s", id)
}

func (t *tasks) Get(id string) Task {
	return t.tasks[id]
}

func (t *tasks) GetAll() []Task {
	return t.GetWithFilter(func(_ Task) bool {
		return true
	})
}

func (t *tasks) GetWithFilter(filter func(Task) bool) []Task {
	tasks := make([]Task, 0)
	for _, task := range t.tasks {
		if filter(task) {
			tasks = append(tasks, task)
		}
	}

	return tasks
}

func (t *tasks) IsReconciling() bool {
	return len(t.GetWithFilter(func(task Task) bool {
		return task.Data().State == TaskStateReconciling
	})) > 0
}

type threadSafeTasks struct {
	tasks    Tasks
	taskLock sync.Mutex
}

func NewThreadSafeTasks() Tasks {
	return &threadSafeTasks{
		tasks: NewTasks(),
	}
}

func (t *threadSafeTasks) Exists(id string) bool {
	t.taskLock.Lock()
	defer t.taskLock.Unlock()

	return t.tasks.Exists(id)
}

func (t *threadSafeTasks) Add(task Task) {
	t.taskLock.Lock()
	defer t.taskLock.Unlock()

	t.tasks.Add(task)
}

func (t *threadSafeTasks) Remove(id string) {
	t.taskLock.Lock()
	defer t.taskLock.Unlock()

	t.tasks.Remove(id)
}

func (t *threadSafeTasks) Get(id string) Task {
	t.taskLock.Lock()
	defer t.taskLock.Unlock()

	return t.tasks.Get(id)
}

func (t *threadSafeTasks) GetAll() []Task {
	t.taskLock.Lock()
	defer t.taskLock.Unlock()

	return t.tasks.GetAll()
}

func (t *threadSafeTasks) GetWithFilter(filter func(Task) bool) []Task {
	t.taskLock.Lock()
	defer t.taskLock.Unlock()

	return t.tasks.GetWithFilter(filter)
}

func (t *threadSafeTasks) IsReconciling() bool {
	return t.tasks.IsReconciling()
}

type TaskState string

const (
	TaskStateInactive    TaskState = "inactive"
	TaskStateStopped     TaskState = "stopped"
	TaskStateStaging     TaskState = "staging"
	TaskStateRunning     TaskState = "running"
	TaskStateReconciling TaskState = "reconciling"
)

type Task interface {
	Data() *TaskData
	Matches(*mesos.Offer) string
	NewTaskInfo(*mesos.Offer) *mesos.TaskInfo
}

// also implements Constrained
type TaskData struct {
	ID            string
	TaskID        string
	SlaveID       string
	ExecutorID    string
	Attributes    map[string]string
	State         TaskState
	Cpu           float64
	Mem           float64
	ConstraintMap Constraints
}

func (td *TaskData) Attribute(name string) string {
	return td.Attributes[name]
}

func (td *TaskData) Constraints() map[string][]Constraint {
	return td.ConstraintMap
}

func (td *TaskData) WaitFor(state TaskState, timeout time.Duration) bool {
	timeoutChan := time.After(timeout)
	ticker := time.Tick(100 * time.Millisecond)
	for {
		select {
		case <-timeoutChan:
			return false
		case <-ticker:
			{
				if td.State == state {
					return true
				}
			}
		}
	}
}

// Reset resets executor-specific information for this task, e.g. TaskID, ExecutorID, SlaveID and Attributes
func (td *TaskData) Reset() {
	td.TaskID = ""
	td.ExecutorID = ""
	td.SlaveID = ""
	td.Attributes = make(map[string]string)
}
