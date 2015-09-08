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

package framework

import "sync"

type Cluster struct {
	tasks    map[string]Task
	taskLock sync.Mutex
}

func NewCluster() *Cluster {
	return &Cluster{
		tasks: make(map[string]Task),
	}
}

func (c *Cluster) Exists(id string) bool {
	c.taskLock.Lock()
	defer c.taskLock.Unlock()

	_, exists := c.tasks[id]
	return exists
}

func (c *Cluster) Add(task Task) {
	c.taskLock.Lock()
	defer c.taskLock.Unlock()

	c.tasks[task.GetID()] = task
	Logger.Infof("Added task:\n%s", task)
}

func (c *Cluster) Remove(id string) {
	c.taskLock.Lock()
	defer c.taskLock.Unlock()

	delete(c.tasks, id)
	Logger.Infof("Removed task %s", id)
}

func (c *Cluster) Get(id string) Task {
	return c.tasks[id]
}

func (c *Cluster) GetAllTasks() []Task {
	c.taskLock.Lock()
	defer c.taskLock.Unlock()

	tasks := make([]Task, 0)
	for _, task := range c.tasks {
		tasks = append(tasks, task)
	}

	return tasks
}

func (c *Cluster) GetTasksWithState(state TaskState) []Task {
	c.taskLock.Lock()
	defer c.taskLock.Unlock()

	tasks := make([]Task, 0)
	for _, task := range c.tasks {
		if task.GetState() == state {
			tasks = append(tasks, task)
		}
	}

	return tasks
}
