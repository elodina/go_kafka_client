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

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"

	utils "github.com/elodina/go-mesos-utils"
)

type Cluster struct {
	frameworkID string
	storage     utils.Storage
	tasks       map[string]Task
	taskLock    sync.Mutex
}

func NewCluster() *Cluster {
	storage, err := NewStorage(Config.Storage)
	if err != nil {
		panic(err)
	}
	return &Cluster{
		storage: storage,
		tasks:   make(map[string]Task),
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

	fmt.Println(task.Data())
	c.tasks[task.Data().ID] = task
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

func (c *Cluster) GetConstrained() []utils.Constrained {
	c.taskLock.Lock()
	defer c.taskLock.Unlock()

	constrained := make([]utils.Constrained, 0)
	for _, task := range c.tasks {
		constrained = append(constrained, task.Data())
	}

	return constrained
}

func (c *Cluster) GetTasksWithState(state TaskState) []Task {
	c.taskLock.Lock()
	defer c.taskLock.Unlock()

	tasks := make([]Task, 0)
	for _, task := range c.tasks {
		if task.Data().State == state {
			tasks = append(tasks, task)
		}
	}

	return tasks
}

func (c *Cluster) IsReconciling() bool {
	return len(c.GetTasksWithState(TaskStateReconciling)) > 0
}

func (c *Cluster) ExpandIDs(expr string) ([]string, error) {
	if expr == "" {
		return nil, errors.New("ID expression cannot be empty")
	}

	ids := make([]string, 0)

	ranges := strings.Split(expr, ",")
	for _, rangeExpr := range ranges {
		if rangeExpr == "*" {
			tasks := c.GetAllTasks()
			for _, task := range tasks {
				ids = append(ids, task.Data().ID)
			}
			sort.Strings(ids)
			return ids, nil
		} else {
			rng, err := utils.ParseRange(rangeExpr)
			if err != nil {
				return nil, err
			}

			for _, value := range rng.Values() {
				ids = append(ids, strconv.Itoa(value))
			}
		}
	}

	sort.Strings(ids)
	return ids, nil
}

func (c *Cluster) Save() {
	jsonMap := make(map[string]interface{})
	jsonMap["frameworkID"] = c.frameworkID
	jsonMap["tasks"] = c.GetAllTasks()

	js, err := json.Marshal(jsonMap)
	if err != nil {
		panic(err)
	}

	c.storage.Save(js)
}

func (c *Cluster) Load() {
	js, err := c.storage.Load()
	if err != nil || js == nil {
		Logger.Warnf("Could not load cluster state from %s, assuming no cluster state available...", c.storage)
		return
	}

	//golang's dynamic type detection is really shitty
	jsonMap := make(map[string]json.RawMessage)
	err = json.Unmarshal(js, &jsonMap)
	if err != nil {
		panic(err)
	}
	err = json.Unmarshal(jsonMap["frameworkID"], &c.frameworkID)
	if err != nil {
		panic(err)
	}
	rawTasks := make([]map[string]json.RawMessage, 0)
	err = json.Unmarshal(jsonMap["tasks"], &rawTasks)
	if err != nil {
		panic(err)
	}

	for _, rawTask := range rawTasks {
		taskData := &TaskData{}
		err = json.Unmarshal(rawTask["data"], taskData)
		if err != nil {
			panic(err)
		}

		var taskType string
		json.Unmarshal(rawTask["type"], &taskType)
		switch taskType {
		case TaskTypeMirrorMaker:
			c.Add(&MirrorMakerTask{&CommonTask{TaskData: taskData}})
		default:
			panic(fmt.Errorf("Unknown task type %s", taskType))
		}
	}
}

func NewStorage(storage string) (utils.Storage, error) {
	storageTokens := strings.SplitN(storage, ":", 2)
	if len(storageTokens) != 2 {
		return nil, fmt.Errorf("Unsupported storage")
	}

	switch storageTokens[0] {
	case "file":
		return utils.NewFileStorage(storageTokens[1]), nil
	case "zk":
		return utils.NewZKStorage(storageTokens[1])
	default:
		return nil, fmt.Errorf("Unsupported storage")
	}
}
