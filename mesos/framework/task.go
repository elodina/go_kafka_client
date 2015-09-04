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
	"github.com/golang/protobuf/proto"
	mesos "github.com/mesos/mesos-go/mesosproto"
	util "github.com/mesos/mesos-go/mesosutil"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

const (
	TaskTypeMirrorMaker = "mirrormaker"
)

type TaskState string

const (
	TaskStateInactive TaskState = "inactive"
	TaskStateStopped  TaskState = "stopped"
	TaskStateStaging  TaskState = "staging"
	TaskStateRunning  TaskState = "running"
)

type TaskConfig map[string]string

func (tc TaskConfig) GetString(key string) (string, error) {
	value, ok := tc[key]
	if !ok {
		return "", fmt.Errorf("Key %s missing", key)
	}

	return value, nil
}

func (tc TaskConfig) GetInt(key string) (int64, error) {
	value, ok := tc[key]
	if !ok {
		return math.MinInt64, fmt.Errorf("Key %s missing", key)
	}

	intValue, err := strconv.Atoi(value)
	if err != nil {
		return math.MinInt64, err
	}

	return int64(intValue), nil
}

func (tc TaskConfig) SetStringConfig(key string, where *string) {
	value, err := tc.GetString(key)
	if err == nil {
		*where = value
	}
}

func (tc TaskConfig) SetStringSliceConfig(key string, where *[]string) {
	value, err := tc.GetString(key)
	if err == nil {
		*where = strings.Split(value, ",")
	}
}

func (tc TaskConfig) SetIntConfig(key string, where *int) {
	value, err := tc.GetInt(key)
	if err == nil {
		*where = int(value)
	}
}

//TODO i don't like that every implementor will have to implement plenty of similar stuff
type Task interface {
	GetID() string
	GetTaskID() string
	GetState() TaskState
	SetState(TaskState)
	Matches(*mesos.Offer) string
	CreateTaskInfo(*mesos.Offer) *mesos.TaskInfo
	Update(url.Values) error
}

func NewTaskFromRequest(r *http.Request) (Task, error) {
	taskType := r.URL.Query().Get("type")
	id := r.URL.Query().Get("id")
	if id == "" {
		return nil, errors.New("No task id supplied")
	}

	switch taskType {
	case TaskTypeMirrorMaker:
		return NewMirrorMakerTask(id, r.URL.Query())
	default:
		return nil, fmt.Errorf("Unknown task type %s", taskType)
	}
}

type MirrorMakerTask struct {
	ID     string
	taskID string
	state  TaskState
	cpu    float64
	mem    float64
	config map[string]string
}

func NewMirrorMakerTask(id string, queryParams url.Values) (*MirrorMakerTask, error) {
	cpu, err := strconv.ParseFloat(queryParams.Get("cpu"), 64)
	if err != nil {
		return nil, err
	}
	mem, err := strconv.ParseFloat(queryParams.Get("mem"), 64)
	if err != nil {
		return nil, err
	}

	return &MirrorMakerTask{
		ID:     id,
		state:  TaskStateInactive,
		cpu:    cpu,
		mem:    mem,
		config: make(map[string]string),
	}, nil
}

func (mm *MirrorMakerTask) GetID() string {
	return mm.ID
}

func (mm *MirrorMakerTask) GetTaskID() string {
	return mm.taskID
}

func (mm *MirrorMakerTask) GetState() TaskState {
	return mm.state
}

func (mm *MirrorMakerTask) SetState(state TaskState) {
	mm.state = state
}

func (mm *MirrorMakerTask) Matches(offer *mesos.Offer) string {
	if mm.cpu > getScalarResources(offer, "cpus") {
		return "no cpus"
	}

	if mm.mem > getScalarResources(offer, "mem") {
		return "no mem"
	}

	return ""
}

func (mm *MirrorMakerTask) CreateTaskInfo(offer *mesos.Offer) *mesos.TaskInfo {
	taskName := fmt.Sprintf("mirrormaker-%s", mm.ID)
	mm.taskID = fmt.Sprintf("%s-%s", taskName, uuid())
	taskId := &mesos.TaskID{
		Value: proto.String(mm.taskID),
	}

	data, err := json.Marshal(mm.config)
	if err != nil {
		panic(err)
	}

	taskInfo := &mesos.TaskInfo{
		Name:     proto.String(taskName),
		TaskId:   taskId,
		SlaveId:  offer.GetSlaveId(),
		Executor: mm.createExecutor(),
		Resources: []*mesos.Resource{
			util.NewScalarResource("cpus", mm.cpu),
			util.NewScalarResource("mem", mm.mem),
		},
		Data: data,
	}

	return taskInfo
}

func (mm *MirrorMakerTask) Update(queryParams url.Values) error {
	updateConfig(queryParams, mm.config)

	return nil
}

func (mm *MirrorMakerTask) createExecutor() *mesos.ExecutorInfo {
	id := fmt.Sprintf("mirrormaker-%s", mm.ID)

	return &mesos.ExecutorInfo{
		ExecutorId: util.NewExecutorID(id),
		Name:       proto.String(id),
		Command: &mesos.CommandInfo{
			Value: proto.String(fmt.Sprintf("./%s --log.level %s --type %s", Config.Executor, Config.LogLevel, TaskTypeMirrorMaker)),
			Uris: []*mesos.CommandInfo_URI{
				&mesos.CommandInfo_URI{
					Value:      proto.String(fmt.Sprintf("%s/resource/%s", Config.Api, Config.Executor)),
					Executable: proto.Bool(true),
				},
				&mesos.CommandInfo_URI{
					Value: proto.String(fmt.Sprintf("%s/resource/%s", Config.Api, mm.config["producer.config"])),
				},
				&mesos.CommandInfo_URI{
					Value: proto.String(fmt.Sprintf("%s/resource/%s", Config.Api, mm.config["consumer.config"])),
				},
			},
		},
	}
}

func updateConfig(queryParams url.Values, config map[string]string) {
	for key, value := range queryParams {
		if len(value) > 0 {
			config[key] = value[0]
		}
	}
}
