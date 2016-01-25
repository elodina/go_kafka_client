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
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	utils "github.com/elodina/go-mesos-utils"
	"github.com/golang/protobuf/proto"
	mesos "github.com/mesos/mesos-go/mesosproto"
)

const (
	TaskTypeMirrorMaker = "mirrormaker"
	TaskTypeConsumer    = "consumer"
)

type TaskState string

const (
	TaskStateInactive    TaskState = "inactive"
	TaskStateStopped     TaskState = "stopped"
	TaskStateStaging     TaskState = "staging"
	TaskStateRunning     TaskState = "running"
	TaskStateReconciling TaskState = "reconciling"
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

type TaskData struct {
	ID          string
	TaskID      string
	SlaveID     string
	ExecutorID  string
	Attributes  map[string]string
	State       TaskState
	Config      TaskConfig
	Cpu         float64
	Mem         float64
	constraints map[string][]utils.Constraint
}

func (td *TaskData) Attribute(name string) string {
	return td.Attributes[name]
}

func (td *TaskData) Constraints() map[string][]utils.Constraint {
	return td.constraints
}

func (td *TaskData) Update(queryParams url.Values) error {
	for key, value := range queryParams {
		if len(value) > 0 {
			switch key {
			case "id", "type":
				continue
			case "cpu":
				{
					cpus, err := strconv.ParseFloat(value[0], 64)
					if err != nil {
						return err
					}
					td.Cpu = cpus
				}
			case "mem":
				{
					mem, err := strconv.ParseFloat(value[0], 64)
					if err != nil {
						return err
					}
					td.Mem = mem
				}
			case "constraints":
				{
					td.constraints = make(map[string][]utils.Constraint)

					rawAttributesAndConstraints := strings.Split(value[0], ",")
					for _, rawAttributeAndConstraint := range rawAttributesAndConstraints {
						attributeAndConstraint := strings.Split(rawAttributeAndConstraint, "=")
						if len(attributeAndConstraint) != 2 {
							return fmt.Errorf("Invalid constraint definition: %s", rawAttributeAndConstraint)
						}
						attribute := attributeAndConstraint[0]
						constraint, err := utils.ParseConstraint(attributeAndConstraint[1])
						if err != nil {
							return err
						}
						td.constraints[attribute] = append(td.constraints[attribute], constraint)
					}
				}
			case "options":
				attributes := strings.Split(value[0], ";")
				for _, attribute := range attributes {
					keypair := strings.Split(attribute, "=")
					if len(keypair) == 2 {
						td.Config[keypair[0]] = keypair[1]
					}
				}
			default:
				td.Config[key] = value[0]
			}
		}
	}

	return nil
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

func (td *TaskData) ResetTaskInfo() {
	td.TaskID = ""
	td.ExecutorID = ""
	td.SlaveID = ""
	td.Attributes = make(map[string]string)
}

func (td *TaskData) String() string {
	response := fmt.Sprintf("    id: %s\n", td.ID)
	response += fmt.Sprintf("    state: %s\n", td.State)
	response += fmt.Sprintf("    cpu: %.2f\n", td.Cpu)
	response += fmt.Sprintf("    mem: %.2f\n", td.Mem)
	if td.TaskID != "" {
		response += fmt.Sprintf("    task id: %s\n", td.TaskID)
	}
	if td.SlaveID != "" {
		response += fmt.Sprintf("    slave id: %s\n", td.SlaveID)
	}
	if td.ExecutorID != "" {
		response += fmt.Sprintf("    executor id: %s\n", td.ExecutorID)
	}
	if len(td.Attributes) > 0 {
		response += "    attributes:\n"
		for key, value := range td.Attributes {
			response += fmt.Sprintf("      %s: %s\n", key, value)
		}
	}
	if len(td.Config) > 0 {
		response += "    configs:\n"
		for key, value := range td.Config {
			response += fmt.Sprintf("      %s: %s\n", key, value)
		}
	}
	if len(td.constraints) > 0 {
		response += "    constraints:\n"
		for key, value := range td.constraints {
			stringValues := make([]string, len(value))
			for i, constraint := range value {
				stringValues[i] = fmt.Sprintf("%s", constraint)
			}
			response += fmt.Sprintf("      %s: %s\n", key, strings.Join(stringValues, ", "))
		}
	}

	return response
}

type Task interface {
	Data() *TaskData
	Matches(*mesos.Offer) string
	NewTaskInfo(*mesos.Offer) *mesos.TaskInfo
}

func NewTaskFromRequest(taskType string, id string, r *http.Request) (Task, error) {
	switch taskType {
	case TaskTypeMirrorMaker:
		return NewMirrorMakerTask(id, r.URL.Query())
	case TaskTypeConsumer:
		return NewConsumerTask(id, r.URL.Query())
	default:
		return nil, fmt.Errorf("Unknown task type %s", taskType)
	}
}

func toURI(resource string) *mesos.CommandInfo_URI {
	value := resource
	if !strings.HasPrefix(resource, "http") {
		resourceTokens := strings.Split(resource, "/")
		value = fmt.Sprintf("%s/resource/%s", Config.Api, resourceTokens[len(resourceTokens)-1])
	}

	return &mesos.CommandInfo_URI{
		Value: proto.String(value),
	}
}
