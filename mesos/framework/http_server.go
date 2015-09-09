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
	"net/http"
	"strings"

	"fmt"
	"strconv"
	"time"
)

type HttpServer struct {
	address string
}

func NewHttpServer(address string) *HttpServer {
	if strings.HasPrefix(address, "http://") {
		address = address[len("http://"):]
	}
	return &HttpServer{
		address: address,
	}
}

func (hs *HttpServer) Start() {
	http.HandleFunc("/resource/", serveFile)
	http.HandleFunc("/api/add", handleAdd)
	http.HandleFunc("/api/start", handleStart)
	http.HandleFunc("/api/stop", handleStop)
	http.HandleFunc("/api/remove", handleRemove)
	http.HandleFunc("/api/update", handleUpdate)
	http.HandleFunc("/api/status", handleStatus)
	http.ListenAndServe(hs.address, nil)
}

func serveFile(w http.ResponseWriter, r *http.Request) {
	resourceTokens := strings.Split(r.URL.Path, "/")
	resource := resourceTokens[len(resourceTokens)-1]
	Logger.Debugf("Serving file %s", resource)
	http.ServeFile(w, r, resource)
}

func handleAdd(w http.ResponseWriter, r *http.Request) {
	id := idFromRequest(r)
	if sched.cluster.Exists(id) {
		respond(false, fmt.Sprintf("Task with id %s already exists", id), w)
		return
	}

	task, err := NewTaskFromRequest(r)
	if err != nil {
		respond(false, err.Error(), w)
		return
	}

	sched.cluster.Add(task)
	result := fmt.Sprintf("Added task %s\n\n", id)
	result += fmt.Sprintf("cluster:\n  task:\n%s", task)
	respond(true, result, w)
}

func handleUpdate(w http.ResponseWriter, r *http.Request) {
	id := idFromRequest(r)
	task := sched.cluster.Get(id)
	if task == nil {
		respond(false, fmt.Sprintf("Task with id %s does not exist", id), w)
	} else {
		err := task.Data().Update(r.URL.Query())
		if err != nil {
			respond(false, err.Error(), w)
		} else {
			Logger.Infof("Task configuration updated:\n%s", task)
			result := "Configuration updated:\n\n"
			result += fmt.Sprintf("cluster:\n  task:\n%s", task)
			respond(true, result, w)
		}
	}
}

func handleStart(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	timeout, err := strconv.Atoi(r.URL.Query().Get("timeout"))
	if err != nil {
		respond(false, err.Error(), w)
		return
	}
	task := sched.cluster.Get(id)
	if task == nil {
		respond(false, fmt.Sprintf("Task with id %s not found", id), w)
	} else {
		if task.Data().State == TaskStateInactive {
			task.Data().State = TaskStateStopped
			if timeout > 0 {
				if task.Data().WaitFor(TaskStateRunning, time.Duration(timeout)*time.Second) {
					result := fmt.Sprintf("Started task %s\n\n", id)
					result += fmt.Sprintf("cluster:\n  task:\n%s", task)
					respond(true, result, w)
				} else {
					respond(false, fmt.Sprintf("Start task %s timed out after %d seconds", id, timeout), w)
				}
			} else {
				respond(true, fmt.Sprintf("Task %s scheduled to start", id), w)
			}
		} else {
			Logger.Infof("Task %s already started", id)
			respond(false, fmt.Sprintf("Task %s already started", id), w)
		}
	}
}

func handleStatus(w http.ResponseWriter, r *http.Request) {
	tasks := sched.cluster.GetAllTasks()
	response := "cluster:\n"
	for _, task := range tasks {
		response += "  task:\n"
		response += fmt.Sprintf("%s", task)
	}
	respond(true, response, w)
}

func handleStop(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	if sched.cluster.Exists(id) {
		sched.stopTask(sched.cluster.Get(id))
		respond(true, fmt.Sprintf("Stopped task %s", id), w)
	} else {
		respond(false, fmt.Sprintf("Task with id %s does not exist", id), w)
	}
}

func handleRemove(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	if sched.cluster.Exists(id) {
		task := sched.cluster.Get(id)
		if task.Data().State == TaskStateInactive {
			sched.cluster.Remove(id)
			respond(true, fmt.Sprintf("Removed task %s", id), w)
		} else {
			respond(false, fmt.Sprintf("Please stop task %s before removing", id), w)
		}
	} else {
		respond(false, fmt.Sprintf("Task with id %s does not exist", id), w)
	}
}

func idFromRequest(r *http.Request) string {
	return r.URL.Query().Get("id")
}

func respond(success bool, message string, w http.ResponseWriter) {
	response := NewApiResponse(success, message)
	bytes, err := json.Marshal(response)
	if err != nil {
		panic(err) //this shouldn't happen
	}
	if success {
		w.WriteHeader(200)
	} else {
		w.WriteHeader(500)
	}
	w.Write(bytes)
}
