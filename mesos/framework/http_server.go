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
	http.HandleFunc("/health", handleHealth)
	http.ListenAndServe(hs.address, nil)
}

func serveFile(w http.ResponseWriter, r *http.Request) {
	resourceTokens := strings.Split(r.URL.Path, "/")
	resource := resourceTokens[len(resourceTokens)-1]
	Logger.Debugf("Serving file %s", resource)
	http.ServeFile(w, r, resource)
}

func handleAdd(w http.ResponseWriter, r *http.Request) {
	idExpr := idFromRequest(r)
	ids, err := sched.cluster.ExpandIDs(idExpr)
	if err != nil {
		respond(false, err.Error(), w)
		return
	}

	for _, id := range ids {
		if sched.cluster.Exists(id) {
			respond(false, fmt.Sprintf("Task with id %s already exists", id), w)
			return
		}
	}

	tasks := make([]Task, 0)

	taskType := r.URL.Query().Get("type")
	for _, id := range ids {
		task, err := NewTaskFromRequest(taskType, id, r)
		if err != nil {
			respond(false, err.Error(), w)
			return
		}

		sched.cluster.Add(task)
		tasks = append(tasks, task)
	}

	result := fmt.Sprintf("Added tasks %s\n\n", idExpr)
	result += "cluster:\n"
	for _, task := range tasks {
		result += fmt.Sprintf("  task:\n%s", task)
	}
	sched.cluster.Save()
	respond(true, result, w)
}

func handleUpdate(w http.ResponseWriter, r *http.Request) {
	idExpr := idFromRequest(r)
	ids, err := sched.cluster.ExpandIDs(idExpr)
	if err != nil {
		respond(false, err.Error(), w)
		return
	}

	for _, id := range ids {
		if !sched.cluster.Exists(id) {
			respond(false, fmt.Sprintf("Task with id %s does not exist", id), w)
			return
		}
	}

	tasks := make([]Task, 0)
	for _, id := range ids {
		task := sched.cluster.Get(id)
		err := task.Data().Update(r.URL.Query())
		if err != nil {
			respond(false, err.Error(), w)
			return
		} else {
			Logger.Infof("Task configuration updated:\n%s", task)
			tasks = append(tasks, task)
		}
	}

	result := "Configuration updated:\n\n"
	result += "cluster:\n"
	for _, task := range tasks {
		result += fmt.Sprintf("  task:\n%s", task)
	}

	sched.cluster.Save()
	respond(true, result, w)
}

func handleStart(w http.ResponseWriter, r *http.Request) {
	idExpr := idFromRequest(r)
	ids, err := sched.cluster.ExpandIDs(idExpr)
	if err != nil {
		respond(false, err.Error(), w)
		return
	}

	for _, id := range ids {
		if !sched.cluster.Exists(id) {
			respond(false, fmt.Sprintf("Task with id %s does not exist", id), w)
			return
		}
	}

	timeout, err := strconv.Atoi(r.URL.Query().Get("timeout"))
	if err != nil {
		respond(false, err.Error(), w)
		return
	}

	result := fmt.Sprintf("Started tasks %s\n\n", idExpr)
	result += "cluster:\n"
	for _, id := range ids {
		task := sched.cluster.Get(id)

		if task.Data().State == TaskStateInactive {
			task.Data().State = TaskStateStopped
			if timeout > 0 {
				if task.Data().WaitFor(TaskStateRunning, time.Duration(timeout)*time.Second) {
					result += fmt.Sprintf("  task:\n%s", task)
				} else {
					respond(false, fmt.Sprintf("Start task %s timed out after %d seconds", id, timeout), w)
					return
				}
			} else {
				respond(true, fmt.Sprintf("Tasks %s scheduled to start", idExpr), w)
				return
			}
		} else {
			Logger.Infof("Task %s already started", id)
			respond(false, fmt.Sprintf("Task %s already started", id), w)
			return
		}
	}

	respond(true, result, w)
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
	idExpr := idFromRequest(r)
	ids, err := sched.cluster.ExpandIDs(idExpr)
	if err != nil {
		respond(false, err.Error(), w)
		return
	}

	for _, id := range ids {
		if !sched.cluster.Exists(id) {
			respond(false, fmt.Sprintf("Task with id %s does not exist", id), w)
			return
		}
	}

	for _, id := range ids {
		sched.stopTask(sched.cluster.Get(id))
	}

	sched.cluster.Save()
	respond(true, fmt.Sprintf("Stopped tasks %s", idExpr), w)
}

func handleRemove(w http.ResponseWriter, r *http.Request) {
	idExpr := idFromRequest(r)
	ids, err := sched.cluster.ExpandIDs(idExpr)
	if err != nil {
		respond(false, err.Error(), w)
		return
	}

	for _, id := range ids {
		if !sched.cluster.Exists(id) {
			respond(false, fmt.Sprintf("Task with id %s does not exist", id), w)
			return
		}
	}

	for _, id := range ids {
		task := sched.cluster.Get(id)
		if task.Data().State == TaskStateInactive {
			sched.cluster.Remove(id)
		} else {
			respond(false, fmt.Sprintf("Please stop task %s before removing", id), w)
			return
		}
	}

	sched.cluster.Save()
	respond(true, fmt.Sprintf("Removed tasks %s", idExpr), w)
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
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
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusBadRequest)
	}
	w.Write(bytes)
}
