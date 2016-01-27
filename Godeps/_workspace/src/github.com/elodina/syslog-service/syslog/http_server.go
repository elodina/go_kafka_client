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

package syslog

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	mesos "github.com/mesos/mesos-go/mesosproto"
	"strconv"
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
	http.HandleFunc("/api/start", handleStart)
	http.HandleFunc("/api/stop", handleStop)
	http.HandleFunc("/api/update", handleUpdate)
	http.HandleFunc("/api/status", handleStatus)
	http.ListenAndServe(hs.address, nil)
}

func serveFile(w http.ResponseWriter, r *http.Request) {
	resourceTokens := strings.Split(r.URL.Path, "/")
	resource := resourceTokens[len(resourceTokens)-1]
	http.ServeFile(w, r, resource)
}

func handleStart(w http.ResponseWriter, r *http.Request) {
	if Config.CanStart() {
		sched.SetActive(true)
		respond(true, "Servers started", w)
	} else {
		respond(false, "topic and broker.list must be set before starting", w)
	}
}

func handleStop(w http.ResponseWriter, r *http.Request) {
	sched.SetActive(false)
	respond(true, "Servers stopped", w)
}

func handleUpdate(w http.ResponseWriter, r *http.Request) {
	queryParams := r.URL.Query()
	setConfig(queryParams, "producer.properties", &Config.ProducerProperties)
	setConfig(queryParams, "topic", &Config.Topic)
	setConfig(queryParams, "broker.list", &Config.BrokerList)
	setConfig(queryParams, "tcp.port", &Config.TcpPort)
	setConfig(queryParams, "udp.port", &Config.UdpPort)
	setConfig(queryParams, "transform", &Config.Transform)
	setConfig(queryParams, "schema.registry.url", &Config.SchemaRegistryUrl)
	setIntConfig(queryParams, "num.producers", &Config.NumProducers)
	setIntConfig(queryParams, "channel.size", &Config.ChannelSize)

	Logger.Infof("Scheduler configuration updated: \n%s", Config)
	respond(true, "Configuration updated", w)
}

func handleStatus(w http.ResponseWriter, r *http.Request) {
	tasks := sched.cluster.GetAllTasks()
	response := "cluster:\n"
	for host, task := range tasks {
		response += fmt.Sprintf("  server: %d\n", host)
		response += fmt.Sprintf("    id: %s\n", task.GetTaskId().GetValue())
		response += fmt.Sprintf("    slave id: %s\n", task.GetSlaveId().GetValue())
		for _, resource := range task.GetResources() {
			switch *resource.Type {
			case mesos.Value_SCALAR:
				response += fmt.Sprintf("    %s: %f\n", resource.GetName(), resource.GetScalar().GetValue())
			case mesos.Value_RANGES:
				response += fmt.Sprintf("    %s: %s\n", resource.GetName(), resource.GetRanges())
			case mesos.Value_SET:
				response += fmt.Sprintf("    %s: %s\n", resource.GetName(), resource.GetSet())
			}
		}
	}
	respond(true, response, w)
}

func setConfig(queryParams url.Values, name string, config *string) {
	value := queryParams.Get(name)
	if value != "" {
		*config = value
	}
}

func setIntConfig(queryParams url.Values, name string, config *int) {
	value := queryParams.Get(name)
	if value != "" {
		intValue, err := strconv.Atoi(value)
		if err != nil {
			panic(err)
		}
		*config = intValue
	}
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
