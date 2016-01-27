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
	"io/ioutil"
	"log"
	"net/http"
	"strings"
)

type status struct {
	Frameworks []framework
	Slaves     []slave
}

type slave struct {
	Id  string
	Pid string
}

type framework struct {
	Name  string
	Tasks []task
}

type task struct {
	SlaveId   string `json:"slave_id"`
	Resources resources
}

type resources struct {
	Ports string
}

func getBrokers(master string) []string {
	state := getState(master)
	return kafkaBrokerList(state)
}

func getState(master string) *status {
	stateUrl := fmt.Sprintf("http://%s/state.json", master)
	resp, err := http.Get(stateUrl)
	if err != nil {
		log.Fatal("Can't connect to mesos master", err)
	}
	body, err := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()
	state := &status{}
	err = json.Unmarshal(body, state)
	if err != nil {
		log.Printf("Can't parse state.json %v", err)
	}
	return state
}

func kafkaBrokerList(state *status) []string {
	ports := make(map[string]string)
	for _, framework := range state.Frameworks {
		if framework.Name == "kafka" {
			for _, task := range framework.Tasks {
				brokerPort := strings.Split(task.Resources.Ports, "-")[0]
				brokerPort = brokerPort[1:]
				ports[task.SlaveId] = brokerPort
			}
		}
	}
	return mapSlaves(state.Slaves, ports)
}

func mapSlaves(slaves []slave, ports map[string]string) []string {
	brokers := make([]string, 0)
	for _, slave := range slaves {
		if ports[slave.Id] != "" {
			brokerHost := strings.Split(strings.Split(slave.Pid, "@")[1], ":")[0]
			brokerConn := fmt.Sprintf("%s:%s", brokerHost, ports[slave.Id])
			brokers = append(brokers, brokerConn)
		}
	}
	return brokers
}
