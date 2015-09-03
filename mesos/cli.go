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

package main

import (
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/stealthly/go_kafka_client/mesos/framework"
	"strconv"
)

func main() {
	if err := exec(); err != nil {
		fmt.Printf("Error: %s\n", err)
		os.Exit(1)
	}
}

func exec() error {
	args := os.Args
	if len(args) == 1 {
		handleHelp()
		return errors.New("No command supplied")
	}

	command := args[1]
	commandArgs := args[1:]
	os.Args = commandArgs

	switch command {
	case "help":
		return handleHelp()
	case "scheduler":
		return handleScheduler()
	case "add":
		return handleAdd()
	case "start":
		return handleStart()
	case "stop":
		return handleStop()
	case "update":
		return handleUpdate()
	case "status":
		return handleStatus()
	}

	return fmt.Errorf("Unknown command: %s\n", command)
}

func handleHelp() error {
	fmt.Println(`Usage:
  help: show this message
  scheduler: start scheduler
  add: add task
  start: start task
  stop: stop task
  update: update configuration
  status: get current cluster status
More help you can get from ./cli <command> -h`)
	return nil
}

func handleStatus() error {
	var api string
	flag.StringVar(&api, "api", "", "Binding host:port for http/artifact server. Optional if SM_API env is set.")

	flag.Parse()
	if err := resolveApi(api); err != nil {
		return err
	}
	response := framework.NewApiRequest(framework.Config.Api + "/api/status").Get()
	fmt.Println(response.Message)
	return nil
}

func handleScheduler() error {
	var api string
	var user string
	var logLevel string

	flag.StringVar(&framework.Config.Master, "master", "", "Mesos Master addresses.")
	flag.StringVar(&api, "api", "", "API host:port for advertizing.")
	flag.StringVar(&user, "user", "", "Mesos user. Defaults to current system user")
	flag.StringVar(&logLevel, "log.level", framework.Config.LogLevel, "Log level. trace|debug|info|warn|error|critical. Defaults to info.")
	flag.StringVar(&framework.Config.FrameworkName, "framework.name", framework.Config.FrameworkName, "Framework name.")
	flag.StringVar(&framework.Config.FrameworkRole, "framework.role", framework.Config.FrameworkRole, "Framework role.")

	flag.Parse()

	if err := resolveApi(api); err != nil {
		return err
	}

	if err := framework.InitLogging(logLevel); err != nil {
		return err
	}

	if framework.Config.Master == "" {
		return errors.New("--master flag is required.")
	}

	return new(framework.Scheduler).Start()
}

func handleAdd() error {
	args := os.Args
	if len(args) == 1 {
		handleHelp()
		return errors.New("No task type supplied to add")
	}

	addType := args[1]
	commandArgs := args[1:]
	os.Args = commandArgs

	switch addType {
	case framework.TaskTypeMirrorMaker:
		return handleAddMirrorMaker()
	default:
		{
			handleHelp()
			return fmt.Errorf("Unknown task type %s", addType)
		}
	}
}

func handleAddMirrorMaker() error {
	args := os.Args
	if len(args) == 1 {
		return errors.New("No task id supplied to add")
	}

	id := args[1]
	commandArgs := args[1:]
	os.Args = commandArgs

	var api string
	var cpu float64
	var mem float64
	//TODO constraints

	flag.StringVar(&api, "api", "", "API host:port for advertizing.")
	flag.Float64Var(&cpu, "cpu", 0.5, "CPUs per task.")
	flag.Float64Var(&mem, "mem", 512, "Mem per task.")
	flag.Parse()

	if err := resolveApi(api); err != nil {
		return err
	}

	request := framework.NewApiRequest(framework.Config.Api + "/api/add")
	request.AddParam("type", framework.TaskTypeMirrorMaker)
	request.AddParam("id", id)
	request.AddParam("cpu", strconv.FormatFloat(cpu, 'E', -1, 64))
	request.AddParam("mem", strconv.FormatFloat(mem, 'E', -1, 64))

	response := request.Get()

	fmt.Println(response.Message)

	return nil
}

func handleStart() error {
	args := os.Args
	if len(args) == 1 {
		return errors.New("No task id supplied to start")
	}

	id := args[1]
	commandArgs := args[1:]
	os.Args = commandArgs

	var api string

	flag.StringVar(&api, "api", "", "API host:port for advertizing.")
	flag.Parse()

	if err := resolveApi(api); err != nil {
		return err
	}

	request := framework.NewApiRequest(framework.Config.Api + "/api/start")
	request.AddParam("id", id)

	response := request.Get()
	fmt.Println(response.Message)

	return nil
}

func handleStop() error {
	return nil
}

func handleUpdate() error {
	args := os.Args
	if len(args) == 1 {
		return errors.New("No task id supplied to update")
	}

	id := args[1]
	commandArgs := args[1:]
	os.Args = commandArgs

	var api string
	var cpu float64
	var mem float64
	var whitelist string
	var blacklist string
	var producerConfig string
	var consumerConfig string
	//TODO constraints

	flag.StringVar(&api, "api", "", "API host:port for advertizing.")
	flag.Float64Var(&cpu, "cpu", 0.0, "CPUs per task.")
	flag.Float64Var(&mem, "mem", 0.0, "Mem per task.")
	flag.StringVar(&whitelist, "whitelist", "", "Whitelist to consume.")
	flag.StringVar(&blacklist, "blacklist", "", "Blacklist to consume.")
	flag.StringVar(&producerConfig, "producer.config", "", "Producer config file name.")
	flag.StringVar(&consumerConfig, "consumer.config", "", "Consumer config file name.")
	flag.Parse()

	if err := resolveApi(api); err != nil {
		return err
	}

	request := framework.NewApiRequest(framework.Config.Api + "/api/update")
	request.AddParam("id", id)
	if cpu != 0.0 {
		request.AddParam("cpu", strconv.FormatFloat(cpu, 'E', -1, 64))
	}
	if mem != 0.0 {
		request.AddParam("mem", strconv.FormatFloat(mem, 'E', -1, 64))
	}
	request.AddParam("whitelist", whitelist)
	request.AddParam("blacklist", blacklist)
	request.AddParam("producer.config", producerConfig)
	request.AddParam("consumer.config", consumerConfig)

	response := request.Get()

	fmt.Println(response.Message)

	return nil
}

func resolveApi(api string) error {
	if api != "" {
		framework.Config.Api = api
		return nil
	}

	if os.Getenv("GM_API") != "" {
		framework.Config.Api = os.Getenv("GM_API")
		return nil
	}

	return errors.New("Undefined API url. Please provide either a CLI --api option or GM_API env.")
}
