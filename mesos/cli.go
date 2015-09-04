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
	"math"
)

func main() {
	if err := exec(); err != nil {
		fmt.Printf("Error: %s\n", err)
		os.Exit(1)
	}
}

func exec() error {
	command := stripArgument()
	if command == "" {
		handleHelp()
		return errors.New("No command supplied")
	}

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
	addType := stripArgument()
	if addType == "" {
		handleHelp()
		return errors.New("No task type supplied to add")
	}

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
	id := stripArgument()
	if id == "" {
		return errors.New("No task id supplied to add")
	}

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
	request.PutString("type", framework.TaskTypeMirrorMaker)
	request.PutString("id", id)
	request.PutFloat("cpu", cpu)
	request.PutFloat("mem", mem)

	response := request.Get()

	fmt.Println(response.Message)

	return nil
}

func handleStart() error {
	id := stripArgument()
	if id == "" {
		return errors.New("No task id supplied to start")
	}

	var api string

	flag.StringVar(&api, "api", "", "API host:port for advertizing.")
	flag.Parse()

	if err := resolveApi(api); err != nil {
		return err
	}

	request := framework.NewApiRequest(framework.Config.Api + "/api/start")
	request.PutString("id", id)

	response := request.Get()
	fmt.Println(response.Message)

	return nil
}

func handleStop() error {
	id := stripArgument()
	if id == "" {
		return errors.New("No task id supplied to start")
	}

	var api string
	flag.StringVar(&api, "api", "", "API host:port for advertizing.")
	flag.Parse()

	if err := resolveApi(api); err != nil {
		return err
	}

	request := framework.NewApiRequest(framework.Config.Api + "/api/stop")
	request.PutString("id", id)
	response := request.Get()

	fmt.Println(response.Message)

	return nil
}

func handleUpdate() error {
	id := stripArgument()
	if id == "" {
		return errors.New("No task id supplied to start")
	}

	var api string
	var cpu float64
	var mem float64
	var whitelist string
	var blacklist string
	var consumerConfig consumerConfigs
	var producerConfig string
	var numProducers int64
	var numStreams int64
	var preservePartitions bool
	var preserveOrder bool
	var prefix string
	var channelSize int64
	//TODO constraints

	flag.StringVar(&api, "api", "", "API host:port for advertizing.")
	flag.Float64Var(&cpu, "cpu", math.SmallestNonzeroFloat64, "CPUs per task.")
	flag.Float64Var(&mem, "mem", math.SmallestNonzeroFloat64, "Mem per task.")
	flag.StringVar(&whitelist, "whitelist", "", "Regex pattern for whitelist. Providing both whitelist and blacklist is an error.")
	flag.StringVar(&blacklist, "blacklist", "", "Regex pattern for blacklist. Providing both whitelist and blacklist is an error.")
	flag.StringVar(&producerConfig, "producer.config", "", "Producer config file name.")
	flag.Var(&consumerConfig, "consumer.config", "Consumer config file name.")
	flag.Int64Var(&numProducers, "num.producers", math.MinInt64, "Number of producers.")
	flag.Int64Var(&numStreams, "num.streams", math.MinInt64, "Number of consumption streams.")
	flag.BoolVar(&preservePartitions, "preserve.partitions", false, "Preserve partition number. E.g. if message was read from partition 5 it'll be written to partition 5.")
	flag.BoolVar(&preserveOrder, "preserve.order", false, "E.g. message sequence 1, 2, 3, 4, 5 will remain 1, 2, 3, 4, 5 in destination topic.")
	flag.StringVar(&prefix, "prefix", "", "Destination topic prefix.")
	flag.Int64Var(&channelSize, "queue.size", math.MinInt64, "Maximum number of messages that are buffered between the consumer and producer.")
	flag.Parse()

	if err := resolveApi(api); err != nil {
		return err
	}

	request := framework.NewApiRequest(framework.Config.Api + "/api/update")
	request.PutString("id", id)
	request.PutFloat("cpu", cpu)
	request.PutFloat("mem", mem)
	request.PutString("whitelist", whitelist)
	request.PutString("blacklist", blacklist)
	request.PutString("producer.config", producerConfig)
	request.PutStringSlice("consumer.config", consumerConfig)
	request.PutInt("num.producers", numProducers)
	request.PutInt("num.streams", numStreams)
	request.PutBool("preserve.partitions", preservePartitions)
	request.PutBool("preserve.order", preserveOrder)
	request.PutString("prefix", prefix)
	request.PutInt("queue.size", channelSize)

	response := request.Get()

	fmt.Println(response.Message)

	return nil
}

func stripArgument() string {
	args := os.Args
	if len(args) == 1 {
		return ""
	}

	arg := args[1]
	commandArgs := args[1:]
	os.Args = commandArgs

	return arg
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

type consumerConfigs []string

func (i *consumerConfigs) String() string {
	return fmt.Sprintf("%s", *i)
}

func (i *consumerConfigs) Set(value string) error {
	*i = append(*i, value)
	return nil
}
