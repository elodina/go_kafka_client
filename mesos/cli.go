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

	"math"

	"github.com/stealthly/go_kafka_client/mesos/framework"
)

var executor = flag.String("executor", "", "Executor binary name")

func main() {
	if err := exec(); err != nil {
		fmt.Printf("Error: %s\n", err)
		os.Exit(1)
	}
}

func exec() error {
	command := stripArgument()
	if command == "" {
		handleHelp("")
		return errors.New("No command supplied")
	}

	switch command {
	case "help":
		return handleHelp(stripArgument())
	case "scheduler":
		return handleScheduler()
	case "add":
		return handleAdd()
	case "start":
		return handleStart()
	case "stop":
		return handleStop()
	case "remove":
		return handleRemove()
	case "update":
		return handleUpdate()
	case "status":
		return handleStatus()
	}

	return fmt.Errorf("Unknown command: %s\n", command)
}

func handleHelp(cmd string) error {
	switch cmd {
	case "scheduler":
		handleHelpScheduler()
	case "add":
		handleHelpAdd()
	case "update":
		handleHelpUpdate()
	case "start":
		handleHelpStart()
	case "status":
		handleHelpStatus()
	case "stop":
		handleHelpStop()
	case "remove":
		handleHelpRemove()
	default:
		handleGenericHelp()
	}
	return nil
}

func handleGenericHelp() {
	fmt.Println(`Usage:
  help: show this message
  scheduler: start scheduler
  add: add task
  update: update configuration
  start: start task
  status: get current cluster status
  stop: stop task
  remove: remove task
Get detailed help from ./cli help <command>`)
}

func handleHelpScheduler() {
	fmt.Println(`Usage: scheduler [options]

Options:
    --master: Mesos Master addresses.
    --api: API host:port for advertizing.
    --user: Mesos user. Defaults to current system user.
    --storage: Storage for cluster state. Examples: file:go_kafka_client_mesos.json; zk:master:2181/go-mesos.
    --log.level: Log level. trace|debug|info|warn|error|critical. Defaults to info.
    --framework.name: Framework name.
    --framework.role: Framework role.
    --framework.timeout: Framework failover timeout.`)
}

func handleHelpAdd() {
	fmt.Println(`Usage: add <type> <id-expr> [options]

Types:
    mirrormaker
    consumer

Options:
		--executor: Executor binary name
    --api: API host:port for advertizing. Optional if GM_API env is set.
    --cpu: CPUs per task. Defaults to 0.5.
    --mem: Mem per task. Defaults to 512.
    --constraints: Constraints (hostname=like:^master$,rack=like:^1.*$).
    `)
	printIDExprExamples()
	fmt.Println()
	printConstraintExamples()
}

func handleHelpUpdate() {
	fmt.Println(`Usage: update <id-expr> [options]

Options:
    --api: API host:port for advertizing. Optional if GM_API env is set.
    --cpu: CPUs per task.
    --mem: Mem per task.
    --constraints: Constraints (hostname=like:^master$,rack=like:^1.*$).
    --whitelist: Regex pattern for whitelist. Providing both whitelist and blacklist is an error.
    --blacklist: Regex pattern for blacklist. Providing both whitelist and blacklist is an error.
    --producer.config: Producer config file name.
    --consumer.config: Consumer config file name.
    --num.producers: Number of producers.
    --num.streams: Number of consumption streams.
    --preserve.partitions: Preserve partition number. E.g. if message was read from partition 5 it'll be written to partition 5.
    --preserve.order: E.g. message sequence 1, 2, 3, 4, 5 will remain 1, 2, 3, 4, 5 in destination topic.
    --prefix: Destination topic prefix.")
    --queue.size: Maximum number of messages that are buffered between the consumer and producer.
    `)
	printIDExprExamples()
	printConstraintExamples()
}

func handleHelpStart() {
	fmt.Println(`Usage: start <id-expr> [options]

Options:
    --api: API host:port for advertizing. Optional if GM_API env is set.
    --timeout: Timeout in seconds to wait until the task receives Running status.
    `)
	printIDExprExamples()
}

func handleHelpStatus() {
	fmt.Println(`Usage: status [options]

Options:
    --api: API host:port for advertizing. Optional if GM_API env is set.`)
}

func handleHelpStop() {
	fmt.Println(`Usage: stop <id-expr> [options]

Options:
    --api: API host:port for advertizing. Optional if GM_API env is set.
    `)
	printIDExprExamples()
}

func handleHelpRemove() {
	fmt.Println(`Usage: remove <id-expr> [options]

Options:
    --api: API host:port for advertizing. Optional if GM_API env is set.
    `)
	printIDExprExamples()
}

func printIDExprExamples() {
	fmt.Println(`id-expr examples:
    0      - task 0
    0,1    - tasks 0,1
    0..2   - tasks 0,1,2
    0,1..2 - tasks 0,1,2
    *      - all tasks in cluster`)
}

func printConstraintExamples() {
	fmt.Println(`constraint examples:
    like:slave0    - value equals 'slave0'
    unlike:slave0  - value is not equal to 'slave0'
    like:slave.*   - value starts with 'slave'
    unique         - all values are unique
    cluster        - all values are the same
    cluster:slave0 - value equals 'slave0'
    groupBy        - all values are the same
    groupBy:3      - all values are within 3 different groups`)
}

func handleStatus() error {
	var api string
	flag.StringVar(&api, "api", "", "Binding host:port for http/artifact server. Optional if SM_API env is set.")

	ParseFlags("status")
	if err := resolveApi(api); err != nil {
		return err
	}
	response := framework.NewApiRequest(framework.Config.Api + "/api/status").Get()
	fmt.Println(response.Message)
	return nil
}

func handleScheduler() error {
	var api string
	var logLevel string

	flag.StringVar(&framework.Config.Master, "master", "", "Mesos Master addresses.")
	flag.StringVar(&api, "api", "", "API host:port for advertizing.")
	flag.StringVar(&framework.Config.User, "user", "", "Mesos user. Defaults to current system user")
	flag.StringVar(&framework.Config.Storage, "storage", "file:go_kafka_client_mesos.json", "Storage for cluster state. Examples: file:go_kafka_client_mesos.json; zk:master:2181/go-mesos")
	flag.StringVar(&logLevel, "log.level", framework.Config.LogLevel, "Log level. trace|debug|info|warn|error|critical. Defaults to info.")
	flag.StringVar(&framework.Config.FrameworkName, "framework.name", framework.Config.FrameworkName, "Framework name.")
	flag.StringVar(&framework.Config.FrameworkRole, "framework.role", framework.Config.FrameworkRole, "Framework role.")
	flag.DurationVar(&framework.Config.FrameworkTimeout, "framework.timeout", framework.Config.FrameworkTimeout, "Framework failover timeout.")

	ParseFlags("scheduler")

	if err := resolveApi(api); err != nil {
		return err
	}

	if err := framework.InitLogging(logLevel); err != nil {
		return err
	}

	if framework.Config.Master == "" {
		return errors.New("--master flag is required.")
	}

	return framework.NewScheduler().Start()
}

func handleAdd() error {
	addType := stripArgument()
	if addType == "" {
		handleHelp("add")
		return errors.New("No task type supplied to add")
	}

	switch addType {
	case framework.TaskTypeMirrorMaker:
		return handleAddMirrorMaker()
	case framework.TaskTypeConsumer:
		return handleAddConsumer()
	default:
		{
			handleHelp("add")
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
	var constraints string

	flag.StringVar(&api, "api", "", "API host:port for advertizing.")
	flag.Float64Var(&cpu, "cpu", 0.5, "CPUs per task.")
	flag.Float64Var(&mem, "mem", 512, "Mem per task.")
	flag.StringVar(&constraints, "constraints", "", "Constraints (hostname=like:^master$,rack=like:^1.*$).")
	ParseFlags("add")

	if err := resolveApi(api); err != nil {
		return err
	}

	if *executor == "" {
		return errors.New("Executor name required")
	}

	request := framework.NewApiRequest(framework.Config.Api + "/api/add")
	request.PutString("type", framework.TaskTypeMirrorMaker)
	request.PutString("id", id)
	request.PutFloat("cpu", cpu)
	request.PutFloat("mem", mem)
	request.PutString("constraints", constraints)
	request.PutString("executor", *executor)

	response := request.Get()

	fmt.Println(response.Message)

	return nil
}

func handleAddConsumer() error {
	id := stripArgument()
	if id == "" {
		return errors.New("No task id supplied to add")
	}
	var (
		api = flag.String("api", "", "API host:port")
		cpu = flag.Float64("cpu", 0.1, "CPUs per task")
		mem = flag.Float64("mem", 128, "Mem per task")
	)
	ParseFlags("add")
	if err := resolveApi(*api); err != nil {
		return err
	}

	if *executor == "" {
		return errors.New("Executor name required")
	}

	request := framework.NewApiRequest(framework.Config.Api + "/api/add")
	request.PutString("type", framework.TaskTypeConsumer)
	request.PutString("id", id)
	request.PutFloat("cpu", *cpu)
	request.PutFloat("mem", *mem)
	request.PutString("executor", *executor)

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
	var timeout int64

	flag.StringVar(&api, "api", "", "API host:port for advertizing.")
	flag.Int64Var(&timeout, "timeout", 30, "Timeout in seconds to wait until the task receives Running status.")

	ParseFlags("start")

	if err := resolveApi(api); err != nil {
		return err
	}

	request := framework.NewApiRequest(framework.Config.Api + "/api/start")
	request.PutString("id", id)
	request.PutInt("timeout", timeout)

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
	ParseFlags("stop")

	if err := resolveApi(api); err != nil {
		return err
	}

	request := framework.NewApiRequest(framework.Config.Api + "/api/stop")
	request.PutString("id", id)
	response := request.Get()

	fmt.Println(response.Message)

	return nil
}

func handleRemove() error {
	id := stripArgument()
	if id == "" {
		return errors.New("No task id supplied to remove")
	}

	var api string
	flag.StringVar(&api, "api", "", "API host:port for advertizing.")
	ParseFlags("remove")

	if err := resolveApi(api); err != nil {
		return err
	}

	request := framework.NewApiRequest(framework.Config.Api + "/api/remove")
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
	preservePartitions := new(boolFlag)
	preserveOrder := new(boolFlag)
	var prefix string
	var channelSize int64
	var constraints string
	var options string

	flag.StringVar(&api, "api", "", "API host:port for advertizing.")
	flag.Float64Var(&cpu, "cpu", math.SmallestNonzeroFloat64, "CPUs per task.")
	flag.Float64Var(&mem, "mem", math.SmallestNonzeroFloat64, "Mem per task.")
	flag.StringVar(&whitelist, "whitelist", "", "Regex pattern for whitelist. Providing both whitelist and blacklist is an error.")
	flag.StringVar(&blacklist, "blacklist", "", "Regex pattern for blacklist. Providing both whitelist and blacklist is an error.")
	flag.StringVar(&producerConfig, "producer.config", "", "Producer config url or file name.")
	flag.Var(&consumerConfig, "consumer.config", "Consumer config url or file name.")
	flag.Int64Var(&numProducers, "num.producers", math.MinInt64, "Number of producers.")
	flag.Int64Var(&numStreams, "num.streams", math.MinInt64, "Number of consumption streams.")
	flag.Var(preservePartitions, "preserve.partitions", "Preserve partition number. E.g. if message was read from partition 5 it'll be written to partition 5.")
	flag.Var(preserveOrder, "preserve.order", "E.g. message sequence 1, 2, 3, 4, 5 will remain 1, 2, 3, 4, 5 in destination topic.")
	flag.StringVar(&prefix, "prefix", "", "Destination topic prefix.")
	flag.Int64Var(&channelSize, "queue.size", math.MinInt64, "Maximum number of messages that are buffered between the consumer and producer.")
	flag.StringVar(&constraints, "constraints", "", "Constraints (hostname=like:^master$,rack=like:^1.*$).")
	flag.StringVar(&options, "options", "", "Additional options")
	ParseFlags("update")

	if err := resolveApi(api); err != nil {
		return err
	}

	request := framework.NewApiRequest(framework.Config.Api + "/api/update")
	request.PutString("id", id)
	request.PutFloat("cpu", cpu)
	request.PutFloat("mem", mem)
	request.PutString("constraints", constraints)
	request.PutString("whitelist", whitelist)
	request.PutString("blacklist", blacklist)
	request.PutString("producer.config", producerConfig)
	request.PutStringSlice("consumer.config", consumerConfig)
	request.PutInt("num.producers", numProducers)
	request.PutInt("num.streams", numStreams)
	if preservePartitions.isSet {
		request.PutBool("preserve.partitions", preservePartitions.value)
	}
	if preserveOrder.isSet {
		request.PutBool("preserve.order", preserveOrder.value)
	}
	request.PutString("prefix", prefix)
	request.PutInt("queue.size", channelSize)
	request.PutString("options", options)

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

func ParseFlags(cmd string) {
	flag.CommandLine.Usage = func() {
		handleHelp(cmd)
		fmt.Println()
	}
	flag.Parse()
}

type consumerConfigs []string

func (i *consumerConfigs) String() string {
	return fmt.Sprintf("%s", *i)
}

func (i *consumerConfigs) Set(value string) error {
	*i = append(*i, value)
	return nil
}

type boolFlag struct {
	isSet bool
	value bool
}

func (b *boolFlag) String() string {
	return fmt.Sprintf("%t", b.value)
}

func (b *boolFlag) Set(value string) error {
	if value == "" || value == "true" {
		b.value = true
		b.isSet = true
		return nil
	} else if value == "false" {
		b.value = false
		b.isSet = true
		return nil
	}

	return fmt.Errorf("Invalid value: %s", value)
}
