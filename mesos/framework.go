// +build scheduler

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
	"flag"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/mesos/mesos-go/mesosproto"
	"github.com/mesos/mesos-go/scheduler"
	kafka "github.com/stealthly/go_kafka_client"
	"net/http"
	"os"
	"os/signal"
	"strings"
)

var cliEndpoint = flag.String("cli.endpoint", "0.0.0.0:12321", "Address to run the CLI server at.")

var artifactServerHost = flag.String("artifact.host", "master", "Binding host for artifact server.")
var artifactServerPort = flag.Int("artifact.port", 6666, "Binding port for artifact server.")
var master = flag.String("master", "127.0.0.1:5050", "Mesos Master address <ip:port>.")
var cpuPerConsumer = flag.Float64("cpu.per.consumer", 1, "CPUs per consumer instance.")
var memPerConsumer = flag.Float64("mem.per.consumer", 256, "Memory per consumer instance.")
var executorArchiveName = flag.String("executor.archive", "executor.zip", "Executor archive name. Absolute or relative path are both ok.")
var executorBinaryName = flag.String("executor.name", "executor", "Executor binary name contained in archive.")

var zookeeper = flag.String("zookeeper", "", "Zookeeper connection string separated by comma.")
var group = flag.String("group", "", "Consumer group name to start consumers in.")
var whitelist = flag.String("whitelist", "", "Whitelist of topics to consume.")
var blacklist = flag.String("blacklist", "", "Blacklist of topics to consume.")
var numConsumers = flag.Int("num.consumers", 1, "Number of consumers for non-static configuration.")
var static = flag.Bool("static", true, "Flag to use static partition configuration. Defaults to true. <num.consumers> is ignored when set to true.")

var logLevel = flag.String("log.level", "info", "Log level for built-in logger.")

func parseAndValidateSchedulerArgs() {
	flag.Parse()

	if *zookeeper == "" {
		fmt.Println("Zookeeper connection string is required.")
		flag.Usage()
		os.Exit(1)
	}

	if *group == "" {
		fmt.Println("Consumer group name is required.")
		flag.Usage()
		os.Exit(1)
	}

	if *whitelist == "" && *blacklist == "" {
		fmt.Println("Whitelist or blacklist of topics to consume is required.")
		flag.Usage()
		os.Exit(1)
	}
}

func setLogLevel() {
	var level kafka.LogLevel
	switch strings.ToLower(*logLevel) {
	case "trace":
		level = kafka.TraceLevel
	case "debug":
		level = kafka.DebugLevel
	case "info":
		level = kafka.InfoLevel
	case "warn":
		level = kafka.WarnLevel
	case "error":
		level = kafka.ErrorLevel
	case "critical":
		level = kafka.CriticalLevel
	default:
		{
			fmt.Printf("Invalid log level: %s\n", *logLevel)
			os.Exit(1)
		}
	}
	kafka.Logger = kafka.NewDefaultLogger(level)
}

func startArtifactServer() {
	//if the full path is given, take the last token only
	path := strings.Split(*executorArchiveName, "/")
	http.HandleFunc(fmt.Sprintf("/%s", path[len(path)-1]), func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, *executorArchiveName)
	})
	http.ListenAndServe(fmt.Sprintf("%s:%d", *artifactServerHost, *artifactServerPort), nil)
}

func main() {
	parseAndValidateSchedulerArgs()
	setLogLevel()

	ctrlc := make(chan os.Signal, 1)
	signal.Notify(ctrlc, os.Interrupt)

	go startArtifactServer()

	frameworkInfo := &mesosproto.FrameworkInfo{
		User: proto.String(""),
		Name: proto.String("Go Kafka Client Framework"),
	}

	schedulerConfig := kafka.NewSchedulerConfig()

	var filter kafka.TopicFilter
	if *whitelist != "" {
		filter = kafka.NewWhiteList(*whitelist)
	} else {
		filter = kafka.NewBlackList(*blacklist)
	}

	schedulerConfig.CpuPerTask = *cpuPerConsumer
	schedulerConfig.MemPerTask = *memPerConsumer
	schedulerConfig.Filter = filter
	schedulerConfig.Zookeeper = strings.Split(*zookeeper, ",")
	schedulerConfig.GroupId = *group
	schedulerConfig.ExecutorBinaryName = *executorBinaryName
	schedulerConfig.ExecutorArchiveName = *executorArchiveName
	schedulerConfig.ArtifactServerHost = *artifactServerHost
	schedulerConfig.ArtifactServerPort = *artifactServerPort
	schedulerConfig.LogLevel = *logLevel
	var tracker kafka.ConsumerTracker
	if *static {
		track, err := kafka.NewStaticConsumerTracker(schedulerConfig)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		tracker = track
	} else {
		tracker = kafka.NewLoadBalancingConsumerTracker(schedulerConfig, *numConsumers)
	}
	consumerScheduler, err := kafka.NewGoKafkaClientScheduler(schedulerConfig, tracker, *cliEndpoint)
    if err != nil {
        fmt.Println(err)
        os.Exit(1)
    }

	driver, err := scheduler.NewMesosSchedulerDriver(consumerScheduler, frameworkInfo, *master, nil)
	go func() {
		<-ctrlc
		consumerScheduler.Shutdown(driver)
		driver.Stop(false)
	}()

	if err != nil {
		fmt.Println("Unable to create a SchedulerDriver ", err.Error())
	}

	if stat, err := driver.Run(); err != nil {
		fmt.Println("Framework stopped with status %s and error: %s\n", stat.String(), err.Error())
	}
}
