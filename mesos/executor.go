// +build executor

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
	kafka "github.com/stealthly/go_kafka_client"
	"github.com/mesos/mesos-go/executor"
	"fmt"
	"flag"
	"os"
	"strings"
)

var zookeeper = flag.String("zookeeper", "", "Zookeeper connection string separated by comma.")
var group = flag.String("group", "", "Consumer group name to start consumers in.")
var topic = flag.String("topic", "", "Topic to consume.")
var partition = flag.Int("partition", 0, "Partition to consume. Defaults to 0.")
var whitelist = flag.String("whitelist", "", "Whitelist of topics to consume.")
var blacklist = flag.String("blacklist", "", "Blacklist of topics to consume.")

var logLevel = flag.String("log.level", "info", "Log level for built-in logger.")

func parseAndValidateExecutorArgs() {
	flag.Parse()

	if *zookeeper == "" {
		fmt.Println("Zookeeper connection string is required.")
		os.Exit(1)
	}

	isStatic := *whitelist == "" && *blacklist == ""

	if isStatic {
		if *topic == "" {
			fmt.Println("Topic to consume is required.")
			os.Exit(1)
		}

		if *partition < 0 {
			fmt.Println("Partition to consume should be >= 0.")
			os.Exit(1)
		}
	}

	if *group == "" {
		fmt.Println("Consumer group name is required.")
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

func main() {
	parseAndValidateExecutorArgs()
	setLogLevel()
	fmt.Println("Starting Go Kafka Client Executor")

	executorConfig := kafka.NewExecutorConfig()

	if *whitelist != "" {
		executorConfig.Filter = kafka.NewWhiteList(*whitelist)
	} else if *blacklist != "" {
		executorConfig.Filter = kafka.NewBlackList(*blacklist)
	}

	executorConfig.Zookeeper = strings.Split(*zookeeper, ",")
	executorConfig.Group = *group
	executorConfig.Topic = *topic
	executorConfig.Partition = int32(*partition)
	driver, err := executor.NewMesosExecutorDriver(kafka.NewGoKafkaClientExecutor(executorConfig))

	if err != nil {
		fmt.Println("Unable to create a ExecutorDriver ", err.Error())
	}

	_, err = driver.Start()
	if err != nil {
		fmt.Println("Got error:", err)
		return
	}
	fmt.Println("Executor process has started and running.")
	driver.Join()
}
