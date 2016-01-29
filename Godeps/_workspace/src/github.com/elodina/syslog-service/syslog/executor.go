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
	"os"

	"fmt"
	"github.com/elodina/go-kafka-avro"
	"github.com/elodina/siesta-producer"
	"github.com/mesos/mesos-go/executor"
	mesos "github.com/mesos/mesos-go/mesosproto"
	"time"
)

type Executor struct {
	tcpPort  int
	udpPort  int
	hostname string
	producer *SyslogProducer
	close    chan struct{}
}

func NewExecutor(tcpPort int, udpPort int, hostname string) *Executor {
	return &Executor{
		tcpPort:  tcpPort,
		udpPort:  udpPort,
		hostname: hostname,
		close:    make(chan struct{}, 1),
	}
}

func (e *Executor) Registered(driver executor.ExecutorDriver, executor *mesos.ExecutorInfo, framework *mesos.FrameworkInfo, slave *mesos.SlaveInfo) {
	Logger.Infof("[Registered] framework: %s slave: %s", framework.GetId().GetValue(), slave.GetId().GetValue())
}

func (e *Executor) Reregistered(driver executor.ExecutorDriver, slave *mesos.SlaveInfo) {
	Logger.Infof("[Reregistered] slave: %s", slave.GetId().GetValue())
}

func (e *Executor) Disconnected(executor.ExecutorDriver) {
	Logger.Info("[Disconnected]")
}

func (e *Executor) LaunchTask(driver executor.ExecutorDriver, task *mesos.TaskInfo) {
	Logger.Infof("[LaunchTask] %s", task)

	Config.Read(task)

	runStatus := &mesos.TaskStatus{
		TaskId: task.GetTaskId(),
		State:  mesos.TaskState_TASK_RUNNING.Enum(),
	}

	if _, err := driver.SendStatusUpdate(runStatus); err != nil {
		Logger.Errorf("Failed to send status update: %s", runStatus)
	}

	go func() {
		e.producer = e.newSyslogProducer()
		e.producer.Start()
		<-e.close

		// finish task
		Logger.Infof("Finishing task %s", task.GetName())
		finStatus := &mesos.TaskStatus{
			TaskId: task.GetTaskId(),
			State:  mesos.TaskState_TASK_FINISHED.Enum(),
		}
		if _, err := driver.SendStatusUpdate(finStatus); err != nil {
			Logger.Errorf("Failed to send status update: %s", finStatus)
			os.Exit(1)
		}
		Logger.Infof("Task %s has finished", task.GetName())
		time.Sleep(time.Second)
		os.Exit(0)
	}()
}

func (e *Executor) KillTask(driver executor.ExecutorDriver, id *mesos.TaskID) {
	Logger.Infof("[KillTask] %s", id.GetValue())
	e.producer.Stop()
	e.close <- struct{}{}
}

func (e *Executor) FrameworkMessage(driver executor.ExecutorDriver, message string) {
	Logger.Infof("[FrameworkMessage] %s", message)
}

func (e *Executor) Shutdown(driver executor.ExecutorDriver) {
	Logger.Infof("[Shutdown]")
	e.producer.Stop()
	e.close <- struct{}{}
}

func (e *Executor) Error(driver executor.ExecutorDriver, message string) {
	Logger.Errorf("[Error] %s", message)
}

func (e *Executor) newSyslogProducer() *SyslogProducer {
	config := NewSyslogProducerConfig()
	conf, err := producer.ProducerConfigFromFile(Config.ProducerProperties)
	useFile := true
	if err != nil {
		//we dont have a producer configuraiton which is ok
		useFile = false
	}

	if useFile {
		config.ProducerConfig = conf
	} else {
		config.ProducerConfig = producer.NewProducerConfig()
	}
	config.NumProducers = 1 //TODO configurable
	config.BrokerList = Config.BrokerList
	config.TCPAddr = fmt.Sprintf("0.0.0.0:%d", e.tcpPort)
	config.UDPAddr = fmt.Sprintf("0.0.0.0:%d", e.udpPort)
	config.Topic = Config.Topic
	config.Hostname = e.hostname
	config.Namespace = Config.Namespace
	if Config.Transform == TransformAvro {
		config.Transformer = avroTransformFunc
		config.ValueSerializer = avro.NewKafkaAvroEncoder(Config.SchemaRegistryUrl).Encode
	}

	return NewSyslogProducer(config)
}
