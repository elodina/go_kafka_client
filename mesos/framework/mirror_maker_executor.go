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
	"github.com/mesos/mesos-go/executor"
	mesos "github.com/mesos/mesos-go/mesosproto"
	kafka "github.com/stealthly/go_kafka_client"
	"os"
	"strings"
)

type MirrorMakerExecutor struct {
	config      TaskConfig
	mirrorMaker *kafka.MirrorMaker
}

func NewMirrorMakerExecutor() *MirrorMakerExecutor {
	return &MirrorMakerExecutor{
		config: make(TaskConfig),
	}
}

func (e *MirrorMakerExecutor) Registered(driver executor.ExecutorDriver, executor *mesos.ExecutorInfo, framework *mesos.FrameworkInfo, slave *mesos.SlaveInfo) {
	Logger.Infof("[Registered] framework: %s slave: %s", framework.GetId().GetValue(), slave.GetId().GetValue())
}

func (e *MirrorMakerExecutor) Reregistered(driver executor.ExecutorDriver, slave *mesos.SlaveInfo) {
	Logger.Infof("[Reregistered] slave: %s", slave.GetId().GetValue())
}

func (e *MirrorMakerExecutor) Disconnected(executor.ExecutorDriver) {
	Logger.Info("[Disconnected]")
}

func (e *MirrorMakerExecutor) LaunchTask(driver executor.ExecutorDriver, task *mesos.TaskInfo) {
	Logger.Infof("[LaunchTask] %s", task)

	err := json.Unmarshal(task.GetData(), &e.config)
	if err != nil {
		Logger.Errorf("Could not unmarshal json data: %s", err)
		panic(err)
	}

	Logger.Info(e.config)

	runStatus := &mesos.TaskStatus{
		TaskId: task.GetTaskId(),
		State:  mesos.TaskState_TASK_RUNNING.Enum(),
	}

	if _, err := driver.SendStatusUpdate(runStatus); err != nil {
		Logger.Errorf("Failed to send status update: %s", runStatus)
		os.Exit(1) //TODO not sure if we should exit in this case, but probably yes
	}

	go func() {
		e.startMirrorMaker()

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
	}()
}

func (e *MirrorMakerExecutor) KillTask(driver executor.ExecutorDriver, id *mesos.TaskID) {
	Logger.Infof("[KillTask] %s", id.GetValue())
	e.mirrorMaker.Stop()
}

func (e *MirrorMakerExecutor) FrameworkMessage(driver executor.ExecutorDriver, message string) {
	Logger.Infof("[FrameworkMessage] %s", message)
}

func (e *MirrorMakerExecutor) Shutdown(driver executor.ExecutorDriver) {
	Logger.Infof("[Shutdown]")
	e.mirrorMaker.Stop()
}

func (e *MirrorMakerExecutor) Error(driver executor.ExecutorDriver, message string) {
	Logger.Errorf("[Error] %s", message)
}

func (e *MirrorMakerExecutor) startMirrorMaker() {
	consumerConfigs := make([]string, 0)
	rawConsumerConfigs := strings.Split(e.config["consumer.config"], ",")
	for _, config := range rawConsumerConfigs {
		resourceTokens := strings.Split(config, "/")
		consumerConfigs = append(consumerConfigs, resourceTokens[len(resourceTokens)-1])
	}

	rawProducerConfig := strings.Split(e.config["producer.config"], "/")
	producerConfig := rawProducerConfig[len(rawProducerConfig)-1]

	mmConfig := kafka.NewMirrorMakerConfig()
	e.config.SetIntConfig("num.producers", &mmConfig.NumProducers)
	e.config.SetIntConfig("num.streams", &mmConfig.NumStreams)
	e.config.SetStringConfig("prefix", &mmConfig.TopicPrefix)
	e.config.SetIntConfig("queue.size", &mmConfig.ChannelSize)
	e.config.SetStringConfig("whitelist", &mmConfig.Whitelist)
	e.config.SetStringConfig("blacklist", &mmConfig.Blacklist)
	mmConfig.ProducerConfig = producerConfig
	mmConfig.ConsumerConfigs = consumerConfigs

	e.mirrorMaker = kafka.NewMirrorMaker(mmConfig)
	e.mirrorMaker.Start()
}
