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

package go_kafka_client

import (
	"fmt"
	"github.com/mesos/mesos-go/executor"
	mesos "github.com/mesos/mesos-go/mesosproto"
)

// ExecutorConfig defines configuration options for GoKafkaClientExecutor
type ExecutorConfig struct {
	// Topic to consume for this executor. Only needed for static partition configuration and will be used only when Filter is not set.
	Topic string

	// Partition to consume for this executor. Only needed for static partition configuration and will be used only when Filter is not set.
	Partition int32

	// TopicFilter to consume for this executor. Only needed for load balancing configuration. Ignores Topic and Partition configurations if set.
	Filter TopicFilter

	// Slice of Zookeeper connection strings
	Zookeeper []string

	// Consumer group id to start the executor in.
	Group string
}

// Creates an empty ExecutorConfig.
func NewExecutorConfig() *ExecutorConfig {
	return &ExecutorConfig{}
}

// The Mesos Executor implementation for Go Kafka Client.
type GoKafkaClientExecutor struct {
	// Configuration options for the executor.
	Config    *ExecutorConfig
	consumers map[string]*Consumer
}

// Creates a new GoKafkaClientExecutor with a given config.
func NewGoKafkaClientExecutor(config *ExecutorConfig) *GoKafkaClientExecutor {
	return &GoKafkaClientExecutor{
		Config:    config,
		consumers: make(map[string]*Consumer),
	}
}

// Returns a string represntation of GoKafkaClientExecutor.
func (this *GoKafkaClientExecutor) String() string {
	return fmt.Sprintf("Go Kafka Client Executor %s-%d", this.Config.Topic, this.Config.Partition)
}

// mesos.Executor interface method.
// Invoked once the executor driver has been able to successfully connect with Mesos.
// Not used by GoKafkaClientExecutor yet.
func (this *GoKafkaClientExecutor) Registered(driver executor.ExecutorDriver, execInfo *mesos.ExecutorInfo, fwinfo *mesos.FrameworkInfo, slaveInfo *mesos.SlaveInfo) {
	Infof(this, "Registered Executor on slave %s", slaveInfo.GetHostname())
}

// mesos.Executor interface method.
// Invoked when the executor re-registers with a restarted slave.
// Not used by GoKafkaClientExecutor yet.
func (this *GoKafkaClientExecutor) Reregistered(driver executor.ExecutorDriver, slaveInfo *mesos.SlaveInfo) {
	Infof(this, "Re-registered Executor on slave %s", slaveInfo.GetHostname())
}

// mesos.Executor interface method.
// Invoked when the executor becomes "disconnected" from the slave.
// Not used by GoKafkaClientExecutor yet.
func (this *GoKafkaClientExecutor) Disconnected(executor.ExecutorDriver) {
	Info(this, "Executor disconnected.")
}

// mesos.Executor interface method.
// Invoked when a task has been launched on this executor.
// Starts a new consumer with given executor configurations.
func (this *GoKafkaClientExecutor) LaunchTask(driver executor.ExecutorDriver, taskInfo *mesos.TaskInfo) {
	Infof(this, "Launching task %s with command %s", taskInfo.GetName(), taskInfo.Command.GetValue())

	runStatus := &mesos.TaskStatus{
		TaskId: taskInfo.GetTaskId(),
		State:  mesos.TaskState_TASK_RUNNING.Enum(),
	}

	if _, err := driver.SendStatusUpdate(runStatus); err != nil {
		Errorf(this, "Failed to send status update: %s", runStatus)
	}

	taskId := taskInfo.GetTaskId().GetValue()

	consumer := this.createNewConsumer()
	if oldConsumer, exists := this.consumers[taskId]; exists {
		<-oldConsumer.Close()
	}
	this.consumers[taskId] = consumer
	go func() {
		if this.Config.Filter != nil {
			consumer.StartWildcard(this.Config.Filter, 1)
		} else {
			consumer.StartStaticPartitions(map[string][]int32{this.Config.Topic: []int32{this.Config.Partition}})
		}

		// finish task
		Debugf(this, "Finishing task %s", taskInfo.GetName())
		finStatus := &mesos.TaskStatus{
			TaskId: taskInfo.GetTaskId(),
			State:  mesos.TaskState_TASK_FINISHED.Enum(),
		}
		if _, err := driver.SendStatusUpdate(finStatus); err != nil {
			Errorf(this, "Failed to send status update: %s", finStatus)
		}
		Infof(this, "Task %s has finished", taskInfo.GetName())
	}()
}

// mesos.Executor interface method.
// Invoked when a task running within this executor has been killed.
// Stops the running Kafka consumer associasted with the given TaskID.
func (this *GoKafkaClientExecutor) KillTask(_ executor.ExecutorDriver, taskId *mesos.TaskID) {
	Info(this, "Kill task")

	this.closeConsumer(taskId.GetValue())
}

// mesos.Executor interface method.
// Invoked when a framework message has arrived for this executor.
// Not used by GoKafkaClientExecutor yet.
func (this *GoKafkaClientExecutor) FrameworkMessage(driver executor.ExecutorDriver, msg string) {
	Infof(this, "Got framework message: %s", msg)
}

// mesos.Executor interface method.
// Invoked when the executor should terminate all of its currently running tasks.
// Stops all running Kafka consumers associated with this executor.
func (this *GoKafkaClientExecutor) Shutdown(executor.ExecutorDriver) {
	Info(this, "Shutting down the executor")

	for taskId := range this.consumers {
		this.closeConsumer(taskId)
	}
}

// mesos.Executor interface method.
// Invoked when a fatal error has occured with the executor and/or executor driver.
// Not used by GoKafkaClientExecutor yet.
func (this *GoKafkaClientExecutor) Error(driver executor.ExecutorDriver, err string) {
	Errorf(this, "Got error message: %s", err)
}

func (this *GoKafkaClientExecutor) closeConsumer(taskId string) {
	consumer, exists := this.consumers[taskId]
	if !exists {
		Warnf(this, "Got KillTask for unknown TaskID: %s", taskId)
		return
	}

	Debugf(this, "Closing consumer for TaskID %s", taskId)
	delete(this.consumers, taskId)
	<-consumer.Close()
	Debugf(this, "Closed consumer for TaskID %s", taskId)
}

func (this *GoKafkaClientExecutor) createNewConsumer() *Consumer {
	config := DefaultConsumerConfig()
	SetupConsumerConfig(config)
	config.Groupid = this.Config.Group

	zkConfig := NewZookeeperConfig()
	SetupZookeeperConfig(zkConfig)
	zkConfig.ZookeeperConnect = this.Config.Zookeeper

	config.Coordinator = NewZookeeperCoordinator(zkConfig)

	return NewConsumer(config)
}
