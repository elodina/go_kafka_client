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
	"github.com/golang/protobuf/proto"
	mesos "github.com/mesos/mesos-go/mesosproto"
	"github.com/mesos/mesos-go/scheduler"
)

// SchedulerConfig defines configuration options for GoKafkaClientScheduler
type SchedulerConfig struct {
	// Number of CPUs allocated for each created Mesos task.
	CpuPerTask float64

	// Number of RAM allocated for each created Mesos task.
	MemPerTask float64

	// A WhiteList or BlackList of topics to consume.
	Filter TopicFilter

	// Zookeeper hosts for consumers to use.
	Zookeeper []string

	// Consumer group id for all consumers started with the given scheduler.
	GroupId string

	// Artifact server host name. Will be used to fetch the executor.
	ArtifactServerHost string

	// Artifact server port.Will be used to fetch the executor.
	ArtifactServerPort int

	// Name of the executor archive file.
	ExecutorArchiveName string

	// Name of the executor binary file contained in the executor archive.
	ExecutorBinaryName string

	// Maximum retries to kill a task.
	KillTaskRetries int

	// Flag to use static partitioning strategy.
	Static bool

	// Number of consumers to use for load balancing strategy.
	NumConsumers int

	// Log level for the scheduler and all executors to use.
	LogLevel string
}

// Creates a SchedulerConfig with some basic configurations set.
func NewSchedulerConfig() *SchedulerConfig {
	return &SchedulerConfig{
		CpuPerTask:      0.2,
		MemPerTask:      256,
		KillTaskRetries: 3,
	}
}

// The Mesos Scheduler implementation for Go Kafka Client.
type GoKafkaClientScheduler struct {
	// Configuration options for the scheduler.
	Config *SchedulerConfig

	// Tracker keeps track of alive executors, handles lost/finished/killed etc. tasks and decides whether to run new tasks on offered resources.
	Tracker ConsumerTracker
}

// Creates a new GoKafkaClientScheduler with a given config and tracker.
func NewGoKafkaClientScheduler(config *SchedulerConfig, tracker ConsumerTracker) *GoKafkaClientScheduler {
	return &GoKafkaClientScheduler{
		Config:  config,
		Tracker: tracker,
	}
}

// Returns a string represntation of GoKafkaClientScheduler.
func (this *GoKafkaClientScheduler) String() string {
	return "Go Kafka Client Scheduler"
}

// mesos.Scheduler interface method.
// Invoked when the scheduler successfully registers with a Mesos master.
// Not used by GoKafkaClientScheduler yet.
func (this *GoKafkaClientScheduler) Registered(driver scheduler.SchedulerDriver, frameworkId *mesos.FrameworkID, masterInfo *mesos.MasterInfo) {
	Infof(this, "Framework Registered with Master %s", masterInfo)
}

// mesos.Scheduler interface method.
// Invoked when the scheduler re-registers with a newly elected Mesos master.
// Not used by GoKafkaClientScheduler yet.
func (this *GoKafkaClientScheduler) Reregistered(driver scheduler.SchedulerDriver, masterInfo *mesos.MasterInfo) {
	Infof(this, "Framework Re-Registered with Master %s", masterInfo)
}

// mesos.Scheduler interface method.
// Invoked when the scheduler becomes "disconnected" from the master.
// Not used by GoKafkaClientScheduler yet.
func (this *GoKafkaClientScheduler) Disconnected(scheduler.SchedulerDriver) {
	Info(this, "Disconnected")
}

// mesos.Scheduler interface method.
// Invoked when resources have been offered to this framework.
// Delegates offers to ConsumerTracker to decide whether new tasks should be launched.
func (this *GoKafkaClientScheduler) ResourceOffers(driver scheduler.SchedulerDriver, offers []*mesos.Offer) {
	Tracef(this, "Received offers: %s", offers)

	offersAndTasks := this.Tracker.CreateTasks(offers)
	for _, offer := range offers {
		tasks := offersAndTasks[offer]
		driver.LaunchTasks([]*mesos.OfferID{offer.Id}, tasks, &mesos.Filters{RefuseSeconds: proto.Float64(1)})
	}
}

// mesos.Scheduler interface method.
// Invoked when the status of a task has changed.
// Informs the ConsumerTracker of this update if the task has died.
func (this *GoKafkaClientScheduler) StatusUpdate(driver scheduler.SchedulerDriver, status *mesos.TaskStatus) {
	Infof(this, "Status update: task %s is in state %s", status.TaskId.GetValue(), status.State.Enum().String())

	if status.GetState() == mesos.TaskState_TASK_LOST || status.GetState() == mesos.TaskState_TASK_FAILED || status.GetState() == mesos.TaskState_TASK_FINISHED {
		this.Tracker.TaskDied(status.GetTaskId())
	}
}

// mesos.Scheduler interface method.
// Invoked when an offer is no longer valid.
// Not used by GoKafkaClientScheduler yet.
func (this *GoKafkaClientScheduler) OfferRescinded(scheduler.SchedulerDriver, *mesos.OfferID) {}

// mesos.Scheduler interface method.
// Invoked when an executor sends a message.
// Not used by GoKafkaClientScheduler yet.
func (this *GoKafkaClientScheduler) FrameworkMessage(scheduler.SchedulerDriver, *mesos.ExecutorID, *mesos.SlaveID, string) {
}

//TODO probably will have to implement these 2 methods as well to handle outages

// mesos.Scheduler interface method.
// Invoked when a slave has been determined unreachable
// Not used by GoKafkaClientScheduler yet.
func (this *GoKafkaClientScheduler) SlaveLost(scheduler.SchedulerDriver, *mesos.SlaveID) {}

// mesos.Scheduler interface method.
// Invoked when an executor has exited/terminated.
// Not used by GoKafkaClientScheduler yet.
func (this *GoKafkaClientScheduler) ExecutorLost(scheduler.SchedulerDriver, *mesos.ExecutorID, *mesos.SlaveID, int) {
}

// mesos.Scheduler interface method.
// Invoked when there is an unrecoverable error in the scheduler or scheduler driver.
// Not used by GoKafkaClientScheduler yet.
func (this *GoKafkaClientScheduler) Error(driver scheduler.SchedulerDriver, err string) {
	Errorf(this, "Scheduler received error: %s", err)
}

// Gracefully shuts down all running tasks.
func (this *GoKafkaClientScheduler) Shutdown(driver scheduler.SchedulerDriver) {
	Debug(this, "Shutting down scheduler.")
	for _, taskId := range this.Tracker.GetAllTasks() {
		if err := this.tryKillTask(driver, taskId); err != nil {
			Errorf(this, "Failed to kill task %s", taskId.GetValue())
		}
	}
}

func (this *GoKafkaClientScheduler) tryKillTask(driver scheduler.SchedulerDriver, taskId *mesos.TaskID) error {
	Debugf(this, "Trying to kill task %s", taskId.GetValue())

	var err error
	for i := 0; i <= this.Config.KillTaskRetries; i++ {
		if _, err = driver.KillTask(taskId); err == nil {
			return nil
		}
	}
	return err
}
