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
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"strings"

	"github.com/elodina/go-mesos-utils/pretty"
	"github.com/golang/protobuf/proto"
	mesos "github.com/mesos/mesos-go/mesosproto"
	util "github.com/mesos/mesos-go/mesosutil"
	"github.com/mesos/mesos-go/scheduler"
)

var sched *Scheduler // This is needed for HTTP server to be able to update this scheduler

type Scheduler struct {
	httpServer *HttpServer
	cluster    *Cluster
	driver     scheduler.SchedulerDriver
}

func (s *Scheduler) Start() error {
	Logger.Infof("Starting scheduler with configuration: \n%s", Config)
	sched = s // set this scheduler reachable for http server

	ctrlc := make(chan os.Signal, 1)
	signal.Notify(ctrlc, os.Interrupt)

	if err := s.resolveDeps(); err != nil {
		return err
	}

	s.httpServer = NewHttpServer(Config.Api)
	go s.httpServer.Start()

	s.cluster = NewCluster()

	frameworkInfo := &mesos.FrameworkInfo{
		User:       proto.String(Config.User),
		Name:       proto.String(Config.FrameworkName),
		Role:       proto.String(Config.FrameworkRole),
		Checkpoint: proto.Bool(true),
	}

	driverConfig := scheduler.DriverConfig{
		Scheduler: s,
		Framework: frameworkInfo,
		Master:    Config.Master,
	}

	driver, err := scheduler.NewMesosSchedulerDriver(driverConfig)
	go func() {
		<-ctrlc
		s.Shutdown(driver)
	}()

	if err != nil {
		return fmt.Errorf("Unable to create SchedulerDriver: %s", err)
	}

	if stat, err := driver.Run(); err != nil {
		Logger.Infof("Framework stopped with status %s and error: %s\n", stat.String(), err)
		return err
	}

	//TODO stop http server

	return nil
}

func (s *Scheduler) Registered(driver scheduler.SchedulerDriver, id *mesos.FrameworkID, master *mesos.MasterInfo) {
	Logger.Infof("[Registered] framework: %s master: %s:%d", id.GetValue(), master.GetHostname(), master.GetPort())

	s.driver = driver
}

func (s *Scheduler) Reregistered(driver scheduler.SchedulerDriver, master *mesos.MasterInfo) {
	Logger.Infof("[Reregistered] master: %s:%d", master.GetHostname(), master.GetPort())

	s.driver = driver
}

func (s *Scheduler) Disconnected(scheduler.SchedulerDriver) {
	Logger.Info("[Disconnected]")

	s.driver = nil
}

func (s *Scheduler) ResourceOffers(driver scheduler.SchedulerDriver, offers []*mesos.Offer) {
	Logger.Debugf("[ResourceOffers] %s", pretty.Offers(offers))

	for _, offer := range offers {
		declineReason := s.acceptOffer(driver, offer)
		if declineReason != "" {
			driver.DeclineOffer(offer.GetId(), &mesos.Filters{RefuseSeconds: proto.Float64(1)})
			Logger.Debugf("Declined offer: %s", declineReason)
		}
	}
}

func (s *Scheduler) OfferRescinded(driver scheduler.SchedulerDriver, id *mesos.OfferID) {
	Logger.Infof("[OfferRescinded] %s", id.GetValue())
}

func (s *Scheduler) StatusUpdate(driver scheduler.SchedulerDriver, status *mesos.TaskStatus) {
	Logger.Infof("[StatusUpdate] %s", pretty.Status(status))

	id := s.idFromTaskId(status.GetTaskId().GetValue())

	switch status.GetState() {
	case mesos.TaskState_TASK_RUNNING:
		s.onTaskStarted(id, status)
	case mesos.TaskState_TASK_LOST, mesos.TaskState_TASK_FAILED, mesos.TaskState_TASK_ERROR:
		s.onTaskFailed(id, status)
	case mesos.TaskState_TASK_FINISHED, mesos.TaskState_TASK_KILLED:
		s.onTaskFinished(id, status)
	default:
		Logger.Warnf("Got unexpected task state %s for task %s", pretty.Status(status), id)
	}
}

func (s *Scheduler) FrameworkMessage(driver scheduler.SchedulerDriver, executor *mesos.ExecutorID, slave *mesos.SlaveID, message string) {
	Logger.Infof("[FrameworkMessage] executor: %s slave: %s message: %s", executor, slave, message)
}

func (s *Scheduler) SlaveLost(driver scheduler.SchedulerDriver, slave *mesos.SlaveID) {
	Logger.Infof("[SlaveLost] %s", slave.GetValue())
}

func (s *Scheduler) ExecutorLost(driver scheduler.SchedulerDriver, executor *mesos.ExecutorID, slave *mesos.SlaveID, status int) {
	Logger.Infof("[ExecutorLost] executor: %s slave: %s status: %d", executor, slave, status)
}

func (s *Scheduler) Error(driver scheduler.SchedulerDriver, message string) {
	Logger.Errorf("[Error] %s", message)
}

func (s *Scheduler) Shutdown(driver *scheduler.MesosSchedulerDriver) {
	Logger.Info("Shutdown triggered, stopping driver")
	driver.Stop(false)
}

func (s *Scheduler) acceptOffer(driver scheduler.SchedulerDriver, offer *mesos.Offer) string {
	declineReasons := make([]string, 0)

	tasks := s.cluster.GetTasksWithState(TaskStateStopped)
	if len(tasks) == 0 {
		return "all tasks are running"
	}

	for _, task := range tasks {
		declineReason := task.Matches(offer)
		if declineReason == "" {
			s.launchTask(task, offer)
			return ""
		} else {
			declineReasons = append(declineReasons, declineReason)
		}
	}

	return strings.Join(declineReasons, ", ")
}

func (s *Scheduler) launchTask(task Task, offer *mesos.Offer) {
	taskInfo := task.NewTaskInfo(offer)
	task.Data().State = TaskStateStaging

	s.driver.LaunchTasks([]*mesos.OfferID{offer.GetId()}, []*mesos.TaskInfo{taskInfo}, &mesos.Filters{RefuseSeconds: proto.Float64(1)})
}

func (s *Scheduler) onTaskStarted(id string, status *mesos.TaskStatus) {
	if s.cluster.Exists(id) {
		task := s.cluster.Get(id)
		task.Data().State = TaskStateRunning
	} else {
		Logger.Infof("Got %s for unknown/stopped task, killing task %s", pretty.Status(status), status.GetTaskId().GetValue())
	}
}

func (s *Scheduler) onTaskFailed(id string, status *mesos.TaskStatus) {
	if s.cluster.Exists(id) {
		task := s.cluster.Get(id)
		if task.Data().State != TaskStateInactive {
			task.Data().State = TaskStateStopped
		}
	} else {
		Logger.Infof("Got %s for unknown/stopped task %s", pretty.Status(status), status.GetTaskId().GetValue())
	}
}

func (s *Scheduler) onTaskFinished(id string, status *mesos.TaskStatus) {
	if !s.cluster.Exists(id) {
		Logger.Infof("Got %s for unknown/stopped task %s", pretty.Status(status), status.GetTaskId().GetValue())
	}
}

func (s *Scheduler) stopTask(task Task) {
	if task.Data().State == TaskStateRunning || task.Data().State == TaskStateStaging {
		Logger.Infof("Stopping task %s", task.Data().TaskID)
		s.driver.KillTask(util.NewTaskID(task.Data().TaskID))
	}

	task.Data().State = TaskStateInactive
}

func (s *Scheduler) idFromTaskId(taskId string) string {
	tokens := strings.Split(taskId, "-")
	id := tokens[1]
	Logger.Debugf("ID extracted from %s is %s", taskId, id)
	return id
}

func (s *Scheduler) resolveDeps() error {
	files, _ := ioutil.ReadDir("./")
	for _, file := range files {
		if !file.IsDir() && executorMask.MatchString(file.Name()) {
			Config.Executor = file.Name()
		}
	}

	if Config.Executor == "" {
		return fmt.Errorf("%s not found in current dir", executorMask)
	}

	return nil
}

func getScalarResources(offer *mesos.Offer, resourceName string) float64 {
	resources := 0.0
	filteredResources := util.FilterResources(offer.Resources, func(res *mesos.Resource) bool {
		return res.GetName() == resourceName
	})
	for _, res := range filteredResources {
		resources += res.GetScalar().GetValue()
	}
	return resources
}
