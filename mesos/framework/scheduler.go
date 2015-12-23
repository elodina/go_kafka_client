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
	"os"
	"os/signal"
	"strings"

	"time"

	utils "github.com/elodina/go-mesos-utils"
	"github.com/elodina/go-mesos-utils/pretty"
	"github.com/golang/protobuf/proto"
	mesos "github.com/mesos/mesos-go/mesosproto"
	util "github.com/mesos/mesos-go/mesosutil"
	"github.com/mesos/mesos-go/scheduler"
)

const (
	reconcileDelay    = 10 * time.Second
	reconcileMaxTries = 3
)

var sched *Scheduler // This is needed for HTTP server to be able to update this scheduler

type Scheduler struct {
	httpServer      *HttpServer
	cluster         *Cluster
	driver          scheduler.SchedulerDriver
	schedulerDriver *scheduler.MesosSchedulerDriver

	reconcileTime time.Time
	reconciles    int
}

func NewScheduler() *Scheduler {
	return &Scheduler{
		reconcileTime: time.Unix(0, 0),
	}
}

func (s *Scheduler) Start() error {
	Logger.Infof("Starting scheduler with configuration: \n%s", Config)
	sched = s // set this scheduler reachable for http server

	ctrlc := make(chan os.Signal, 1)
	signal.Notify(ctrlc, os.Interrupt)

	s.cluster = NewCluster()
	s.cluster.Load()

	s.httpServer = NewHttpServer(Config.Api)
	go s.httpServer.Start()

	frameworkInfo := &mesos.FrameworkInfo{
		User:            proto.String(Config.User),
		Name:            proto.String(Config.FrameworkName),
		Role:            proto.String(Config.FrameworkRole),
		FailoverTimeout: proto.Float64(float64(Config.FrameworkTimeout / 1e9)),
		Checkpoint:      proto.Bool(true),
	}

	if s.cluster.frameworkID != "" {
		frameworkInfo.Id = util.NewFrameworkID(s.cluster.frameworkID)
	}

	driverConfig := scheduler.DriverConfig{
		Scheduler: s,
		Framework: frameworkInfo,
		Master:    Config.Master,
	}

	driver, err := scheduler.NewMesosSchedulerDriver(driverConfig)
	s.schedulerDriver = driver

	if err != nil {
		return fmt.Errorf("Unable to create SchedulerDriver: %s", err)
	}

	go func() {
		if stat, err := driver.Run(); err != nil {
			Logger.Infof("Framework stopped with status %s and error: %s\n", stat.String(), err)
			panic(err)
		}
	}()

	<-ctrlc
	return nil
}

func (s *Scheduler) Registered(driver scheduler.SchedulerDriver, id *mesos.FrameworkID, master *mesos.MasterInfo) {
	Logger.Infof("[Registered] framework: %s master: %s:%d", id.GetValue(), master.GetHostname(), master.GetPort())

	s.cluster.frameworkID = id.GetValue()
	s.cluster.Save()

	s.driver = driver
	s.reconcileTasks(true)
}

func (s *Scheduler) Reregistered(driver scheduler.SchedulerDriver, master *mesos.MasterInfo) {
	Logger.Infof("[Reregistered] master: %s:%d", master.GetHostname(), master.GetPort())

	s.driver = driver
	s.reconcileTasks(true)
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
			driver.DeclineOffer(offer.GetId(), &mesos.Filters{RefuseSeconds: proto.Float64(10)})
			Logger.Debugf("Declined offer: %s", declineReason)
		}
	}

	s.reconcileTasks(false)
	s.cluster.Save()
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

	s.cluster.Save()
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

	if s.schedulerDriver.Status() == mesos.Status_DRIVER_ABORTED {
		Logger.Errorf("Driver aborted, exiting...")
		time.Sleep(1 * time.Second) // sometimes logs do not flush so give them some time
		os.Exit(1)
	}
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
		declineReason := utils.CheckConstraints(offer, task.Data().constraints, s.cluster.GetConstrained())
		if declineReason == "" {
			declineReason = task.Matches(offer)
			if declineReason == "" {
				s.launchTask(task, offer)
				return ""
			} else {
				declineReasons = append(declineReasons, declineReason)
			}
		} else {
			declineReasons = append(declineReasons, declineReason)
		}
	}

	return strings.Join(declineReasons, ", ")
}

func (s *Scheduler) launchTask(task Task, offer *mesos.Offer) {
	taskInfo := task.NewTaskInfo(offer)
	task.Data().State = TaskStateStaging
	task.Data().Attributes = utils.OfferAttributes(offer)
	task.Data().ExecutorID = taskInfo.GetExecutor().GetExecutorId().GetValue()
	task.Data().SlaveID = taskInfo.GetSlaveId().GetValue()
	task.Data().TaskID = taskInfo.GetTaskId().GetValue()

	s.driver.LaunchTasks([]*mesos.OfferID{offer.GetId()}, []*mesos.TaskInfo{taskInfo}, &mesos.Filters{RefuseSeconds: proto.Float64(10)})
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
	task.Data().ResetTaskInfo()
}

func (s *Scheduler) idFromTaskId(taskId string) string {
	tokens := strings.Split(taskId, "-")
	id := tokens[1]
	Logger.Debugf("ID extracted from %s is %s", taskId, id)
	return id
}

func (s *Scheduler) reconcileTasks(force bool) {
	if time.Now().Sub(s.reconcileTime) >= reconcileDelay {
		if !s.cluster.IsReconciling() {
			s.reconciles = 0
		}
		s.reconciles++
		s.reconcileTime = time.Now()

		if s.reconciles > reconcileMaxTries {
			for _, task := range s.cluster.GetTasksWithState(TaskStateReconciling) {
				if task.Data().TaskID != "" {
					Logger.Infof("Reconciling exceeded %d tries for task %s, sending killTask for task %s", reconcileMaxTries, task.Data().ID, task.Data().TaskID)
					s.driver.KillTask(util.NewTaskID(task.Data().TaskID))

					task.Data().ResetTaskInfo()
				}
			}
		} else {
			if force {
				s.driver.ReconcileTasks(nil)
			} else {
				statuses := make([]*mesos.TaskStatus, 0)
				for _, task := range s.cluster.GetAllTasks() {
					if task.Data().TaskID != "" {
						task.Data().State = TaskStateReconciling
						Logger.Infof("Reconciling %d/%d task state for id %s, task id %s", s.reconciles, reconcileMaxTries, task.Data().ID, task.Data().TaskID)
						statuses = append(statuses, util.NewTaskStatus(util.NewTaskID(task.Data().TaskID), mesos.TaskState_TASK_STAGING))
					}
				}
				s.driver.ReconcileTasks(statuses)
			}
		}
	}
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
