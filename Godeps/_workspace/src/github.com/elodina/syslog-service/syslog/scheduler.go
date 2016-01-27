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
	crand "crypto/rand"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"strings"
	"sync"

	"github.com/elodina/go-mesos-utils"
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
	active     bool
	activeLock sync.Mutex
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

	listenAddr := s.listenAddr()
	s.httpServer = NewHttpServer(listenAddr)
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

func (s *Scheduler) SetActive(active bool) {
	s.activeLock.Lock()
	defer s.activeLock.Unlock()

	s.active = active
	if !s.active {
		for _, task := range s.cluster.GetAllTasks() {
			Logger.Debugf("Killing task %s", task.GetTaskId().GetValue())
			s.driver.KillTask(task.GetTaskId())
		}
	}
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

	s.activeLock.Lock()
	defer s.activeLock.Unlock()

	if !s.active {
		Logger.Debug("Scheduler is inactive. Declining all offers.")
		for _, offer := range offers {
			driver.DeclineOffer(offer.GetId(), &mesos.Filters{RefuseSeconds: proto.Float64(1)})
		}
		return
	}

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

	slave := s.slaveFromTaskId(status.GetTaskId().GetValue())

	if status.GetState() == mesos.TaskState_TASK_FAILED || status.GetState() == mesos.TaskState_TASK_KILLED ||
		status.GetState() == mesos.TaskState_TASK_LOST || status.GetState() == mesos.TaskState_TASK_ERROR ||
		status.GetState() == mesos.TaskState_TASK_FINISHED {
		s.cluster.Remove(slave)
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
	if s.cluster.Exists(offer.GetSlaveId().GetValue()) {
		return fmt.Sprintf("Server on slave %s is already running.", offer.GetSlaveId().GetValue())
	} else {
		declineReason := s.match(offer)
		if declineReason == "" {
			s.launchTask(driver, offer)
		}
		return declineReason
	}
}

func (s *Scheduler) match(offer *mesos.Offer) string {
	if Config.Cpus > getScalarResources(offer, "cpus") {
		return "no cpus"
	}

	if Config.Mem > getScalarResources(offer, "mem") {
		return "no mem"
	}

	tcpPort := s.getPort(Config.TcpPort, offer, -1)
	if tcpPort == -1 {
		return "no suitable port"
	}

	if s.getPort(Config.UdpPort, offer, tcpPort) == -1 {
		return "no suitable port"
	}

	return ""
}

func (s *Scheduler) getPort(targetPort string, offer *mesos.Offer, excludePort int) int {
	ports := getRangeResources(offer, "ports")
	portRanges := make([]*utils.Range, len(ports))
	for idx, rng := range ports {
		portRanges[idx] = utils.NewRange(int(rng.GetBegin()), int(rng.GetEnd()))
	}

	if len(portRanges) == 0 {
		return -1
	}

	if targetPort == "auto" {
		for _, rng := range portRanges {
			for _, rngValue := range rng.Values() {
				if rngValue != excludePort {
					return rngValue
				}
			}
		}
		return -1
	} else {
		rng, err := utils.ParseRange(targetPort)
		if err != nil {
			Logger.Warn(err)
			return -1
		}

		for _, offerRng := range portRanges {
			overlappingPorts := offerRng.Overlap(rng)
			if overlappingPorts != nil {
				for _, rngValue := range rng.Values() {
					if rngValue != excludePort {
						return rngValue
					}
				}
			}
		}

		return -1
	}
}

func (s *Scheduler) launchTask(driver scheduler.SchedulerDriver, offer *mesos.Offer) {
	taskName := fmt.Sprintf("syslog-%s", offer.GetSlaveId().GetValue())
	taskId := &mesos.TaskID{
		Value: proto.String(fmt.Sprintf("%s-%s", taskName, uuid())),
	}

	data, err := json.Marshal(Config)
	if err != nil {
		panic(err) //shouldn't happen
	}
	Logger.Debugf("Task data: %s", string(data))

	tcpPort := uint64(s.getPort(Config.TcpPort, offer, -1))
	udpPort := uint64(s.getPort(Config.UdpPort, offer, int(tcpPort)))

	task := &mesos.TaskInfo{
		Name:     proto.String(taskName),
		TaskId:   taskId,
		SlaveId:  offer.GetSlaveId(),
		Executor: s.createExecutor(offer, tcpPort, udpPort),
		Resources: []*mesos.Resource{
			util.NewScalarResource("cpus", Config.Cpus),
			util.NewScalarResource("mem", Config.Mem),
			util.NewRangesResource("ports", []*mesos.Value_Range{util.NewValueRange(tcpPort, tcpPort)}),
			util.NewRangesResource("ports", []*mesos.Value_Range{util.NewValueRange(udpPort, udpPort)}),
		},
		Data: data,
	}

	s.cluster.Add(offer.GetSlaveId().GetValue(), task)

	driver.LaunchTasks([]*mesos.OfferID{offer.GetId()}, []*mesos.TaskInfo{task}, &mesos.Filters{RefuseSeconds: proto.Float64(1)})
}

func (s *Scheduler) createExecutor(offer *mesos.Offer, tcpPort uint64, udpPort uint64) *mesos.ExecutorInfo {
	name := fmt.Sprintf("syslog-%s", offer.GetSlaveId().GetValue())
	id := fmt.Sprintf("%s-%s", name, uuid())

	uris := []*mesos.CommandInfo_URI{
		&mesos.CommandInfo_URI{
			Value:      proto.String(fmt.Sprintf("%s/resource/%s", Config.Api, Config.Executor)),
			Executable: proto.Bool(true),
		},
	}

	if Config.ProducerProperties != "" {
		uris = append(uris, &mesos.CommandInfo_URI{
			Value: proto.String(fmt.Sprintf("%s/resource/%s", Config.Api, Config.ProducerProperties)),
		})
	}

	return &mesos.ExecutorInfo{
		ExecutorId: util.NewExecutorID(id),
		Name:       proto.String(name),
		Command: &mesos.CommandInfo{
			Value: proto.String(fmt.Sprintf("./%s --log.level %s --tcp %d --udp %d --host %s", Config.Executor, Config.LogLevel, tcpPort, udpPort, offer.GetHostname())),
			Uris:  uris,
		},
	}
}

func (s *Scheduler) slaveFromTaskId(taskId string) string {
	tokens := strings.SplitN(taskId, "-", 2)
	slave := tokens[len(tokens)-1]
	slave = slave[:len(slave)-37] //strip uuid part
	Logger.Debugf("Slave ID extracted from %s is %s", taskId, slave)
	return slave
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

func (s *Scheduler) listenAddr() string {
	address := Config.Api
	if strings.HasPrefix(address, "http://") {
		address = address[len("http://"):]
	}

	colonIndex := strings.LastIndex(address, ":")
	if colonIndex != -1 {
		address = "0.0.0.0" + address[colonIndex:]
	}

	return address
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

func getRangeResources(offer *mesos.Offer, resourceName string) []*mesos.Value_Range {
	resources := make([]*mesos.Value_Range, 0)
	filteredResources := util.FilterResources(offer.Resources, func(res *mesos.Resource) bool {
		return res.GetName() == resourceName
	})
	for _, res := range filteredResources {
		resources = append(resources, res.GetRanges().GetRange()...)
	}
	return resources
}

func uuid() string {
	b := make([]byte, 16)
	crand.Read(b)
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}
