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
	mesos "github.com/mesos/mesos-go/mesosproto"
	util "github.com/mesos/mesos-go/mesosutil"
	"github.com/golang/protobuf/proto"
	"fmt"
	"strings"
)

// ConsumerTracker is responsible of keeping track of running tasks and making decisions on whether it should launch new tasks using given resource offers.
type ConsumerTracker interface {
	// CreateTasks is called each time the scheduler receives a resource offer.
	// Must return a map where keys are resource offers and values are slices of tasks to launch for a corresponding offer.
	// If no tasks should be launched for an offer the key may be omitted at all or contain a nil value.
	CreateTasks([]*mesos.Offer) map[*mesos.Offer][]*mesos.TaskInfo

	// TaskDied is called each time the task is lost, failed or finished giving the ConsumerTracker a chance to launch it on next resource offer.
	TaskDied(*mesos.TaskID)

	// Returns all running tasks. For now used only to kill them all.
	GetAllTasks() []*mesos.TaskID
}

// StaticConsumerTracker is a ConsumerTracker implementation that creates tasks with static partition configuration.
// The number of tasks created is determined as number of total partitions to consume,
// e.g. if the whitelist matches 2 topics with 5 partitions each, 10 tasks will be created (assuming enough resources have been offered).
type StaticConsumerTracker struct {
	// Configuration options for the tracker.
	Config *SchedulerConfig
	zookeeper *ZookeeperCoordinator
	consumerMap map[string]map[int32]*mesos.TaskID
}

// Creates a new StaticConsumerTracker with a given scheduler config.
// As this ConsumerTracker implementation has to know the exact number of partitions matching by the topic filter,
// the ZookeeperCoordinator is used under the hood. Hence an error can be returned if the tracker fails to reach Zookeeper.
func NewStaticConsumerTracker(config *SchedulerConfig) (*StaticConsumerTracker, error) {
	zkConfig := NewZookeeperConfig()
	zkConfig.ZookeeperConnect = config.Zookeeper
	zookeeper := NewZookeeperCoordinator(zkConfig)
	if err := zookeeper.Connect(); err != nil {
		return nil, err
	}

	return &StaticConsumerTracker{
		Config: config,
		zookeeper: zookeeper,
		consumerMap: make(map[string]map[int32]*mesos.TaskID),
	}, nil
}

// Returns a string represntation of StaticConsumerTracker.
func (this *StaticConsumerTracker) String() string {
	return "StaticConsumerTracker"
}

// CreateTasks is called each time the scheduler receives a resource offer.
// Returns a map where keys are resource offers and values are slices of tasks to launch for a corresponding offer.
func (this *StaticConsumerTracker) CreateTasks(offers []*mesos.Offer) map[*mesos.Offer][]*mesos.TaskInfo {
	topicPartitions, err := this.getUnoccupiedTopicPartitions()
	if err != nil {
		Errorf(this, "Could not get topic-partitions to consume: %s", err)
		return nil
	}
	if len(topicPartitions) == 0 {
		Debugf(this, "There are no unoccupied topic-partitions, no need to start new consumers.")
		return nil
	}

	offersAndTasks := make(map[*mesos.Offer][]*mesos.TaskInfo)
	for _, offer := range offers {
		cpus := getScalarResources(offer, "cpus")
		mems := getScalarResources(offer, "mem")

		Debugf(this, "Received Offer <%s> with cpus=%f, mem=%f", offer.Id.GetValue(), cpus, mems)

		remainingCpus := cpus
		remainingMems := mems

		var tasks []*mesos.TaskInfo
		for len(topicPartitions) > 0 && this.Config.CpuPerTask <= remainingCpus && this.Config.MemPerTask <= remainingMems {
			topic, partition := this.takeTopicPartition(topicPartitions)
			taskId := &mesos.TaskID {
				Value: proto.String(fmt.Sprintf("%s-%d", topic, partition)),
			}

			task := &mesos.TaskInfo{
				Name:     proto.String(taskId.GetValue()),
				TaskId:   taskId,
				SlaveId:  offer.SlaveId,
				Executor: this.createExecutorForTopicPartition(topic, partition),
				Resources: []*mesos.Resource{
					util.NewScalarResource("cpus", float64(this.Config.CpuPerTask)),
					util.NewScalarResource("mem", float64(this.Config.MemPerTask)),
				},
			}
			Debugf(this, "Prepared task: %s with offer %s for launch", task.GetName(), offer.Id.GetValue())

			tasks = append(tasks, task)
			remainingCpus -= this.Config.CpuPerTask
			remainingMems -= this.Config.MemPerTask

			this.addConsumerForTopic(topic, partition, taskId)
		}
		Debugf(this, "Launching %d tasks for offer %s", len(tasks), offer.Id.GetValue())
		offersAndTasks[offer] = tasks
	}
	//if we still have unoccupied topic-partitions and lack resources - inform the user about this
	if len(topicPartitions) > 0 {
		partitionsCount := 0
		for _, partitions := range topicPartitions {
			partitionsCount += len(partitions)
		}
		Warnf(this, "There are still %d partitions unoccupied and no more resources are available. Maybe add some more computing power?", partitionsCount)
	}
	return offersAndTasks
}

// TaskDied is called each time the task is lost, failed or finished giving the StaticConsumerTracker a chance to launch it on next resource offer.
func (this *StaticConsumerTracker) TaskDied(id *mesos.TaskID) {
	for topic, partitions := range this.consumerMap {
		for partition, taskId := range partitions {
			if taskId.GetValue() == id.GetValue() {
				delete(this.consumerMap[topic], partition)
				return
			}
		}
	}

	Warn(this, "TaskDied called for not existing TaskID")
}

// Returns all running tasks. For now used only to kill them all.
func (this *StaticConsumerTracker) GetAllTasks() []*mesos.TaskID {
	ids := make([]*mesos.TaskID, 0)

	for _, partitions := range this.consumerMap {
		for _, taskId := range partitions {
			ids = append(ids, taskId)
		}
	}

	return ids
}

func (this *StaticConsumerTracker) getUnoccupiedTopicPartitions() (map[string][]int32, error) {
	topics, err := this.zookeeper.GetAllTopics()
	if err != nil {
		return nil, err
	}

	filteredTopics := make([]string, 0)
	for _, topic := range topics {
		if this.Config.Filter.TopicAllowed(topic, true) {
			filteredTopics = append(filteredTopics, topic)
		}
	}

	topicPartitions, err := this.zookeeper.GetPartitionsForTopics(filteredTopics)
	if err != nil {
		return nil, err
	}

	unoccupiedTopicPartitions := make(map[string][]int32)
	for topic, partitions := range topicPartitions {
		for _, partition := range partitions {
			if this.consumerMap[topic] == nil || this.consumerMap[topic][partition] == nil {
				unoccupiedTopicPartitions[topic] = append(unoccupiedTopicPartitions[topic], partition)
			}
		}
	}

	// copying can be removed after this is stable, but now we need this as the logging is async
	copied := make(map[string][]int32)
	for k, v := range unoccupiedTopicPartitions {
		copied[k] = v
	}
	Debugf(this, "Unoccupied topic-partitions: %s", copied)
	return unoccupiedTopicPartitions, nil
}

func (this *StaticConsumerTracker) takeTopicPartition(topicPartitions map[string][]int32) (string, int32) {
	for topic, partitions := range topicPartitions {
		topicPartitions[topic] = partitions[1:]

		if len(topicPartitions[topic]) == 0 {
			delete(topicPartitions, topic)
		}

		return topic, partitions[0]
	}

	panic("take on empty map")
}

func (this *StaticConsumerTracker) addConsumerForTopic(topic string, partition int32, id *mesos.TaskID) {
	consumersForTopic := this.consumerMap[topic]
	if consumersForTopic == nil {
		this.consumerMap[topic] = make(map[int32]*mesos.TaskID)
		consumersForTopic = this.consumerMap[topic]
	}
	consumersForTopic[partition] = id
}

func (this *StaticConsumerTracker) createExecutorForTopicPartition(topic string, partition int32) *mesos.ExecutorInfo {
	path := strings.Split(this.Config.ExecutorArchiveName, "/")
	return &mesos.ExecutorInfo{
		ExecutorId: util.NewExecutorID(fmt.Sprintf("kafka-%s-%d", topic, partition)),
		Name:       proto.String("Go Kafka Client Executor"),
		Source:     proto.String("go-kafka"),
		Command: &mesos.CommandInfo{
			Value: proto.String(fmt.Sprintf("./%s --zookeeper %s --group %s --topic %s --partition %d --log.level %s", this.Config.ExecutorBinaryName, strings.Join(this.Config.Zookeeper, ","), this.Config.GroupId, topic, partition, this.Config.LogLevel)),
			Uris:  []*mesos.CommandInfo_URI{&mesos.CommandInfo_URI{
				Value: proto.String(fmt.Sprintf("http://%s:%d/%s", this.Config.ArtifactServerHost, this.Config.ArtifactServerPort, path[len(path)-1])),
				Extract: proto.Bool(true),
			}},
		},
	}
}

// LoadBalancingConsumerTracker is a ConsumerTracker implementation that creates tasks with load balancing configuration.
// The number of tasks created is determined as NumConsumers parameter.
type LoadBalancingConsumerTracker struct {
	// Configuration options for the tracker.
	Config *SchedulerConfig

	// Target number of consumers to be running.
	NumConsumers int
	aliveConsumers int
	consumerMap map[int]*mesos.TaskID
}

// Creates a new LoadBalancingConsumerTracker with a given scheduler config and target number of consumers.
func NewLoadBalancingConsumerTracker(config *SchedulerConfig, numConsumers int) *LoadBalancingConsumerTracker {
	return &LoadBalancingConsumerTracker{
		Config: config,
		NumConsumers: numConsumers,
		consumerMap: make(map[int]*mesos.TaskID),
	}
}

// Returns a string represntation of LoadBalancingConsumerTracker.
func (this *LoadBalancingConsumerTracker) String() string {
	return "LoadBalancingConsumerTracker"
}

// CreateTasks is called each time the scheduler receives a resource offer.
// Returns a map where keys are resource offers and values are slices of tasks to launch for a corresponding offer.
func (this *LoadBalancingConsumerTracker) CreateTasks(offers []*mesos.Offer) map[*mesos.Offer][]*mesos.TaskInfo {
	offersAndTasks := make(map[*mesos.Offer][]*mesos.TaskInfo)
	for _, offer := range offers {
		cpus := getScalarResources(offer, "cpus")
		mems := getScalarResources(offer, "mem")

		Debugf(this, "Received Offer <%s> with cpus=%f, mem=%f", offer.Id.GetValue(), cpus, mems)

		remainingCpus := cpus
		remainingMems := mems

		var tasks []*mesos.TaskInfo
		id := this.getFreeId()
		for id > -1 && this.Config.CpuPerTask <= remainingCpus && this.Config.MemPerTask <= remainingMems {
			taskId := &mesos.TaskID {
				Value: proto.String(fmt.Sprintf("go-kafka-%d", id)),
			}

			task := &mesos.TaskInfo{
				Name:     proto.String(taskId.GetValue()),
				TaskId:   taskId,
				SlaveId:  offer.SlaveId,
				Executor: this.createExecutor(id),
				Resources: []*mesos.Resource{
					util.NewScalarResource("cpus", float64(this.Config.CpuPerTask)),
					util.NewScalarResource("mem", float64(this.Config.MemPerTask)),
				},
			}
			Debugf(this, "Prepared task: %s with offer %s for launch", task.GetName(), offer.Id.GetValue())

			tasks = append(tasks, task)
			remainingCpus -= this.Config.CpuPerTask
			remainingMems -= this.Config.MemPerTask

			this.consumerMap[id] = taskId
			id = this.getFreeId()
		}
		Debugf(this, "Launching %d tasks for offer %s", len(tasks), offer.Id.GetValue())
		offersAndTasks[offer] = tasks
	}
	notLaunchedCount := this.NumConsumers - len(this.consumerMap)
	//if we still have unstarted consumers and lack resources - inform the user about this
	if notLaunchedCount != 0 {
		Warnf(this, "There are still %d consumers need to be launched and no more resources are available. Maybe add some more computing power?", notLaunchedCount)
	}
	return offersAndTasks
}

// TaskDied is called each time the task is lost, failed or finished giving the LoadBalancingConsumerTracker a chance to launch it on next resource offer.
func (this *LoadBalancingConsumerTracker) TaskDied(diedId *mesos.TaskID) {
	for id, taskId := range this.consumerMap {
		if taskId.GetValue() == diedId.GetValue() {
			delete(this.consumerMap, id)
			return
		}
	}

	Warn(this, "TaskDied called for not existing TaskID")
}

// Returns all running tasks. For now used only to kill them all.
func (this *LoadBalancingConsumerTracker) GetAllTasks() []*mesos.TaskID {
	ids := make([]*mesos.TaskID, 0)

	for _, taskId := range this.consumerMap {
		ids = append(ids, taskId)
	}

	return ids
}

func (this *LoadBalancingConsumerTracker) getFreeId() int {
	for id := 0; id < this.NumConsumers; id++ {
		if _, exists := this.consumerMap[id]; !exists {
			return id
		}
	}

	return -1
}

func (this *LoadBalancingConsumerTracker) createExecutor(id int) *mesos.ExecutorInfo {
	path := strings.Split(this.Config.ExecutorArchiveName, "/")
	command := fmt.Sprintf("./%s --zookeeper %s --group %s --log.level %s", this.Config.ExecutorBinaryName, strings.Join(this.Config.Zookeeper, ","), this.Config.GroupId, this.Config.LogLevel)
	switch this.Config.Filter.(type) {
	case *WhiteList: command = fmt.Sprintf("%s --whitelist %s", command, this.Config.Filter.Regex())
	case *BlackList: command = fmt.Sprintf("%s --blacklist %s", command, this.Config.Filter.Regex())
	}
	return &mesos.ExecutorInfo{
		ExecutorId: util.NewExecutorID(fmt.Sprintf("kafka-%d", id)),
		Name:       proto.String("Go Kafka Client Executor"),
		Source:     proto.String("go-kafka"),
		Command: &mesos.CommandInfo{
			Value: proto.String(command),
			Uris:  []*mesos.CommandInfo_URI{&mesos.CommandInfo_URI{
				Value: proto.String(fmt.Sprintf("http://%s:%d/%s", this.Config.ArtifactServerHost, this.Config.ArtifactServerPort, path[len(path)-1])),
				Extract: proto.Bool(true),
			}},
		},
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
