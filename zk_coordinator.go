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
	"encoding/json"
	"errors"
	"fmt"
	"github.com/samuel/go-zookeeper/zk"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"
)

var (
	consumersPath    = "/consumers"
	brokerIdsPath    = "/brokers/ids"
	brokerTopicsPath = "/brokers/topics"
	emptyEvent       = zk.Event{}
)

/* ZookeeperCoordinator implements ConsumerCoordinator interface and is used to coordinate multiple consumers that work within the same consumer group. */
type ZookeeperCoordinator struct {
	config      *ZookeeperConfig
	zkConn      *zk.Conn
	unsubscribe chan bool
}

func (this *ZookeeperCoordinator) String() string {
	return "zk"
}

// Creates a new ZookeeperCoordinator with a given configuration.
// The new created ZookeeperCoordinator does NOT automatically connect to zookeeper, you should call Connect() explicitly
func NewZookeeperCoordinator(Config *ZookeeperConfig) *ZookeeperCoordinator {
	return &ZookeeperCoordinator{
		config:      Config,
		unsubscribe: make(chan bool),
	}
}

/* Establish connection to this ConsumerCoordinator. Returns an error if fails to connect, nil otherwise. */
func (this *ZookeeperCoordinator) Connect() error {
	var err error
	for i := 0; i <= this.config.MaxRequestRetries; i++ {
		err := this.tryConnect()
		if err == nil {
			return err
		}
		Tracef(this, "Zookeeper connect failed after %d-th retry", i)
		time.Sleep(this.config.RequestBackoff)
	}
	return err
}

func (this *ZookeeperCoordinator) tryConnect() error {
	Infof(this, "Connecting to ZK at %s\n", this.config.ZookeeperConnect)
	conn, _, err := zk.Connect(this.config.ZookeeperConnect, this.config.ZookeeperTimeout)
	this.zkConn = conn
	return err
}

/* Registers a new consumer with Consumerid id and TopicCount subscription that is a part of consumer group Groupid in this ConsumerCoordinator. Returns an error if registration failed, nil otherwise. */
func (this *ZookeeperCoordinator) RegisterConsumer(Consumerid string, Groupid string, TopicCount TopicsToNumStreams) error {
	this.ensureZkPathsExist(Groupid)
	var err error
	for i := 0; i <= this.config.MaxRequestRetries; i++ {
		err := this.tryRegisterConsumer(Consumerid, Groupid, TopicCount)
		if err == nil {
			return err
		}
		Tracef(this, "Registering consumer %s in group %s failed after %d-th retry", Consumerid, Groupid, i)
		time.Sleep(this.config.RequestBackoff)
	}
	return err
}

func (this *ZookeeperCoordinator) tryRegisterConsumer(Consumerid string, Groupid string, TopicCount TopicsToNumStreams) error {
	Debugf(this, "Trying to register consumer %s at group %s in Zookeeper", Consumerid, Groupid)
	registryDir := newZKGroupDirs(Groupid).ConsumerRegistryDir
	pathToConsumer := fmt.Sprintf("%s/%s", registryDir, Consumerid)
	data, mappingError := json.Marshal(&ConsumerInfo{
		Version:      int16(1),
		Subscription: TopicCount.GetTopicsToNumStreamsMap(),
		Pattern:      TopicCount.Pattern(),
		Timestamp:    time.Now().Unix(),
	})
	if mappingError != nil {
		return mappingError
	}

	Debugf(this, "Path: %s", pathToConsumer)

	_, err := this.zkConn.Create(pathToConsumer, data, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	if err == zk.ErrNoNode {
		err = this.createOrUpdatePathParentMayNotExistFailFast(registryDir, make([]byte, 0))
		if err != nil {
			return err
		}
		_, err = this.zkConn.Create(pathToConsumer, data, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	} else if err == zk.ErrNodeExists {
		_, stat, err := this.zkConn.Get(pathToConsumer)
		if err != nil {
			return err
		}
		_, err = this.zkConn.Set(pathToConsumer, data, stat.Version)
	}

	return err
}

/* Deregisters consumer with Consumerid id that is a part of consumer group Groupid form this ConsumerCoordinator. Returns an error if deregistration failed, nil otherwise. */
func (this *ZookeeperCoordinator) DeregisterConsumer(Consumerid string, Groupid string) error {
	var err error
	for i := 0; i <= this.config.MaxRequestRetries; i++ {
		err := this.tryDeregisterConsumer(Consumerid, Groupid)
		if err == nil {
			return err
		}
		Tracef(this, "Deregistering consumer %s in group %s failed after %d-th retry", Consumerid, Groupid, i)
		time.Sleep(this.config.RequestBackoff)
	}
	return err
}

func (this *ZookeeperCoordinator) tryDeregisterConsumer(Consumerid string, Groupid string) error {
	path := fmt.Sprintf("%s/%s", newZKGroupDirs(Groupid).ConsumerRegistryDir, Consumerid)
	Debugf(this, "Trying to deregister consumer at path: %s", path)
	return this.deleteNode(path)
}

// Gets the information about consumer with Consumerid id that is a part of consumer group Groupid from this ConsumerCoordinator.
// Returns ConsumerInfo on success and error otherwise (For example if consumer with given Consumerid does not exist).
func (this *ZookeeperCoordinator) GetConsumerInfo(Consumerid string, Groupid string) (*ConsumerInfo, error) {
	var err error
	for i := 0; i <= this.config.MaxRequestRetries; i++ {
		info, err := this.tryGetConsumerInfo(Consumerid, Groupid)
		if err == nil {
			return info, err
		}
		Tracef(this, "GetConsumerInfo failed for consumer %s in group %s after %d-th retry", Consumerid, Groupid, i)
		time.Sleep(this.config.RequestBackoff)
	}
	return nil, err
}

func (this *ZookeeperCoordinator) tryGetConsumerInfo(Consumerid string, Groupid string) (*ConsumerInfo, error) {
	data, _, err := this.zkConn.Get(fmt.Sprintf("%s/%s",
		newZKGroupDirs(Groupid).ConsumerRegistryDir, Consumerid))
	if err != nil {
		return nil, err
	}
	consumerInfo := &ConsumerInfo{}
	json.Unmarshal(data, consumerInfo)

	return consumerInfo, nil
}

// Gets the information about consumers per topic in consumer group Groupid excluding internal topics (such as offsets) if ExcludeInternalTopics = true.
// Returns a map where keys are topic names and values are slices of consumer ids and fetcher ids associated with this topic and error on failure.
func (this *ZookeeperCoordinator) GetConsumersPerTopic(Groupid string, ExcludeInternalTopics bool) (map[string][]ConsumerThreadId, error) {
	var err error
	for i := 0; i <= this.config.MaxRequestRetries; i++ {
		consumers, err := this.tryGetConsumersPerTopic(Groupid, ExcludeInternalTopics)
		if err == nil {
			return consumers, err
		}
		Tracef(this, "GetConsumersPerTopic failed for group %s after %d-th retry", Groupid, i)
		time.Sleep(this.config.RequestBackoff)
	}
	return nil, err
}

func (this *ZookeeperCoordinator) tryGetConsumersPerTopic(Groupid string, ExcludeInternalTopics bool) (map[string][]ConsumerThreadId, error) {
	consumers, err := this.GetConsumersInGroup(Groupid)
	if err != nil {
		return nil, err
	}
	consumersPerTopicMap := make(map[string][]ConsumerThreadId)
	for _, consumer := range consumers {
		topicsToNumStreams, err := NewTopicsToNumStreams(Groupid, consumer, this, ExcludeInternalTopics)
		if err != nil {
			return nil, err
		}

		for topic, threadIds := range topicsToNumStreams.GetConsumerThreadIdsPerTopic() {
			for _, threadId := range threadIds {
				consumersPerTopicMap[topic] = append(consumersPerTopicMap[topic], threadId)
			}
		}
	}

	for topic := range consumersPerTopicMap {
		sort.Sort(byName(consumersPerTopicMap[topic]))
	}

	return consumersPerTopicMap, nil
}

/* Gets the list of all consumer ids within a consumer group Groupid. Returns a slice containing all consumer ids in group and error on failure. */
func (this *ZookeeperCoordinator) GetConsumersInGroup(Groupid string) ([]string, error) {
	var err error
	for i := 0; i <= this.config.MaxRequestRetries; i++ {
		consumers, err := this.tryGetConsumersInGroup(Groupid)
		if err == nil {
			return consumers, err
		}
		Tracef(this, "GetConsumersInGroup failed for group %s after %d-th retry", Groupid, i)
		time.Sleep(this.config.RequestBackoff)
	}
	return nil, err
}

func (this *ZookeeperCoordinator) tryGetConsumersInGroup(Groupid string) ([]string, error) {
	Debugf(this, "Getting consumers in group %s", Groupid)
	consumers, _, err := this.zkConn.Children(newZKGroupDirs(Groupid).ConsumerRegistryDir)
	if err != nil {
		return nil, err
	}

	return consumers, nil
}

/* Gets the list of all topics registered in this ConsumerCoordinator. Returns a slice conaining topic names and error on failure. */
func (this *ZookeeperCoordinator) GetAllTopics() ([]string, error) {
	var err error
	for i := 0; i <= this.config.MaxRequestRetries; i++ {
		topics, err := this.tryGetAllTopics()
		if err == nil {
			return topics, err
		}
		Tracef(this, "GetAllTopics failed after %d-th retry", i)
		time.Sleep(this.config.RequestBackoff)
	}
	return nil, err
}

func (this *ZookeeperCoordinator) tryGetAllTopics() ([]string, error) {
	topics, _, _, err := this.zkConn.ChildrenW(brokerTopicsPath)
	return topics, err
}

// Gets the information about existing partitions for a given Topics.
// Returns a map where keys are topic names and values are slices of partition ids associated with this topic and error on failure.
func (this *ZookeeperCoordinator) GetPartitionsForTopics(Topics []string) (map[string][]int32, error) {
	var err error
	for i := 0; i <= this.config.MaxRequestRetries; i++ {
		partitions, err := this.tryGetPartitionsForTopics(Topics)
		if err == nil {
			return partitions, err
		}
		Tracef(this, "GetPartitionsForTopics for topics %s failed after %d-th retry", Topics, i)
		time.Sleep(this.config.RequestBackoff)
	}
	return nil, err
}

func (this *ZookeeperCoordinator) tryGetPartitionsForTopics(Topics []string) (map[string][]int32, error) {
	result := make(map[string][]int32)
	partitionAssignments, err := this.getPartitionAssignmentsForTopics(Topics)
	if err != nil {
		return nil, err
	}
	for topic, partitionAssignment := range partitionAssignments {
		for partition, _ := range partitionAssignment {
			result[topic] = append(result[topic], partition)
		}
	}

	for topic, _ := range partitionAssignments {
		sort.Sort(intArray(result[topic]))
	}

	return result, nil
}

// Gets the information about all Kafka brokers registered in this ConsumerCoordinator.
// Returns a slice of BrokerInfo and error on failure.
func (this *ZookeeperCoordinator) GetAllBrokers() ([]*BrokerInfo, error) {
	var err error
	for i := 0; i <= this.config.MaxRequestRetries; i++ {
		brokers, err := this.tryGetAllBrokers()
		if err == nil {
			return brokers, err
		}
		Tracef(this, "GetAllBrokers failed after %d-th retry", i)
		time.Sleep(this.config.RequestBackoff)
	}
	return nil, err
}

func (this *ZookeeperCoordinator) tryGetAllBrokers() ([]*BrokerInfo, error) {
	Debug(this, "Getting all brokers in cluster")
	brokerIds, _, err := this.zkConn.Children(brokerIdsPath)
	if err != nil {
		return nil, err
	}
	brokers := make([]*BrokerInfo, len(brokerIds))
	for i, brokerId := range brokerIds {
		brokerIdNum, err := strconv.Atoi(brokerId)
		if err != nil {
			return nil, err
		}

		brokers[i], err = this.getBrokerInfo(int32(brokerIdNum))
		brokers[i].Id = int32(brokerIdNum)
		if err != nil {
			return nil, err
		}
	}

	return brokers, nil
}

// Gets the offset for a given TopicPartition and consumer group Groupid.
// Returns offset on sucess, error otherwise.
func (this *ZookeeperCoordinator) GetOffsetForTopicPartition(Groupid string, TopicPartition *TopicAndPartition) (int64, error) {
	var err error
	for i := 0; i <= this.config.MaxRequestRetries; i++ {
		offset, err := this.tryGetOffsetForTopicPartition(Groupid, TopicPartition)
		if err == nil {
			return offset, err
		}
		Tracef(this, "GetOffsetForTopicPartition for group %s and topic-partitions %s failed after %d-th retry", Groupid, TopicPartition, i)
		time.Sleep(this.config.RequestBackoff)
	}
	return InvalidOffset, err
}

func (this *ZookeeperCoordinator) tryGetOffsetForTopicPartition(Groupid string, TopicPartition *TopicAndPartition) (int64, error) {
	dirs := newZKGroupTopicDirs(Groupid, TopicPartition.Topic)
	offset, _, err := this.zkConn.Get(fmt.Sprintf("%s/%d", dirs.ConsumerOffsetDir, TopicPartition.Partition))
	if err != nil {
		if err == zk.ErrNoNode {
			return InvalidOffset, nil
		} else {
			return InvalidOffset, err
		}
	}

	offsetNum, err := strconv.Atoi(string(offset))
	if err != nil {
		return InvalidOffset, err
	}

	return int64(offsetNum), nil
}

// Notifies consumer group about new deployed topic, which should be taken after current one is exhausted
func (this *ZookeeperCoordinator) NotifyConsumerGroup(Groupid string, ConsumerId string) error {
	var err error
	for i := 0; i <= this.config.MaxRequestRetries; i++ {
		err := this.tryNotifyConsumerGroup(Groupid, ConsumerId)
		if err == nil {
			return err
		}
		Tracef(this, "NotifyConsumerGroup for consumer %s and group %s failed after %d-th retry", ConsumerId, Groupid, i)
		time.Sleep(this.config.RequestBackoff)
	}
	return err
}

func (this *ZookeeperCoordinator) tryNotifyConsumerGroup(Groupid string, ConsumerId string) error {
	path := fmt.Sprintf("%s/%s-%d", newZKGroupDirs(Groupid).ConsumerApiDir, ConsumerId, time.Now().Nanosecond())
	Debugf(this, "Sending notification to consumer group at %s", path)
	return this.createOrUpdatePathParentMayNotExistFailFast(path, make([]byte, 0))
}

// Removes a notification notificationId for consumer group Group
func (this *ZookeeperCoordinator) PurgeNotificationForGroup(Groupid string, notificationId string) error {
	var err error
	for i := 0; i <= this.config.MaxRequestRetries; i++ {
		err := this.tryPurgeNotificationForGroup(Groupid, notificationId)
		if err == nil {
			return err
		}
		Tracef(this, "PurgeNotificationForGroup for group %s and notification %s failed after %d-th retry", Groupid, notificationId, i)
		time.Sleep(this.config.RequestBackoff)
	}
	return err
}

func (this *ZookeeperCoordinator) tryPurgeNotificationForGroup(Groupid string, notificationId string) error {
	path := fmt.Sprintf("%s/%s", newZKGroupDirs(Groupid).ConsumerApiDir, notificationId)
	return this.deleteNode(path)
}

// Subscribes for any change that should trigger consumer rebalance on consumer group Groupid in this ConsumerCoordinator.
// Returns a read-only channel of booleans that will get values on any significant coordinator event (e.g. new consumer appeared, new broker appeared etc.) and error if failed to subscribe.
func (this *ZookeeperCoordinator) SubscribeForChanges(Groupid string) (<-chan CoordinatorEvent, error) {
	var err error
	for i := 0; i <= this.config.MaxRequestRetries; i++ {
		events, err := this.trySubscribeForChanges(Groupid)
		if err == nil {
			return events, err
		}
		Tracef(this, "SubscribeForChanges for group %s failed after %d-th retry", Groupid, i)
		time.Sleep(this.config.RequestBackoff)
	}
	return nil, err
}

func (this *ZookeeperCoordinator) trySubscribeForChanges(Groupid string) (<-chan CoordinatorEvent, error) {
	changes := make(chan CoordinatorEvent)
	Infof(this, "Subscribing for changes for %s", Groupid)

	consumersWatcher, err := this.getConsumersInGroupWatcher(Groupid)
	if err != nil {
		return nil, err
	}
	blueGreenWatcher, err := this.getBlueGreenWatcher(Groupid)
	if err != nil {
		return nil, err
	}
	topicsWatcher, err := this.getTopicsWatcher()
	if err != nil {
		return nil, err
	}
	brokersWatcher, err := this.getAllBrokersInClusterWatcher()
	if err != nil {
		return nil, err
	}

	inputChannels := make([]*<-chan zk.Event, 0)
	inputChannels = append(inputChannels, &consumersWatcher, &blueGreenWatcher, &topicsWatcher, &brokersWatcher)
	zkEvents := make(chan zk.Event)
	stopRedirecting := redirectChannelsTo(inputChannels, zkEvents)

	go func() {
		for {
			select {
			case e := <-zkEvents:
			{
				Trace(this, e)
				if e != emptyEvent {
					if e.Type != zk.EventNotWatching && e.State != zk.StateDisconnected {
						if strings.HasPrefix(e.Path, fmt.Sprintf("%s/%s", newZKGroupDirs(Groupid).ConsumerApiDir, BlueGreenDeploymentAPI)) {
							changes <- BlueGreenRequest
						} else {
							changes <- Regular
						}
					}

					if strings.HasPrefix(e.Path, newZKGroupDirs(Groupid).ConsumerRegistryDir) {
						Info(this, "Trying to renew watcher for consumer registry")
						consumersWatcher, err = this.getConsumersInGroupWatcher(Groupid)
						if err != nil {
							panic(err)
						}
					} else if strings.HasPrefix(e.Path, fmt.Sprintf("%s/%s", newZKGroupDirs(Groupid).ConsumerApiDir, BlueGreenDeploymentAPI)) {
						Info(this, "Trying to renew watcher for consumer API dir")
						blueGreenWatcher, err = this.getBlueGreenWatcher(Groupid)
						if err != nil {
							panic(err)
						}
					} else if strings.HasPrefix(e.Path, brokerTopicsPath) {
						Info(this, "Trying to renew watcher for consumer topic dir")
						topicsWatcher, err = this.getTopicsWatcher()
						if err != nil {
							panic(err)
						}
					} else if strings.HasPrefix(e.Path, brokerIdsPath) {
						Info(this, "Trying to renew watcher for brokers in cluster")
						brokersWatcher, err = this.getAllBrokersInClusterWatcher()
						if err != nil {
							panic(err)
						}
					} else {
						Warnf(this, "Unknown event path: %s", e.Path)
					}

					stopRedirecting <- true
					stopRedirecting = redirectChannelsTo(inputChannels, zkEvents)
				}
			}
			case <-this.unsubscribe:
			{
				stopRedirecting <- true
				return
			}
			}
		}
	}()

	return changes, nil
}

// Gets all deployed topics for consume group Group from consumer coordinator.
// Returns a map where keys are notification ids and values are DeployedTopics. May also return an error (e.g. if failed to reach coordinator).
func (this *ZookeeperCoordinator) GetBlueGreenRequest(Group string) (map[string]*BlueGreenDeployment, error) {
	var err error
	for i := 0; i <= this.config.MaxRequestRetries; i++ {
		topics, err := this.tryGetBlueGreenRequest(Group)
		if err == nil {
			return topics, err
		}
		Tracef(this, "GetNewDeployedTopics for group %s failed after %d-th retry", Group, i)
		time.Sleep(this.config.RequestBackoff)
	}
	return nil, err
}

func (this *ZookeeperCoordinator) tryGetBlueGreenRequest(Group string) (map[string]*BlueGreenDeployment, error) {
	apiPath := fmt.Sprintf("%s/%s", newZKGroupDirs(Group).ConsumerApiDir, BlueGreenDeploymentAPI)
	children, _, err := this.zkConn.Children(apiPath)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Unable to get new deployed topics: %s", err.Error()))
	}

	deployedTopics := make(map[string]*BlueGreenDeployment)
	for _, child := range children {
		entryPath := fmt.Sprintf("%s/%s", apiPath, child)
		rawDeployedTopicsEntry, _, err := this.zkConn.Get(entryPath)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("Unable to fetch deployed topic entry %s: %s", entryPath, err.Error()))
		}
		deployedTopicsEntry := &BlueGreenDeployment{}
		err = json.Unmarshal(rawDeployedTopicsEntry, deployedTopicsEntry)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("Unable to parse deployed topic entry %s: %s", rawDeployedTopicsEntry, err.Error()))
		}

		deployedTopics[child] = deployedTopicsEntry
	}

	return deployedTopics, nil
}

func (this *ZookeeperCoordinator) RequestBlueGreenDeployment(blue BlueGreenDeployment, green BlueGreenDeployment) error {
	var err error
	for i := 0; i <= this.config.MaxRequestRetries; i++ {
		err := this.tryRequestBlueGreenDeployment(green.Group, blue)
		if err == nil {
			break
		}
		Tracef(this, "DeployTopics for group %s and topics %s failed after %d-th retry", green.Group, blue.Topics, i)
		time.Sleep(this.config.RequestBackoff)
	}

	if err != nil {
		return err
	}

	for i := 0; i <= this.config.MaxRequestRetries; i++ {
		err := this.tryRequestBlueGreenDeployment(blue.Group, green)
		if err == nil {
			return err
		}
		Tracef(this, "DeployTopics for group %s and topics %s failed after %d-th retry", blue.Group, green.Topics, i)
		time.Sleep(this.config.RequestBackoff)
	}

	return err
}

func (this *ZookeeperCoordinator) tryRequestBlueGreenDeployment(Group string, blueOrGreen BlueGreenDeployment) error {
	data, err := json.Marshal(blueOrGreen)
	if err != nil {
		return err
	}
	return this.createOrUpdatePathParentMayNotExistFailFast(fmt.Sprintf("%s/%s/%d", newZKGroupDirs(Group).ConsumerApiDir, BlueGreenDeploymentAPI, time.Now().Unix()), data)
}

func (this *ZookeeperCoordinator) RemoveOldApiRequests(group string) error {
	var err error
	for _, api := range availableAPIs {
		err = this.tryRemoveOldApiRequests(group, api)
		if err != nil {
			break
		}
	}

	return err
}

func (this *ZookeeperCoordinator) tryRemoveOldApiRequests(group string, api ConsumerGroupApi) error {
	requests := make([]string, 0)
	var err error
	apiPath := fmt.Sprintf("%s/%s", newZKGroupDirs(group).ConsumerApiDir, api)
	for i := 0; i <= this.config.MaxRequestRetries; i++ {
		requests, _, err = this.zkConn.Children(apiPath)
		if err != nil {
			continue
		}
		for _, request := range requests {
			err = this.deleteNode(fmt.Sprintf("%s/%s", apiPath, request))
			if err != nil && err != zk.ErrNoNode {
				break
			}
		}
	}

	return err
}

func (this *ZookeeperCoordinator) AwaitOnStateBarrier(consumerId string, group string, barrierName string,
												   	  barrierSize int, api string, timeout time.Duration) bool {
	finished := make(chan bool)
	memberJoinedEvents, err := this.joinStateBarrier(consumerId, group, barrierName, api, finished)
	if err != nil {
		panic(err)
	}

	passed, err := this.isStateBarrierPassed(group, barrierName, api, barrierSize)
	barrierLoop:
	for !passed {
		select {
		case <-memberJoinedEvents:
		{
			passed, err = this.isStateBarrierPassed(group, barrierName, api, barrierSize)
			if err != nil {
				panic(err)
			}
		}
		case <-time.After(timeout):
		{
			err = this.removeStateBarrier(group, barrierName, api)
			if err != nil {
				panic(err)
			}
			break barrierLoop
		}
		}
	}

	finished <- true

	return passed
}

func (this *ZookeeperCoordinator) joinStateBarrier(consumerId string, group string, stateHash string, api string, finished chan bool) (<-chan CoordinatorEvent, error) {
	path := fmt.Sprintf("%s/%s/%s", newZKGroupDirs(group).ConsumerApiDir, api, stateHash)
	var err error
	for i := 0; i <= this.config.MaxRequestRetries; i++ {
		Infof(this, "Commencing assertion series at %s", path)
		_, err = this.zkConn.Create(path, make([]byte, 0), 0, zk.WorldACL(zk.PermAll))
		if err == nil || err == zk.ErrNodeExists {
			Infof(this, "Joining state barrier %s", path)
			_, _, zkWatcher, err := this.zkConn.ChildrenW(path)
			watcher := make(chan CoordinatorEvent)
			if err == nil {
				go func() {
					for {
						select {
						case <-finished:
							return
						case e := <-zkWatcher:
							{
								if e != emptyEvent {
									if e.Type != zk.EventNotWatching && e.State != zk.StateDisconnected {
										select {
										case watcher <- Regular:
										case <-finished:
											return
										}
									}
									_, _, zkWatcher, err = this.zkConn.ChildrenW(path)
								}
							}
						}
					}
				}()
			}
			err = this.createOrUpdatePathParentMayNotExistFailSafe(fmt.Sprintf("%s/%s", path, consumerId), make([]byte, 0))
			if err != nil {
				panic(err)
			}
			Infof(this, "Successfully joined state barrier %s", path)

			return watcher, err
		}

		Warnf(this, "Failed to join state barrier %s, retrying...", path)
		time.Sleep(this.config.RequestBackoff)
	}

	Errorf(this, "Failed to join state barrier %s after %d retries", path, this.config.MaxRequestRetries)

	return nil, err
}

func (this *ZookeeperCoordinator) isStateBarrierPassed(group string, stateHash string, api string, expected int) (bool, error) {
	Debugf(this, "Trying to assert rebalance state for group %s and hash %s with %d", group, stateHash, expected)
	path := fmt.Sprintf("%s/%s/%s", newZKGroupDirs(group).ConsumerApiDir, api, stateHash)
	var children []string
	var err error
	for i := 0; i <= this.config.MaxRequestRetries; i++ {
		children, _, err = this.zkConn.Children(path)
		if err == zk.ErrNoNode {
			return false, nil
		} else if err == nil {
			return len(children) == expected, err
		}
		Warnf(this, "Failed to assert rebalance state %s, retrying...", path)
		time.Sleep(this.config.RequestBackoff)
	}
	Errorf(this, "Failed to assert rebalance state %s after %d retries", path, this.config.MaxRequestRetries)

	return false, err
}

func (this *ZookeeperCoordinator) removeStateBarrier(group string, stateHash string, api string	) error {
	var err error
	for i := 0; i <= this.config.MaxRequestRetries; i++ {
		err = this.tryRemoveStateBarrier(group, stateHash, api)
		if err == nil || err == zk.ErrNoNode {
			return nil
		}
		Tracef(this, "State assertion deletion %s in group %s failed after %d-th retry", hash, group, i)
		time.Sleep(this.config.RequestBackoff)
	}

	return err
}

func (this *ZookeeperCoordinator) tryRemoveStateBarrier(group string, stateHash string, api string) error {
	path := fmt.Sprintf("%s/%s/%s", newZKGroupDirs(group).ConsumerApiDir, api, stateHash)
	Debugf(this, "Trying to fail rebalance at path: %s", path)

	return this.deleteNode(path)
}

func (this *ZookeeperCoordinator) deleteNode(path string) error {
	children, _, err := this.zkConn.Children(path)
	if err != nil {
		return err
	}
	for _, child := range children {
		err := this.deleteNode(fmt.Sprintf("%s/%s", path, child))
		if err != nil && err != zk.ErrNoNode {
			return err
		}
	}

	_, stat, err := this.zkConn.Get(path)
	if err != nil {
		return err
	}
	return this.zkConn.Delete(path, stat.Version)
}

/* Tells the ConsumerCoordinator to unsubscribe from events for the consumer it is associated with. */
func (this *ZookeeperCoordinator) Unsubscribe() {
	this.unsubscribe <- true
}

// Tells the ConsumerCoordinator to claim partition topic Topic and partition Partition for consumerThreadId fetcher that works within a consumer group Group.
// Returns true if claim is successful, false and error explaining failure otherwise.
func (this *ZookeeperCoordinator) ClaimPartitionOwnership(Groupid string, Topic string, Partition int32, consumerThreadId ConsumerThreadId) (bool, error) {
	var err error
	for i := 0; i <= this.config.MaxRequestRetries; i++ {
		ok, err := this.tryClaimPartitionOwnership(Groupid, Topic, Partition, consumerThreadId)
		if ok {
			return ok, err
		}
		Tracef(this, "Claim failed for topic %s, partition %d after %d-th retry", Topic, Partition, i)
		time.Sleep(this.config.RequestBackoff)
	}
	return false, err
}

func (this *ZookeeperCoordinator) tryClaimPartitionOwnership(group string, topic string, partition int32, consumerThreadId ConsumerThreadId) (bool, error) {
	dirs := newZKGroupTopicDirs(group, topic)
	this.createOrUpdatePathParentMayNotExistFailFast(dirs.ConsumerOwnerDir, make([]byte, 0))

	pathToOwn := fmt.Sprintf("%s/%d", dirs.ConsumerOwnerDir, partition)
	_, err := this.zkConn.Create(pathToOwn, []byte(consumerThreadId.String()), zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	if err == zk.ErrNoNode {
		err = this.createOrUpdatePathParentMayNotExistFailFast(dirs.ConsumerOwnerDir, make([]byte, 0))
		if err != nil {
			return false, err
		}
		_, err = this.zkConn.Create(pathToOwn, []byte(consumerThreadId.String()), zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	}

	if err != nil {
		if err == zk.ErrNodeExists {
			Debugf(consumerThreadId, "waiting for the partition ownership to be deleted: %d", partition)
			return false, nil
		} else {
			Error(consumerThreadId, err)
			return false, err
		}
	}

	Debugf(this, "Successfully claimed partition %d in topic %s for %s", partition, topic, consumerThreadId)

	return true, nil
}

// Tells the ConsumerCoordinator to release partition ownership on topic Topic and partition Partition for consumer group Groupid.
// Returns error if failed to released partition ownership.
func (this *ZookeeperCoordinator) ReleasePartitionOwnership(Groupid string, Topic string, Partition int32) error {
	var err error
	for i := 0; i <= this.config.MaxRequestRetries; i++ {
		err = this.tryReleasePartitionOwnership(Groupid, Topic, Partition)
		if err == nil {
			return err
		}
		Tracef(this, "ReleasePartitionOwnership failed for group %s, topic %s, partition %d after %d-th retry", Groupid, Topic, Partition, i)
		time.Sleep(this.config.RequestBackoff)
	}
	return err
}

func (this *ZookeeperCoordinator) tryReleasePartitionOwnership(group string, topic string, partition int32) error {
	path := fmt.Sprintf("%s/%d", newZKGroupTopicDirs(group, topic).ConsumerOwnerDir, partition)
	err := this.deleteNode(path)
	if err != nil && err != zk.ErrNoNode {
		return err
	} else {
		return nil
	}
}

// Tells the ConsumerCoordinator to commit offset Offset for topic and partition TopicPartition for consumer group Groupid.
// Returns error if failed to commit offset.
func (this *ZookeeperCoordinator) CommitOffset(Groupid string, TopicPartition *TopicAndPartition, Offset int64) error {
	dirs := newZKGroupTopicDirs(Groupid, TopicPartition.Topic)
	err := this.updateRecord(fmt.Sprintf("%s/%d", dirs.ConsumerOffsetDir, TopicPartition.Partition), []byte(strconv.FormatInt(Offset, 10)))
	if err == zk.ErrNoNode {
		return this.createOrUpdatePathParentMayNotExistFailFast(fmt.Sprintf("%s/%d", dirs.ConsumerOffsetDir, TopicPartition.Partition), []byte(strconv.FormatInt(Offset, 10)))
	}

	return err
}

func (this *ZookeeperCoordinator) ensureZkPathsExist(group string) {
	dirs := newZKGroupDirs(group)
	this.createOrUpdatePathParentMayNotExistFailSafe(dirs.ConsumerDir, make([]byte, 0))
	this.createOrUpdatePathParentMayNotExistFailSafe(dirs.ConsumerGroupDir, make([]byte, 0))
	this.createOrUpdatePathParentMayNotExistFailSafe(dirs.ConsumerRegistryDir, make([]byte, 0))
	this.createOrUpdatePathParentMayNotExistFailSafe(dirs.ConsumerApiDir, make([]byte, 0))
	for _, api := range availableAPIs {
		this.createOrUpdatePathParentMayNotExistFailSafe(fmt.Sprintf("%s/%s", dirs.ConsumerApiDir, api), make([]byte, 0))
	}
}

func (this *ZookeeperCoordinator) getAllBrokersInClusterWatcher() (<-chan zk.Event, error) {
	Debug(this, "Subscribing for events from broker registry")
	_, _, watcher, err := this.zkConn.ChildrenW(brokerIdsPath)
	if err != nil {
		return nil, err
	}

	return watcher, nil
}

func (this *ZookeeperCoordinator) getConsumersInGroupWatcher(group string) (<-chan zk.Event, error) {
	Debugf(this, "Getting consumer watcher for group %s", group)
	_, _, watcher, err := this.zkConn.ChildrenW(newZKGroupDirs(group).ConsumerRegistryDir)
	if err != nil {
		return nil, err
	}

	return watcher, nil
}

func (this *ZookeeperCoordinator) getBlueGreenWatcher(group string) (<-chan zk.Event, error) {
	Debugf(this, "Getting watcher for consumer group '%s' API", group)
	_, _, watcher, err := this.zkConn.ChildrenW(fmt.Sprintf("%s/%s", newZKGroupDirs(group).ConsumerApiDir, BlueGreenDeploymentAPI))
	if err != nil {
		return nil, err
	}

	return watcher, nil
}

func (this *ZookeeperCoordinator) getTopicsWatcher() (watcher <-chan zk.Event, err error) {
	_, _, watcher, err = this.zkConn.ChildrenW(brokerTopicsPath)
	return
}

func (this *ZookeeperCoordinator) getBrokerInfo(brokerId int32) (*BrokerInfo, error) {
	Debugf(this, "Getting info for broker %d", brokerId)
	pathToBroker := fmt.Sprintf("%s/%d", brokerIdsPath, brokerId)
	data, _, zkError := this.zkConn.Get(pathToBroker)
	if zkError != nil {
		return nil, zkError
	}

	broker := &BrokerInfo{}
	mappingError := json.Unmarshal([]byte(data), broker)

	return broker, mappingError
}

func (this *ZookeeperCoordinator) getPartitionAssignmentsForTopics(topics []string) (map[string]map[int32][]int32, error) {
	Debugf(this, "Trying to get partition assignments for topics %v", topics)
	result := make(map[string]map[int32][]int32)
	for _, topic := range topics {
		topicInfo, err := this.getTopicInfo(topic)
		if err != nil {
			return nil, err
		}
		result[topic] = make(map[int32][]int32)
		for partition, replicaIds := range topicInfo.Partitions {
			partitionInt, err := strconv.Atoi(partition)
			if err != nil {
				return nil, err
			}
			result[topic][int32(partitionInt)] = replicaIds
		}
	}

	return result, nil
}

func (this *ZookeeperCoordinator) getTopicInfo(topic string) (*TopicInfo, error) {
	data, _, err := this.zkConn.Get(fmt.Sprintf("%s/%s", brokerTopicsPath, topic))
	if err != nil {
		return nil, err
	}
	topicInfo := &TopicInfo{}
	err = json.Unmarshal(data, topicInfo)
	if err != nil {
		return nil, err
	}

	return topicInfo, nil
}

func (this *ZookeeperCoordinator) createOrUpdatePathParentMayNotExistFailSafe(pathToCreate string, data []byte) error {
	return this.createOrUpdatePathParentMayNotExist(pathToCreate, data, true)
}

func (this *ZookeeperCoordinator) createOrUpdatePathParentMayNotExistFailFast(pathToCreate string, data []byte) error {
	return this.createOrUpdatePathParentMayNotExist(pathToCreate, data, false)
}

func (this *ZookeeperCoordinator) createOrUpdatePathParentMayNotExist(pathToCreate string, data []byte, failSafe bool) error {
	Debugf(this, "Trying to create path %s in Zookeeper", pathToCreate)
	_, err := this.zkConn.Create(pathToCreate, data, 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		if zk.ErrNodeExists == err {
			if len(data) > 0 {
				Debugf(this, "Trying to update existing node %s", pathToCreate)
				return this.updateRecord(pathToCreate, data)
			} else {
				return nil
			}
		} else {
			parent, _ := path.Split(pathToCreate)
			err = this.createOrUpdatePathParentMayNotExist(parent[:len(parent)-1], make([]byte, 0), failSafe)
			if err != nil {
				if zk.ErrNodeExists != err {
					Error(this, err.Error())
				}
				return err
			} else {
				Debugf(this, "Successfully created path %s", parent[:len(parent)-1])
			}

			Debugf(this, "Trying again to create path %s in Zookeeper", pathToCreate)
			_, err = this.zkConn.Create(pathToCreate, data, 0, zk.WorldACL(zk.PermAll))
			if err == zk.ErrNodeExists && failSafe {
				err = nil
			}
		}
	}

	return err
}

func (this *ZookeeperCoordinator) updateRecord(pathToCreate string, dataToWrite []byte) error {
	Debugf(this, "Trying to update path %s", pathToCreate)
	_, stat, _ := this.zkConn.Get(pathToCreate)
	_, err := this.zkConn.Set(pathToCreate, dataToWrite, stat.Version)

	return err
}

/* ZookeeperConfig is used to pass multiple configuration entries to ZookeeperCoordinator. */
type ZookeeperConfig struct {
	/* Zookeeper hosts */
	ZookeeperConnect []string

	/* Zookeeper read timeout */
	ZookeeperTimeout time.Duration

	/* Max retries for any request except CommitOffset. CommitOffset is controlled by ConsumerConfig.OffsetsCommitMaxRetries. */
	MaxRequestRetries int

	/* Backoff to retry any request */
	RequestBackoff time.Duration
}

/* Created a new ZookeeperConfig with sane defaults. Default ZookeeperConnect points to localhost. */
func NewZookeeperConfig() *ZookeeperConfig {
	config := &ZookeeperConfig{}
	config.ZookeeperConnect = []string{"localhost"}
	config.ZookeeperTimeout = 1*time.Second
	config.MaxRequestRetries = 3
	config.RequestBackoff = 150*time.Millisecond

	return config
}

func ZookeeperConfigFromFile(filename string) (*ZookeeperConfig, error) {
	z, err := LoadConfiguration(filename)
	if err != nil {
		return nil, err
	}

	config := NewZookeeperConfig()
	setStringSliceConfig(&config.ZookeeperConnect, z["zookeeper.connect"], ",")
	if setDurationConfig(&config.ZookeeperTimeout, z["zookeeper.connection.timeout"]) != nil {
		return nil, err
	}
	if setIntConfig(&config.MaxRequestRetries, z["zookeeper.max.request.retries"]) != nil {
		return nil, err
	}
	if setDurationConfig(&config.RequestBackoff, z["zookeeper.request.backoff"]) != nil {
		return nil, err
	}

	return config, nil
}

type zkGroupDirs struct {
	Group                string
	ConsumerDir          string
	ConsumerGroupDir     string
	ConsumerRegistryDir  string
	ConsumerApiDir       string
	ConsumerRebalanceDir string
}

func newZKGroupDirs(group string) *zkGroupDirs {
	consumerGroupDir := fmt.Sprintf("%s/%s", consumersPath, group)
	consumerRegistryDir := fmt.Sprintf("%s/ids", consumerGroupDir)
	consumerApiDir := fmt.Sprintf("%s/api", consumerGroupDir)
	consumerRebalanceDir := fmt.Sprintf("%s/api/rebalance", consumerGroupDir)
	return &zkGroupDirs{
		Group:                group,
		ConsumerDir:          consumersPath,
		ConsumerGroupDir:     consumerGroupDir,
		ConsumerRegistryDir:  consumerRegistryDir,
		ConsumerApiDir:   	  consumerApiDir,
		ConsumerRebalanceDir: consumerRebalanceDir,
	}
}

type zkGroupTopicDirs struct {
	ZkGroupDirs       *zkGroupDirs
	Topic             string
	ConsumerOffsetDir string
	ConsumerOwnerDir  string
}

func newZKGroupTopicDirs(group string, topic string) *zkGroupTopicDirs {
	zkGroupsDirs := newZKGroupDirs(group)
	return &zkGroupTopicDirs{
		ZkGroupDirs:       zkGroupsDirs,
		Topic:             topic,
		ConsumerOffsetDir: fmt.Sprintf("%s/%s/%s", zkGroupsDirs.ConsumerGroupDir, "offsets", topic),
		ConsumerOwnerDir:  fmt.Sprintf("%s/%s/%s", zkGroupsDirs.ConsumerGroupDir, "owners", topic),
	}
}

//used for tests only
type mockZookeeperCoordinator struct {
	commitHistory map[TopicAndPartition]int64
}

func newMockZookeeperCoordinator() *mockZookeeperCoordinator {
	return &mockZookeeperCoordinator{
		commitHistory: make(map[TopicAndPartition]int64),
	}
}

func (mzk *mockZookeeperCoordinator) Connect() error { panic("Not implemented") }
func (mzk *mockZookeeperCoordinator) RegisterConsumer(consumerid string, group string, topicCount TopicsToNumStreams) error {
	panic("Not implemented")
}
func (mzk *mockZookeeperCoordinator) DeregisterConsumer(consumerid string, group string) error {
	panic("Not implemented")
}
func (mzk *mockZookeeperCoordinator) GetConsumerInfo(consumerid string, group string) (*ConsumerInfo, error) {
	panic("Not implemented")
}
func (mzk *mockZookeeperCoordinator) GetConsumersPerTopic(group string, excludeInternalTopics bool) (map[string][]ConsumerThreadId, error) {
	panic("Not implemented")
}
func (mzk *mockZookeeperCoordinator) GetConsumersInGroup(group string) ([]string, error) {
	panic("Not implemented")
}
func (mzk *mockZookeeperCoordinator) GetAllTopics() ([]string, error) { panic("Not implemented") }
func (mzk *mockZookeeperCoordinator) GetPartitionsForTopics(topics []string) (map[string][]int32, error) {
	panic("Not implemented")
}
func (mzk *mockZookeeperCoordinator) GetAllBrokers() ([]*BrokerInfo, error) { panic("Not implemented") }
func (mzk *mockZookeeperCoordinator) GetOffsetForTopicPartition(group string, topicPartition *TopicAndPartition) (int64, error) {
	panic("Not implemented")
}
func (mzk *mockZookeeperCoordinator) NotifyConsumerGroup(group string, consumerId string) error {
	panic("Not implemented")
}
func (mzk *mockZookeeperCoordinator) PurgeNotificationForGroup(Group string, notificationId string) error {
	panic("Not implemented")
}
func (mzk *mockZookeeperCoordinator) SubscribeForChanges(group string) (<-chan CoordinatorEvent, error) {
	panic("Not implemented")
}
func (mzk *mockZookeeperCoordinator) GetNewDeployedTopics(Group string) (map[string]*BlueGreenDeployment, error) {
	panic("Not implemented")
}
func (mzk *mockZookeeperCoordinator) AwaitOnStateBarrier(consumerId string, group string, stateHash string, barrierSize int, api string, timeout time.Duration) bool {
	panic("Not implemented")
}
func (mzk *mockZookeeperCoordinator) Unsubscribe() { panic("Not implemented") }
func (mzk *mockZookeeperCoordinator) ClaimPartitionOwnership(group string, topic string, partition int32, consumerThreadId ConsumerThreadId) (bool, error) {
	panic("Not implemented")
}
func (mzk *mockZookeeperCoordinator) ReleasePartitionOwnership(group string, topic string, partition int32) error {
	panic("Not implemented")
}
func (mzk *mockZookeeperCoordinator) CommitOffset(group string, topicPartition *TopicAndPartition, offset int64) error {
	mzk.commitHistory[*topicPartition] = offset
	return nil
}
func (this *mockZookeeperCoordinator) RemoveOldApiRequests(group string) error {
	panic("Not implemented")
}
