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
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

var (
	consumersPath    = "/consumers"
	brokerIdsPath    = "/brokers/ids"
	brokerTopicsPath = "/brokers/topics"
)

type GroupWatch struct {
	coordinatorEvents chan CoordinatorEvent
	zkEvents          chan zk.Event
	poisonPillMessage string
}

// ZookeeperCoordinator implements ConsumerCoordinator and OffsetStorage interfaces and is used to coordinate multiple consumers that work within the same consumer group
// as well as storing and retrieving their offsets.
type ZookeeperCoordinator struct {
	config      *ZookeeperConfig
	zkConn      *zk.Conn
	unsubscribe chan bool
	closed      bool
	watches     map[string]*GroupWatch
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
		watches:     make(map[string]*GroupWatch),
	}
}

/* Establish connection to this ConsumerCoordinator. Returns an error if fails to connect, nil otherwise. */
func (this *ZookeeperCoordinator) Connect() (err error) {
	var connectionEvents <-chan zk.Event
	for i := 0; i <= this.config.MaxRequestRetries; i++ {
		this.zkConn, connectionEvents, err = this.tryConnect()
		if err == nil {
			go this.listenConnectionEvents(connectionEvents)
			return
		}
		Tracef(this, "Zookeeper connect failed after %d-th retry", i)
		time.Sleep(this.config.RequestBackoff)
	}

	return
}

func (this *ZookeeperCoordinator) tryConnect() (zkConn *zk.Conn, connectionEvents <-chan zk.Event, err error) {
	Infof(this, "Connecting to ZK at %s\n", this.config.ZookeeperConnect)
	zkConn, connectionEvents, err = zk.Connect(this.config.ZookeeperConnect, this.config.ZookeeperTimeout)
	return
}

func (this *ZookeeperCoordinator) Disconnect() {
	Infof(this, "Closing connection to ZK at %s\n", this.config.ZookeeperConnect)
	this.closed = true
	this.zkConn.Close()
}

func (this *ZookeeperCoordinator) listenConnectionEvents(connectionEvents <-chan zk.Event) {
	for event := range connectionEvents {
		if this.closed {
			return
		}

		if event.State == zk.StateExpired && event.Type == zk.EventSession {
			err := this.Connect()
			if err != nil {
				this.config.PanicHandler(err)
			}
			for groupId, watch := range this.watches {
				watch.zkEvents <- zk.Event{
					Type:  zk.EventNodeCreated,
					State: zk.StateConnected,
					Path:  watch.poisonPillMessage,
				}
				_, err := this.SubscribeForChanges(groupId)
				if err != nil {
					this.config.PanicHandler(err)
				}
				watch.coordinatorEvents <- Reinitialize
			}

			return
		}
	}
}

/* Registers a new consumer with Consumerid id and TopicCount subscription that is a part of consumer group Groupid in this ConsumerCoordinator. Returns an error if registration failed, nil otherwise. */
func (this *ZookeeperCoordinator) RegisterConsumer(Consumerid string, Groupid string, TopicCount TopicsToNumStreams) (err error) {
	backoffMultiplier := 1
	this.ensureZkPathsExist(Groupid)
	for i := 0; i <= this.config.MaxRequestRetries; i++ {
		err = this.tryRegisterConsumer(Consumerid, Groupid, TopicCount)
		if err == nil {
			return
		}
		Tracef(this, "Registering consumer %s in group %s failed after %d-th retry", Consumerid, Groupid, i)
		time.Sleep(this.config.RequestBackoff * time.Duration(backoffMultiplier))
		backoffMultiplier++
	}
	return
}

func (this *ZookeeperCoordinator) tryRegisterConsumer(Consumerid string, Groupid string, TopicCount TopicsToNumStreams) (err error) {
	Debugf(this, "Trying to register consumer %s at group %s in Zookeeper", Consumerid, Groupid)
	registryDir := newZKGroupDirs(this.config.Root, Groupid).ConsumerRegistryDir
	pathToConsumer := fmt.Sprintf("%s/%s", registryDir, Consumerid)
	data, mappingError := json.Marshal(&ConsumerInfo{
		Version:      int16(1),
		Subscription: TopicCount.GetTopicsToNumStreamsMap(),
		Pattern:      TopicCount.Pattern(),
		Timestamp:    time.Now().Unix() * 1000,
	})
	if mappingError != nil {
		return mappingError
	}

	Debugf(this, "Path: %s", pathToConsumer)

	_, err = this.zkConn.Create(pathToConsumer, data, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	if err == zk.ErrNoNode {
		err = this.createOrUpdatePathParentMayNotExistFailFast(registryDir, make([]byte, 0))
		if err != nil {
			return
		}
		_, err = this.zkConn.Create(pathToConsumer, data, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	} else if err == zk.ErrNodeExists {
		var stat *zk.Stat
		_, stat, err = this.zkConn.Get(pathToConsumer)
		if err != nil {
			Debugf(this, "%v; path: %s", err, pathToConsumer)
			return err
		}
		_, err = this.zkConn.Set(pathToConsumer, data, stat.Version)
		if err != nil {
			Debugf(this, "%v; path: %s", err, pathToConsumer)
			return err
		}
	}

	return
}

/* Deregisters consumer with Consumerid id that is a part of consumer group Groupid form this ConsumerCoordinator. Returns an error if deregistration failed, nil otherwise. */
func (this *ZookeeperCoordinator) DeregisterConsumer(Consumerid string, Groupid string) (err error) {
	path := fmt.Sprintf("%s/%s", newZKGroupDirs(this.config.Root, Groupid).ConsumerRegistryDir, Consumerid)
	Debugf(this, "Trying to deregister consumer at path: %s", path)
	backoffMultiplier := 1
	for i := 0; i <= this.config.MaxRequestRetries; i++ {
		err = this.deleteNode(path)
		if err == nil {
			return
		}
		Tracef(this, "Deregistering consumer %s in group %s failed after %d-th retry", Consumerid, Groupid, i)
		time.Sleep(this.config.RequestBackoff * time.Duration(backoffMultiplier))
		backoffMultiplier++
	}
	return
}

// Gets the information about consumer with Consumerid id that is a part of consumer group Groupid from this ConsumerCoordinator.
// Returns ConsumerInfo on success and error otherwise (For example if consumer with given Consumerid does not exist).
func (this *ZookeeperCoordinator) GetConsumerInfo(Consumerid string, Groupid string) (info *ConsumerInfo, err error) {
	backoffMultiplier := 1
	for i := 0; i <= this.config.MaxRequestRetries; i++ {
		info, err = this.tryGetConsumerInfo(Consumerid, Groupid)
		if err == nil {
			return
		}
		Tracef(this, "GetConsumerInfo failed for consumer %s in group %s after %d-th retry", Consumerid, Groupid, i)
		time.Sleep(this.config.RequestBackoff * time.Duration(backoffMultiplier))
		backoffMultiplier++
	}
	return
}

func (this *ZookeeperCoordinator) tryGetConsumerInfo(Consumerid string, Groupid string) (*ConsumerInfo, error) {
	zkPath := fmt.Sprintf("%s/%s", newZKGroupDirs(this.config.Root, Groupid).ConsumerRegistryDir, Consumerid)
	data, _, err := this.zkConn.Get(zkPath)
	if err != nil {
		Debugf(this, "%v; path: %s", err, zkPath)
		return nil, err
	}

	type consumerInfoTmp struct {
		Version      int16
		Subscription map[string]int
		Pattern      string
		Timestamp    json.RawMessage
	}
	tmpInfo := &consumerInfoTmp{}
	err = json.Unmarshal(data, tmpInfo)

	if err != nil {
		return nil, fmt.Errorf("%v Path: %s, Data: %s", err, zkPath, string(data))
	}

	ts, convErr := fixTimestamp(tmpInfo.Timestamp)
	if convErr != nil {
		return nil, fmt.Errorf("%v Path: %s, Data: %s", err, zkPath, string(data))
	}
	consumerInfo := &ConsumerInfo{Version: tmpInfo.Version, Subscription: tmpInfo.Subscription, Pattern: tmpInfo.Pattern, Timestamp: ts}
	return consumerInfo, nil
}

func fixTimestamp(b json.RawMessage) (int64, error) {
	var s string
	var i int64
	var err error
	err = json.Unmarshal(b, &s)
	if err == nil {
		var n int64
		n, err = strconv.ParseInt(s, 10, 64)
		if err == nil {
			return n, nil
		}
	}
	err = json.Unmarshal(b, &i)
	if err == nil {
		return i, nil
	}
	return 0, fmt.Errorf("Unable to convert raw value %+v to int64", b)
}

// Gets the information about consumers per topic in consumer group Groupid excluding internal topics (such as offsets) if ExcludeInternalTopics = true.
// Returns a map where keys are topic names and values are slices of consumer ids and fetcher ids associated with this topic and error on failure.
func (this *ZookeeperCoordinator) GetConsumersPerTopic(Groupid string, ExcludeInternalTopics bool) (consumers map[string][]ConsumerThreadId, err error) {
	backoffMultiplier := 1
	for i := 0; i <= this.config.MaxRequestRetries; i++ {
		consumers, err = this.tryGetConsumersPerTopic(Groupid, ExcludeInternalTopics)
		if err == nil {
			return
		}
		Tracef(this, "GetConsumersPerTopic failed for group %s after %d-th retry", Groupid, i)
		time.Sleep(this.config.RequestBackoff * time.Duration(backoffMultiplier))
		backoffMultiplier++
	}
	return
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
func (this *ZookeeperCoordinator) GetConsumersInGroup(Groupid string) (consumers []string, err error) {
	backoffMultiplier := 1
	for i := 0; i <= this.config.MaxRequestRetries; i++ {
		consumers, err = this.tryGetConsumersInGroup(Groupid)
		if err == nil {
			return
		}
		Tracef(this, "GetConsumersInGroup failed for group %s after %d-th retry", Groupid, i)
		time.Sleep(this.config.RequestBackoff * time.Duration(backoffMultiplier))
		backoffMultiplier++
	}
	return
}

func (this *ZookeeperCoordinator) tryGetConsumersInGroup(Groupid string) (consumers []string, err error) {
	Debugf(this, "Getting consumers in group %s", Groupid)
	zkPath := newZKGroupDirs(this.config.Root, Groupid).ConsumerRegistryDir
	consumers, _, err = this.zkConn.Children(zkPath)
	if err != nil {
		Debugf(this, "%v; path: %s", err, zkPath)
		return nil, err
	}
	return
}

/* Gets the list of all topics registered in this ConsumerCoordinator. Returns a slice conaining topic names and error on failure. */
func (this *ZookeeperCoordinator) GetAllTopics() (topics []string, err error) {
	backoffMultiplier := 1
	for i := 0; i <= this.config.MaxRequestRetries; i++ {
		topics, err = this.tryGetAllTopics()
		if err == nil {
			return
		}
		Tracef(this, "GetAllTopics failed after %d-th retry", i)
		time.Sleep(this.config.RequestBackoff * time.Duration(backoffMultiplier))
		backoffMultiplier++
	}
	return
}

func (this *ZookeeperCoordinator) rootedPath(path string) string {
	return this.config.Root + path
}

func (this *ZookeeperCoordinator) tryGetAllTopics() (topics []string, err error) {
	zkPath := this.rootedPath(brokerTopicsPath)
	topics, _, err = this.zkConn.Children(zkPath)
	if err != nil {
		Debugf(this, "%v; path: %s", err, zkPath)
		return nil, err
	}
	return
}

// Gets the information about existing partitions for a given Topics.
// Returns a map where keys are topic names and values are slices of partition ids associated with this topic and error on failure.
func (this *ZookeeperCoordinator) GetPartitionsForTopics(Topics []string) (partitions map[string][]int32, err error) {
	backoffMultiplier := 1
	for i := 0; i <= this.config.MaxRequestRetries; i++ {
		partitions, err = this.tryGetPartitionsForTopics(Topics)
		if err == nil {
			return
		}
		Tracef(this, "GetPartitionsForTopics for topics %s failed after %d-th retry", Topics, i)
		time.Sleep(this.config.RequestBackoff * time.Duration(backoffMultiplier))
		backoffMultiplier++
	}
	return
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
func (this *ZookeeperCoordinator) GetAllBrokers() (brokers []*BrokerInfo, err error) {
	backoffMultiplier := 1
	for i := 0; i <= this.config.MaxRequestRetries; i++ {
		brokers, err = this.tryGetAllBrokers()
		if err == nil {
			return
		}
		Tracef(this, "GetAllBrokers failed after %d-th retry", i)
		time.Sleep(this.config.RequestBackoff * time.Duration(backoffMultiplier))
		backoffMultiplier++
	}
	return
}

func (this *ZookeeperCoordinator) tryGetAllBrokers() ([]*BrokerInfo, error) {
	Debug(this, "Getting all brokers in cluster")
	zkPath := this.rootedPath(brokerIdsPath)
	brokerIds, _, err := this.zkConn.Children(zkPath)
	if err != nil {
		Debugf(this, "%v; path: %s", err, zkPath)
		return nil, err
	}
	brokers := make([]*BrokerInfo, len(brokerIds))
	for i, brokerId := range brokerIds {
		brokerIdNum, err := strconv.Atoi(brokerId)
		if err != nil {
			return nil, err
		}

		brokers[i], err = this.getBrokerInfo(int32(brokerIdNum))
		if err != nil {
			return nil, err
		}
		brokers[i].Id = int32(brokerIdNum)
	}

	return brokers, nil
}

// Gets the offset for a given topic, partition and consumer group.
// Returns offset on sucess, error otherwise.
func (this *ZookeeperCoordinator) GetOffset(Groupid string, topic string, partition int32) (offset int64, err error) {
	backoffMultiplier := 1
	for i := 0; i <= this.config.MaxRequestRetries; i++ {
		offset, err = this.tryGetOffsetForTopicPartition(Groupid, topic, partition)
		if err == nil {
			return
		}
		Tracef(this, "GetOffset for group %s, topic %s and partition %d failed after %d-th retry", Groupid, topic, partition, i)
		time.Sleep(this.config.RequestBackoff * time.Duration(backoffMultiplier))
		backoffMultiplier++
	}
	return
}

func (this *ZookeeperCoordinator) tryGetOffsetForTopicPartition(Groupid string, topic string, partition int32) (int64, error) {
	dirs := newZKGroupTopicDirs(this.config.Root, Groupid, topic)
	zkPath := fmt.Sprintf("%s/%d", dirs.ConsumerOffsetDir, partition)
	offset, _, err := this.zkConn.Get(zkPath)
	if err != nil {
		if err == zk.ErrNoNode {
			return InvalidOffset, nil
		} else {
			Debugf(this, "%v; path: %s", err, zkPath)
			return InvalidOffset, err
		}
	}

	offsetNum, err := strconv.Atoi(string(offset))
	if err != nil {
		return InvalidOffset, err
	}

	return int64(offsetNum), nil
}

// Subscribes for any change that should trigger consumer rebalance on consumer group Groupid in this ConsumerCoordinator.
// Returns a read-only channel of booleans that will get values on any significant coordinator event (e.g. new consumer appeared, new broker appeared etc.) and error if failed to subscribe.
func (this *ZookeeperCoordinator) SubscribeForChanges(Groupid string) (events <-chan CoordinatorEvent, err error) {
	backoffMultiplier := 1
	for i := 0; i <= this.config.MaxRequestRetries; i++ {
		events, err = this.trySubscribeForChanges(Groupid)
		if err == nil {
			return
		}
		Tracef(this, "SubscribeForChanges for group %s failed after %d-th retry", Groupid, i)
		time.Sleep(this.config.RequestBackoff * time.Duration(backoffMultiplier))
		backoffMultiplier++
	}
	return
}

func (this *ZookeeperCoordinator) trySubscribeForChanges(Groupid string) (<-chan CoordinatorEvent, error) {
	var groupWatch *GroupWatch
	if _, ok := this.watches[Groupid]; !ok {
		groupWatch = &GroupWatch{
			coordinatorEvents: make(chan CoordinatorEvent, 100),
			poisonPillMessage: uuid(),
		}
		this.watches[Groupid] = groupWatch
	} else {
		groupWatch = this.watches[Groupid]
	}

	Infof(this, "Subscribing for changes for %s", Groupid)
	zkEvents := make(chan zk.Event, 100)
	this.watches[Groupid].zkEvents = zkEvents

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
	stopRedirecting := redirectChannelsTo(inputChannels, zkEvents)

	go func() {
		for {
			select {
			case e := <-zkEvents:
				{
					Infof(this, "Received zkEvent Type: %s State: %s Path: %s", e.Type.String(), e.State.String(), e.Path)
					if e.Type != zk.EventNotWatching && e.State != zk.StateDisconnected {
						if strings.HasPrefix(e.Path, fmt.Sprintf("%s/%s",
							newZKGroupDirs(this.config.Root, Groupid).ConsumerApiDir, BlueGreenDeploymentAPI)) {
							groupWatch.coordinatorEvents <- BlueGreenRequest
						} else if e.Path == groupWatch.poisonPillMessage {
							stopRedirecting <- true
							return
						} else {
							groupWatch.coordinatorEvents <- Regular
						}
					}

					if strings.HasPrefix(e.Path, newZKGroupDirs(this.config.Root, Groupid).ConsumerRegistryDir) {
						Info(this, "Trying to renew watcher for consumer registry")
						consumersWatcher, err = this.getConsumersInGroupWatcher(Groupid)
						if err != nil {
							this.config.PanicHandler(err)
						}
					} else if strings.HasPrefix(e.Path, fmt.Sprintf("%s/%s", newZKGroupDirs(this.config.Root, Groupid).ConsumerApiDir, BlueGreenDeploymentAPI)) {
						Info(this, "Trying to renew watcher for consumer API dir")
						blueGreenWatcher, err = this.getBlueGreenWatcher(Groupid)
						if err != nil {
							this.config.PanicHandler(err)
						}
					} else if strings.HasPrefix(e.Path, this.rootedPath(brokerTopicsPath)) {
						Info(this, "Trying to renew watcher for consumer topic dir")
						topicsWatcher, err = this.getTopicsWatcher()
						if err != nil {
							this.config.PanicHandler(err)
						}
					} else if strings.HasPrefix(e.Path, this.rootedPath(brokerIdsPath)) {
						Info(this, "Trying to renew watcher for brokers in cluster")
						brokersWatcher, err = this.getAllBrokersInClusterWatcher()
						if err != nil {
							this.config.PanicHandler(err)
						}
					} else {
						Warnf(this, "Unknown event path: %s", e.Path)
					}

					stopRedirecting <- true
					stopRedirecting = redirectChannelsTo(inputChannels, zkEvents)
				}
			case <-this.unsubscribe:
				{
					stopRedirecting <- true
					return
				}
			}
		}
	}()

	return groupWatch.coordinatorEvents, nil
}

// Gets all deployed topics for consume group Group from consumer coordinator.
// Returns a map where keys are notification ids and values are DeployedTopics. May also return an error (e.g. if failed to reach coordinator).
func (this *ZookeeperCoordinator) GetBlueGreenRequest(Group string) (topics map[string]*BlueGreenDeployment, err error) {
	backoffMultiplier := 1
	for i := 0; i <= this.config.MaxRequestRetries; i++ {
		topics, err = this.tryGetBlueGreenRequest(Group)
		if err == nil {
			return
		}
		Tracef(this, "GetNewDeployedTopics for group %s failed after %d-th retry", Group, i)
		time.Sleep(this.config.RequestBackoff * time.Duration(backoffMultiplier))
		backoffMultiplier++
	}
	return
}

func (this *ZookeeperCoordinator) tryGetBlueGreenRequest(Group string) (map[string]*BlueGreenDeployment, error) {
	apiPath := fmt.Sprintf("%s/%s", newZKGroupDirs(this.config.Root, Group).ConsumerApiDir, BlueGreenDeploymentAPI)
	children, _, err := this.zkConn.Children(apiPath)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Unable to get new deployed topics %s: %s", err.Error(), apiPath))
	}

	deployedTopics := make(map[string]*BlueGreenDeployment)
	for _, child := range children {
		entryPath := fmt.Sprintf("%s/%s", apiPath, child)
		rawDeployedTopicsEntry, _, err := this.zkConn.Get(entryPath)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("Unable to fetch deployed topic entry %s: %s", err.Error(), entryPath))
		}
		deployedTopicsEntry := &BlueGreenDeployment{}
		err = json.Unmarshal(rawDeployedTopicsEntry, deployedTopicsEntry)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("Unable to parse deployed topic entry %s: %s", err.Error(), rawDeployedTopicsEntry))
		}

		deployedTopics[child] = deployedTopicsEntry
	}

	return deployedTopics, nil
}

func (this *ZookeeperCoordinator) RequestBlueGreenDeployment(blue BlueGreenDeployment, green BlueGreenDeployment) error {
	var err error
	backoffMultiplier := 1
	for i := 0; i <= this.config.MaxRequestRetries; i++ {
		err := this.tryRequestBlueGreenDeployment(green.Group, blue)
		if err == nil {
			break
		}
		Tracef(this, "DeployTopics for group %s and topics %s failed after %d-th retry", green.Group, blue.Topics, i)
		time.Sleep(this.config.RequestBackoff * time.Duration(backoffMultiplier))
		backoffMultiplier++
	}

	if err != nil {
		return err
	}

	backoffMultiplier = 1
	for i := 0; i <= this.config.MaxRequestRetries; i++ {
		err := this.tryRequestBlueGreenDeployment(blue.Group, green)
		if err == nil {
			return err
		}
		Tracef(this, "DeployTopics for group %s and topics %s failed after %d-th retry", blue.Group, green.Topics, i)
		time.Sleep(this.config.RequestBackoff * time.Duration(backoffMultiplier))
		backoffMultiplier++
	}

	return err
}

func (this *ZookeeperCoordinator) tryRequestBlueGreenDeployment(Group string, blueOrGreen BlueGreenDeployment) error {
	data, err := json.Marshal(blueOrGreen)
	if err != nil {
		return err
	}
	return this.createOrUpdatePathParentMayNotExistFailFast(fmt.Sprintf("%s/%s/%d", newZKGroupDirs(this.config.Root, Group).ConsumerApiDir, BlueGreenDeploymentAPI, time.Now().Unix()), data)
}

func (this *ZookeeperCoordinator) RemoveOldApiRequests(group string) (err error) {
	for _, api := range availableAPIs {
		err = this.tryRemoveOldApiRequests(group, api)
		if err != nil {
			break
		}
	}
	return
}

func (this *ZookeeperCoordinator) tryRemoveOldApiRequests(group string, api ConsumerGroupApi) error {
	requests := make([]string, 0)
	var err error

	apiPath := fmt.Sprintf("%s/%s", newZKGroupDirs(this.config.Root, group).ConsumerApiDir, api)
	for i := 0; i <= this.config.MaxRequestRetries; i++ {
		requests, _, err = this.zkConn.Children(apiPath)
		if err != nil {
			continue
		}
		for _, request := range requests {
			var data []byte
			var t int64
			childPath := fmt.Sprintf("%s/%s", apiPath, request)
			if api == Rebalance {
				if data, _, err = this.zkConn.Get(childPath); err != nil && err == zk.ErrNoNode {
					// It's possible another consumer deleted the node before we could read it's data
					continue
				}
				if t, err = strconv.ParseInt(string(data), 10, 64); err != nil {
					t = int64(0) // If the data isn't a timestamp ensure it will be deleted anyway.
				}
			} else if api == BlueGreenDeploymentAPI {
				if t, err = strconv.ParseInt(string(request), 10, 64); err != nil {
					break
				}
			}

			// Delete if this zk node has an expired timestamp
			if time.Unix(t, 0).Before(time.Now().Add(-10 * time.Minute)) {
				// If the data is not a timestamp or is a timestamp but has reached expiration delete it
				err = this.deleteNode(childPath)
				if err != nil && err != zk.ErrNoNode {
					break
				}
			}
		}
	}

	return err
}

func (this *ZookeeperCoordinator) AwaitOnStateBarrier(consumerId string, group string, barrierName string,
	barrierSize int, api string, timeout time.Duration) bool {
	barrierPath := fmt.Sprintf("%s/%s/%s", newZKGroupDirs(this.config.Root, group).ConsumerApiDir, api, barrierName)

	var barrierExpiration time.Time
	var err error
	// Block and wait for this to consumerId to join the state barrier
	if barrierExpiration, err = this.joinStateBarrier(barrierPath, consumerId, timeout); err == nil {
		// Now that we've joined the barrier wait to verify all consumers have reached consensus.
		barrierTimeout := barrierExpiration.Sub(time.Now())
		err = this.waitForMembersToJoin(barrierPath, barrierSize, barrierTimeout)
	}

	if err != nil {
		// Encountered an error waiting for consensus... Fail it
		Errorf(this, "Failed awaiting on state barrier %s [%v]", barrierName, err)
		return false
	}

	Infof(this, "Successfully awaited on state barrier %s", barrierName)
	return true
}

func (this *ZookeeperCoordinator) joinStateBarrier(barrierPath, consumerId string, timeout time.Duration) (time.Time, error) {
	deadline := time.Now().Add(timeout)
	var err error
	Infof(this, "Joining state barrier %s", barrierPath)
	for i := 0; i <= this.config.MaxRequestRetries; i++ {
		// Attempt to create the barrier path, with a shared deadline
		_, err = this.zkConn.Create(barrierPath, []byte(strconv.FormatInt(deadline.Unix(), 10)), 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			if err != zk.ErrNodeExists {
				continue
			}
			// If the barrier path already exists, read it's value
			if data, _, err := this.zkConn.Get(barrierPath); err == nil {
				deadlineInt, _ := strconv.ParseInt(string(data), 10, 64)
				deadline = time.Unix(deadlineInt, 0)
				Infof(this, "Barrier already exists with deadline set to %v. Joining...", deadline)
			} else {
				continue
			}
		}
		// Register our consumerId as a child node on the barrierPath. This should notify other consumers we have joined.
		// Need to join as an ephemeral node to ensure that if the barrier Id is re-used we aren't permanently registered giving false counts.
		if _, err = this.zkConn.Create(fmt.Sprintf("%s/%s", barrierPath, consumerId), make([]byte, 0), zk.FlagEphemeral, zk.WorldACL(zk.PermAll)); err == nil || err == zk.ErrNodeExists {
			Infof(this, "Successfully joined state barrier %s", barrierPath)
			return deadline, nil
		}
		Warnf(this, "Failed to join state barrier %s, retrying...", barrierPath)
	}
	return time.Now(), fmt.Errorf("Failed to join state barrier %s after %d retries [%v]", barrierPath, this.config.MaxRequestRetries, err)
}

func (this *ZookeeperCoordinator) waitForMembersToJoin(barrierPath string, expected int, timeout time.Duration) error {
	// Will be used to make sure we don't leave the zk watcher channel without someone to receive events off it.
	blackholeFunc := func(blackhole <-chan zk.Event) {
		<-blackhole
	}

	t := time.NewTimer(timeout)
	defer t.Stop()
	for {
		select {
		// Using a priority select to provide precedence to the timeout
		case <-t.C:
			return fmt.Errorf("Timed out waiting for consensus on barrier path %s", barrierPath)
		default:
			children, _, zkMemberJoinedWatcher, err := this.zkConn.ChildrenW(barrierPath)
			if err != nil && err == zk.ErrNoNode {
				return fmt.Errorf("%v; path: %s", err, barrierPath)
			} else if len(children) == expected {
				// don't leave the zkMemberJoinedWatcher chan out there with no one to receive the message it produces later as it would cause a block.
				go blackholeFunc(zkMemberJoinedWatcher)
				return nil
			}
			// Haven't seen all expected consumers on this barrier path.  Watch for changes to the path...
			select {
			case <-t.C:
				go blackholeFunc(zkMemberJoinedWatcher)
				return fmt.Errorf("Timed out waiting for consensus on barrier path %s", barrierPath)
			case <-zkMemberJoinedWatcher:
				continue
			}
		}
	}

	return nil
}

func (this *ZookeeperCoordinator) RemoveStateBarrier(group string, stateHash string, api string) error {
	var err error
	backoffMultiplier := 1
	for i := 0; i <= this.config.MaxRequestRetries; i++ {
		err = this.tryRemoveStateBarrier(group, stateHash, api)
		if err == nil || err == zk.ErrNoNode {
			return nil
		}
		Tracef(this, "State assertion deletion %s in group %s failed after %d-th retry", hash, group, i)
		time.Sleep(this.config.RequestBackoff * time.Duration(backoffMultiplier))
		backoffMultiplier++
	}

	return err
}

func (this *ZookeeperCoordinator) tryRemoveStateBarrier(group string, stateHash string, api string) error {
	path := fmt.Sprintf("%s/%s/%s", newZKGroupDirs(this.config.Root, group).ConsumerApiDir, api, stateHash)
	Debugf(this, "Trying to fail rebalance at path: %s", path)

	return this.deleteNode(path)
}

func (this *ZookeeperCoordinator) deleteNode(path string) error {
	children, _, err := this.zkConn.Children(path)
	if err != nil {
		Debugf(this, "%v; path: %s", err, path)
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
		Debugf(this, "%v; path: %s", err, path)
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
	backoffMultiplier := 1
	for i := 0; i <= this.config.MaxRequestRetries; i++ {
		ok, err := this.tryClaimPartitionOwnership(Groupid, Topic, Partition, consumerThreadId)
		if ok {
			return ok, err
		}
		Tracef(this, "Claim failed for topic %s, partition %d after %d-th retry", Topic, Partition, i)
		time.Sleep(this.config.RequestBackoff * time.Duration(backoffMultiplier))
		backoffMultiplier++
	}
	return false, err
}

func (this *ZookeeperCoordinator) tryClaimPartitionOwnership(group string, topic string, partition int32, consumerThreadId ConsumerThreadId) (bool, error) {
	dirs := newZKGroupTopicDirs(this.config.Root, group, topic)
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
			var data []byte
			if data, _, err = this.zkConn.Get(pathToOwn); err == nil && string(data) == consumerThreadId.String() {
				// If the current owner of the partition is the same consumer Id as the current one, carry on.
				return true, nil
			}
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
	backoffMultiplier := 1
	for i := 0; i <= this.config.MaxRequestRetries; i++ {
		err = this.tryReleasePartitionOwnership(Groupid, Topic, Partition)
		if err == nil {
			return err
		}
		Tracef(this, "ReleasePartitionOwnership failed for group %s, topic %s, partition %d after %d-th retry", Groupid, Topic, Partition, i)
		time.Sleep(this.config.RequestBackoff * time.Duration(backoffMultiplier))
		backoffMultiplier++
	}
	return err
}

func (this *ZookeeperCoordinator) tryReleasePartitionOwnership(group string, topic string, partition int32) error {
	path := fmt.Sprintf("%s/%d", newZKGroupTopicDirs(this.config.Root, group, topic).ConsumerOwnerDir, partition)
	err := this.deleteNode(path)
	if err != nil && err != zk.ErrNoNode {
		return err
	} else {
		return nil
	}
}

// Tells the ConsumerCoordinator to commit offset Offset for topic and partition TopicPartition for consumer group Groupid.
// Returns error if failed to commit offset.
func (this *ZookeeperCoordinator) CommitOffset(Groupid string, Topic string, Partition int32, Offset int64) error {
	dirs := newZKGroupTopicDirs(this.config.Root, Groupid, Topic)
	err := this.updateRecord(fmt.Sprintf("%s/%d", dirs.ConsumerOffsetDir, Partition), []byte(strconv.FormatInt(Offset, 10)))
	if err == zk.ErrNoNode {
		return this.createOrUpdatePathParentMayNotExistFailFast(fmt.Sprintf("%s/%d", dirs.ConsumerOffsetDir, Partition), []byte(strconv.FormatInt(Offset, 10)))
	}

	return err
}

func (this *ZookeeperCoordinator) ensureZkPathsExist(group string) {
	dirs := newZKGroupDirs(this.config.Root, group)
	this.createOrUpdatePathParentMayNotExistFailSafe(dirs.ConsumerDir, make([]byte, 0))
	this.createOrUpdatePathParentMayNotExistFailSafe(dirs.ConsumerGroupDir, make([]byte, 0))
	this.createOrUpdatePathParentMayNotExistFailSafe(dirs.ConsumerRegistryDir, make([]byte, 0))
	this.createOrUpdatePathParentMayNotExistFailSafe(dirs.ConsumerApiDir, make([]byte, 0))
	for _, api := range availableAPIs {
		this.createOrUpdatePathParentMayNotExistFailSafe(fmt.Sprintf("%s/%s", dirs.ConsumerApiDir, api), make([]byte, 0))
	}
}

func (this *ZookeeperCoordinator) getWatcher(path string) (<-chan zk.Event, error) {
	Debugf(this, "Getting watcher for %s", path)

	var watcher <-chan zk.Event
	var err error
	backoffMultiplier := 1
	for i := 0; i <= this.config.MaxRequestRetries; i++ {
		_, _, watcher, err = this.zkConn.ChildrenW(path)
		if err == nil {
			return watcher, err
		}
		Debugf(this, "%v; path: %s", err, path)
		time.Sleep(this.config.RequestBackoff * time.Duration(backoffMultiplier))
		backoffMultiplier++
	}

	return nil, err
}

func (this *ZookeeperCoordinator) getAllBrokersInClusterWatcher() (<-chan zk.Event, error) {
	return this.getWatcher(this.rootedPath(brokerIdsPath))
}

func (this *ZookeeperCoordinator) getConsumersInGroupWatcher(group string) (<-chan zk.Event, error) {
	return this.getWatcher(newZKGroupDirs(this.config.Root, group).ConsumerRegistryDir)
}

func (this *ZookeeperCoordinator) getBlueGreenWatcher(group string) (<-chan zk.Event, error) {
	return this.getWatcher(fmt.Sprintf("%s/%s", newZKGroupDirs(this.config.Root, group).ConsumerApiDir, BlueGreenDeploymentAPI))
}

func (this *ZookeeperCoordinator) getTopicsWatcher() (<-chan zk.Event, error) {
	return this.getWatcher(this.rootedPath(brokerTopicsPath))
}

func (this *ZookeeperCoordinator) getBrokerInfo(brokerId int32) (*BrokerInfo, error) {
	Debugf(this, "Getting info for broker %d", brokerId)
	pathToBroker := fmt.Sprintf("%s/%d", this.rootedPath(brokerIdsPath), brokerId)
	data, _, zkError := this.zkConn.Get(pathToBroker)
	if zkError != nil {
		Debugf(this, "%v; path: %s", zkError, pathToBroker)
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
	zkPath := fmt.Sprintf("%s/%s", this.rootedPath(brokerTopicsPath), topic)
	data, _, err := this.zkConn.Get(zkPath)
	if err != nil {
		Debugf(this, "%v; path: %s", err, zkPath)
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
			if len(parent) == 0 {
				return errors.New("Specified blank path")
			}
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
	if err != nil {
		return err
	}
	return nil
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

	/* kafka Root */
	Root string

	// PanicHandler is a function that will be called when unrecoverable error occurs to give the possibility to perform cleanups, recover from panic etc
	PanicHandler func(error)
}

/* Created a new ZookeeperConfig with sane defaults. Default ZookeeperConnect points to localhost. */
func NewZookeeperConfig() *ZookeeperConfig {
	config := &ZookeeperConfig{}
	config.ZookeeperConnect = []string{"localhost"}
	config.ZookeeperTimeout = 1 * time.Second
	config.MaxRequestRetries = 3
	config.RequestBackoff = 150 * time.Millisecond
	config.Root = ""
	config.PanicHandler = func(e error) {
		panic(e)
	}

	return config
}

// ZookeeperConfigFromFile is a helper function that loads zookeeper configuration information from file.
// The file accepts the following fields:
//  zookeeper.connect
//  zookeeper.kafka.root
//  zookeeper.connection.timeout
//  zookeeper.max.request.retries
//  zookeeper.request.backoff
// The configuration file entries should be constructed in key=value syntax. A # symbol at the beginning
// of a line indicates a comment. Blank lines are ignored. The file should end with a newline character.
func ZookeeperConfigFromFile(filename string) (*ZookeeperConfig, error) {
	z, err := LoadConfiguration(filename)
	if err != nil {
		return nil, err
	}

	config := NewZookeeperConfig()
	setStringSliceConfig(&config.ZookeeperConnect, z["zookeeper.connect"], ",")
	setStringConfig(&config.Root, z["zookeeper.kafka.root"])

	if err := setDurationConfig(&config.ZookeeperTimeout, z["zookeeper.connection.timeout"]); err != nil {
		return nil, err
	}
	if err := setIntConfig(&config.MaxRequestRetries, z["zookeeper.max.request.retries"]); err != nil {
		return nil, err
	}
	if err := setDurationConfig(&config.RequestBackoff, z["zookeeper.request.backoff"]); err != nil {
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

func newZKGroupDirs(root string, group string) *zkGroupDirs {
	consumerPathRooted := fmt.Sprintf("%s%s", root, consumersPath)
	consumerGroupDir := fmt.Sprintf("%s/%s", consumerPathRooted, group)
	consumerRegistryDir := fmt.Sprintf("%s/ids", consumerGroupDir)
	consumerApiDir := fmt.Sprintf("%s/api", consumerGroupDir)
	consumerRebalanceDir := fmt.Sprintf("%s/api/rebalance", consumerGroupDir)
	return &zkGroupDirs{
		Group:                group,
		ConsumerDir:          consumerPathRooted,
		ConsumerGroupDir:     consumerGroupDir,
		ConsumerRegistryDir:  consumerRegistryDir,
		ConsumerApiDir:       consumerApiDir,
		ConsumerRebalanceDir: consumerRebalanceDir,
	}
}

type zkGroupTopicDirs struct {
	ZkGroupDirs       *zkGroupDirs
	Topic             string
	ConsumerOffsetDir string
	ConsumerOwnerDir  string
}

func newZKGroupTopicDirs(root string, group string, topic string) *zkGroupTopicDirs {
	zkGroupsDirs := newZKGroupDirs(root, group)
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
func (mzk *mockZookeeperCoordinator) Disconnect()    { panic("Not implemented") }
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
func (mzk *mockZookeeperCoordinator) GetOffset(group string, topic string, partition int32) (int64, error) {
	panic("Not implemented")
}
func (mzk *mockZookeeperCoordinator) SubscribeForChanges(group string) (<-chan CoordinatorEvent, error) {
	panic("Not implemented")
}
func (mzk *mockZookeeperCoordinator) RequestBlueGreenDeployment(blue BlueGreenDeployment, green BlueGreenDeployment) error {
	panic("Not implemented")
}
func (mzk *mockZookeeperCoordinator) GetBlueGreenRequest(Group string) (map[string]*BlueGreenDeployment, error) {
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
func (mzk *mockZookeeperCoordinator) CommitOffset(group string, topic string, partition int32, offset int64) error {
	mzk.commitHistory[TopicAndPartition{topic, partition}] = offset
	return nil
}
func (this *mockZookeeperCoordinator) RemoveOldApiRequests(group string) error {
	panic("Not implemented")
}

func (this *mockZookeeperCoordinator) RemoveStateBarrier(group string, stateHash string, api string) error {
	panic("Not implemented")
}
