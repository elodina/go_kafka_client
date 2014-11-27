/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package go_kafka_client

import (
	"time"
	"github.com/samuel/go-zookeeper/zk"
	"sort"
	"fmt"
	"strconv"
	"encoding/json"
	"path"
)

var (
	consumersPath                      = "/consumers"
	brokerIdsPath                      = "/brokers/ids"
	brokerTopicsPath                   = "/brokers/topics"
)

type ZookeeperCoordinator struct {
	config *ZookeeperConfig
	zkConn *zk.Conn
	unsubscribe chan bool
}

func (zc *ZookeeperCoordinator) String() string {
	return "zk"
}

func NewZookeeperCoordinator(config *ZookeeperConfig) *ZookeeperCoordinator {
	return &ZookeeperCoordinator{
		config: config,
		unsubscribe: make(chan bool),
	}
}

func (zc *ZookeeperCoordinator) Connect() error {
	Infof(zc, "Connecting to ZK at %s\n", zc.config.ZookeeperConnect)
	conn, _, err := zk.Connect(zc.config.ZookeeperConnect, zc.config.ZookeeperTimeout)
	zc.zkConn = conn
	return err
}

func (zc *ZookeeperCoordinator) RegisterConsumer(consumerid string, groupid string, topicCount TopicsToNumStreams) error {
	Debugf(zc, "Trying to register consumer %s at group %s in Zookeeper", consumerid, groupid)
	pathToConsumer := fmt.Sprintf("%s/%s", newZKGroupDirs(groupid).ConsumerRegistryDir, consumerid)
	data, mappingError := json.Marshal(&ConsumerInfo{
					Version : int16(1),
					Subscription : topicCount.GetTopicsToNumStreamsMap(),
					Pattern : topicCount.Pattern(),
					Timestamp : time.Now().Unix(),
				})
	if mappingError != nil {
		return mappingError
	}

	Debugf(zc, "Path: %s", pathToConsumer)

	return zc.createOrUpdatePathParentMayNotExist(pathToConsumer, data)
}

func (zc *ZookeeperCoordinator) DeregisterConsumer(consumerid string, groupid string) error {
	pathToConsumer := fmt.Sprintf("%s/%s", newZKGroupDirs(groupid).ConsumerRegistryDir, consumerid)
	Debugf(zc, "Trying to deregister consumer at path: %s", pathToConsumer)
	_, stat, err := zc.zkConn.Get(pathToConsumer)
	if (err != nil) {
		return err
	}
	return zc.zkConn.Delete(pathToConsumer, stat.Version)
}

func (zc *ZookeeperCoordinator) GetConsumerInfo(consumerid string, group string) (*ConsumerInfo, error) {
	data, _, err := zc.zkConn.Get(fmt.Sprintf("%s/%s",
		newZKGroupDirs(group).ConsumerRegistryDir, consumerid))
	if (err != nil) {
		return nil, err
	}
	consumerInfo := &ConsumerInfo{}
	json.Unmarshal(data, consumerInfo)

	return consumerInfo, nil
}

func (zc *ZookeeperCoordinator) GetConsumersPerTopic(group string, excludeInternalTopics bool) (map[string][]ConsumerThreadId, error) {
	consumers, err := zc.GetConsumersInGroup(group)
	if (err != nil) {
		return nil, err
	}
	consumersPerTopicMap := make(map[string][]ConsumerThreadId)
	for _, consumer := range consumers {
		topicsToNumStreams, err := NewTopicsToNumStreams(group, consumer, zc, excludeInternalTopics)
		if (err != nil) {
			return nil, err
		}

		for topic, threadIds := range topicsToNumStreams.GetConsumerThreadIdsPerTopic() {
			for _, threadId := range threadIds {
				consumersPerTopicMap[topic] = append(consumersPerTopicMap[topic], threadId)
			}
		}
	}

	for topic := range consumersPerTopicMap {
		sort.Sort(ByName(consumersPerTopicMap[topic]))
	}

	return consumersPerTopicMap, nil
}

func (zc *ZookeeperCoordinator) GetConsumersInGroup(group string) ([]string, error) {
	Debugf(zc, "Getting consumers in group %s", group)
	consumers, _, err := zc.zkConn.Children(newZKGroupDirs(group).ConsumerRegistryDir)
	if (err != nil) {
		return nil, err
	}

	return consumers, nil
}

func (zc *ZookeeperCoordinator) GetAllTopics() ([]string, error) {
	topics, _, _, err := zc.zkConn.ChildrenW(brokerTopicsPath)
	return topics, err
}

func (zc *ZookeeperCoordinator) GetPartitionsForTopics(topics []string) (map[string][]int32, error) {
	result := make(map[string][]int32)
	partitionAssignments, err := zc.getPartitionAssignmentsForTopics(topics)
	if (err != nil) {
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

func (zc *ZookeeperCoordinator) GetAllBrokers() ([]*BrokerInfo, error) {
	Debug(zc, "Getting all brokers in cluster")
	brokerIds, _, err := zc.zkConn.Children(brokerIdsPath)
	if (err != nil) {
		return nil, err
	}
	brokers := make([]*BrokerInfo, len(brokerIds))
	for i, brokerId := range brokerIds {
		brokerIdNum, err := strconv.Atoi(brokerId)
		if (err != nil) {
			return nil, err
		}

		brokers[i], err = zc.getBrokerInfo(int32(brokerIdNum))
		brokers[i].Id = int32(brokerIdNum)
		if (err != nil) {
			return nil, err
		}
	}

	return brokers, nil
}

func (zc *ZookeeperCoordinator) GetOffsetForTopicPartition(group string, topicPartition *TopicAndPartition) (int64, error) {
	dirs := newZKGroupTopicDirs(group, topicPartition.Topic)
	offset, _, err := zc.zkConn.Get(fmt.Sprintf("%s/%d", dirs.ConsumerOffsetDir, topicPartition.Partition))
	if (err != nil) {
		if (err == zk.ErrNoNode) {
			return InvalidOffset, nil
		} else {
			return InvalidOffset, err
		}
	}

	offsetNum, err := strconv.Atoi(string(offset))
	if (err != nil) {
		return InvalidOffset, err
	}

	return int64(offsetNum), nil
}

func (zc *ZookeeperCoordinator) NotifyConsumerGroup(group string, consumerId string) error {
	path := fmt.Sprintf("%s/%s-%d", newZKGroupDirs(group).ConsumerChangesDir, consumerId, time.Now().Nanosecond())
	Debugf(zc, "Sending notification to consumer group at %s", path)
	return zc.createOrUpdatePathParentMayNotExist(path, make([]byte, 0))
}

func (zc *ZookeeperCoordinator) SubscribeForChanges(group string) (<-chan bool, error) {
	changes := make(chan bool)

	zc.ensureZkPathsExist(group)
	Infof(zc, "Subscribing for changes for %s", group)

	consumersWatcher, err := zc.getConsumersInGroupWatcher(group)
	if err != nil {
		return nil, err
	}
	consumerGroupChangesWatcher, err := zc.getConsumerGroupChangesWatcher(group)
	if err != nil {
		return nil, err
	}
	topicsWatcher, err := zc.getTopicsWatcher()
	if err != nil {
		return nil, err
	}
	brokersWatcher, err := zc.getAllBrokersInClusterWatcher()
	if err != nil {
		return nil, err
	}

	inputChannels := make([]*<-chan zk.Event, 0)
	inputChannels = append(inputChannels, &consumersWatcher, &consumerGroupChangesWatcher, &topicsWatcher, &brokersWatcher)
	zkEvents := make(chan zk.Event)
	stopRedirecting := RedirectChannelsTo(inputChannels, zkEvents)

	go func() {
		for {
			select {
				case e := <-zkEvents: {
					Trace(zc, e)
					if e.State == zk.StateDisconnected {
						Debug(zc, "ZK watcher session ended, reconnecting...")
						consumersWatcher, err = zc.getConsumersInGroupWatcher(group)
						if err != nil {
							panic(err)
						}
						consumerGroupChangesWatcher, err = zc.getConsumerGroupChangesWatcher(group)
						if err != nil {
							panic(err)
						}
						topicsWatcher, err = zc.getTopicsWatcher()
						if err != nil {
							panic(err)
						}
						brokersWatcher, err = zc.getAllBrokersInClusterWatcher()
						if err != nil {
							panic(err)
						}
					} else {
						emptyEvent := zk.Event{}
						if e != emptyEvent {
							changes <- true
						} else {
							//TODO configurable?
							time.Sleep(2 * time.Second)
						}
					}
				}
				case <-zc.unsubscribe: {
					stopRedirecting <- true
					return
				}
			}
		}
	}()

	return changes, nil
}

func (zc *ZookeeperCoordinator) Unsubscribe() {
	zc.unsubscribe <- true
}

func (zc *ZookeeperCoordinator) ClaimPartitionOwnership(group string, topic string, partition int32, consumerThreadId ConsumerThreadId) (bool, error) {
	dirs := newZKGroupTopicDirs(group, topic)
	zc.createOrUpdatePathParentMayNotExist(dirs.ConsumerOwnerDir, make([]byte, 0))

	pathToOwn := fmt.Sprintf("%s/%d", dirs.ConsumerOwnerDir, partition)
	_, err := zc.zkConn.Create(pathToOwn, []byte(consumerThreadId.String()), 0, zk.WorldACL(zk.PermAll))
	if (err != nil) {
		if (err == zk.ErrNodeExists) {
			Debugf(consumerThreadId, "waiting for the partition ownership to be deleted: %d", partition)
			return false, nil
		} else {
			Error(consumerThreadId, err)
			return false, err
		}
	}

	Debugf(zc, "Successfully claimed partition %d in topic %s for %s", partition, topic, consumerThreadId)

	return true, nil
}

func (zc *ZookeeperCoordinator) ReleasePartitionOwnership(group string, topic string, partition int32) error {
	err := zc.deletePartitionOwnership(group, topic, partition)
	if (err != nil) {
		if err == zk.ErrNoNode {
			Warn(zc, err)
			return nil
		} else {
			return err
		}
	}
	return nil
}

func (zc *ZookeeperCoordinator) CommitOffset(group string, topicPartition *TopicAndPartition, offset int64) error {
	dirs := newZKGroupTopicDirs(group, topicPartition.Topic)
	return zc.createOrUpdatePathParentMayNotExist(fmt.Sprintf("%s/%d", dirs.ConsumerOffsetDir, topicPartition.Partition), []byte(strconv.FormatInt(offset, 10)))
}

func (zc *ZookeeperCoordinator) ensureZkPathsExist(group string) {
	dirs := newZKGroupDirs(group)
	zc.createOrUpdatePathParentMayNotExist(dirs.ConsumerDir, make([]byte, 0))
	zc.createOrUpdatePathParentMayNotExist(dirs.ConsumerGroupDir, make([]byte, 0))
	zc.createOrUpdatePathParentMayNotExist(dirs.ConsumerRegistryDir, make([]byte, 0))
	zc.createOrUpdatePathParentMayNotExist(dirs.ConsumerChangesDir, make([]byte, 0))
}

func (zc *ZookeeperCoordinator) getAllBrokersInClusterWatcher() (<- chan zk.Event, error) {
	Debug(zc, "Subscribing for events from broker registry")
	_, _, watcher, err := zc.zkConn.ChildrenW(brokerIdsPath)
	if (err != nil) {
		return nil, err
	}

	return watcher, nil
}

func (zc *ZookeeperCoordinator) getConsumersInGroupWatcher(group string) (<- chan zk.Event, error) {
	Debugf(zc, "Getting consumer watcher for group %s", group)
	_, _, watcher, err := zc.zkConn.ChildrenW(newZKGroupDirs(group).ConsumerRegistryDir)
	if (err != nil) {
		return nil, err
	}

	return watcher, nil
}

func (zc *ZookeeperCoordinator) getConsumerGroupChangesWatcher(group string) (<- chan zk.Event, error) {
	Debugf(zc, "Getting watcher for consumer group '%s' changes", group)
	_, _, watcher, err := zc.zkConn.ChildrenW(newZKGroupDirs(group).ConsumerChangesDir)
	if (err != nil) {
		return nil, err
	}

	return watcher, nil
}

func (zc *ZookeeperCoordinator) getTopicsWatcher() (watcher <- chan zk.Event, err error) {
	_, _, watcher, err = zc.zkConn.ChildrenW(brokerTopicsPath)
	return
}

func (zc *ZookeeperCoordinator) getBrokerInfo(brokerId int32) (*BrokerInfo, error) {
	Debugf(zc, "Getting info for broker %d", brokerId)
	pathToBroker := fmt.Sprintf("%s/%d", brokerIdsPath, brokerId)
	data, _, zkError := zc.zkConn.Get(pathToBroker)
	if (zkError != nil) {
		return nil, zkError
	}

	broker := &BrokerInfo{}
	mappingError := json.Unmarshal([]byte(data), broker)

	return broker, mappingError
}

func (zc *ZookeeperCoordinator) getPartitionAssignmentsForTopics(topics []string) (map[string]map[int32][]int32, error) {
	Debugf(zc, "Trying to get partition assignments for topics %v", topics)
	result := make(map[string]map[int32][]int32)
	for _, topic := range topics {
		topicInfo, err := zc.getTopicInfo(topic)
		if (err != nil) {
			return nil, err
		}
		result[topic] = make(map[int32][]int32)
		for partition, replicaIds := range topicInfo.Partitions {
			partitionInt, err := strconv.Atoi(partition)
			if (err != nil) {
				return nil, err
			}
			result[topic][int32(partitionInt)] = replicaIds
		}
	}

	return result, nil
}

func (zc *ZookeeperCoordinator) getTopicInfo(topic string) (*TopicInfo, error) {
	data, _, err := zc.zkConn.Get(fmt.Sprintf("%s/%s", brokerTopicsPath, topic))
	if (err != nil) {
		return nil, err
	}
	topicInfo := &TopicInfo{}
	err = json.Unmarshal(data, topicInfo)
	if (err != nil) {
		return nil, err
	}

	return topicInfo, nil
}

func (zc *ZookeeperCoordinator) createOrUpdatePathParentMayNotExist(pathToCreate string, data []byte) error {
	Debugf(zc, "Trying to create path %s in Zookeeper", pathToCreate)
	_, err := zc.zkConn.Create(pathToCreate, data, 0, zk.WorldACL(zk.PermAll))
	if (err != nil) {
		if (zk.ErrNodeExists == err) {
			if (len(data) > 0) {
				Debugf(zc, "Trying to update existing node %s", pathToCreate)
				return zc.updateRecord(pathToCreate, data)
			} else {
				return nil
			}
		} else {
			parent, _ := path.Split(pathToCreate)
			err = zc.createOrUpdatePathParentMayNotExist(parent[:len(parent)-1], make([]byte, 0))
			if (err != nil) {
				Error(zc, err.Error())
				return err
			} else {
				Debugf(zc, "Successfully created path %s", parent[:len(parent)-1])
			}

			Debugf(zc, "Trying again to create path %s in Zookeeper", pathToCreate)
			_, err = zc.zkConn.Create(pathToCreate, data, 0, zk.WorldACL(zk.PermAll))
		}
	}

	return err
}

func (zc *ZookeeperCoordinator) deletePartitionOwnership(group string, topic string, partition int32) error {
	pathToDelete := fmt.Sprintf("%s/%d", newZKGroupTopicDirs(group, topic).ConsumerOwnerDir, partition)
	_, stat, err := zc.zkConn.Get(pathToDelete)
	if (err != nil) {
		return err
	}
	err = zc.zkConn.Delete(pathToDelete, stat.Version)
	if (err != nil) {
		return err
	}

	return nil
}

func (zc *ZookeeperCoordinator) updateRecord(pathToCreate string, dataToWrite []byte) error {
	Debugf(zc, "Trying to update path %s", pathToCreate)
	_, stat, _ := zc.zkConn.Get(pathToCreate)
	_, err := zc.zkConn.Set(pathToCreate, dataToWrite, stat.Version)

	return err
}

type ZookeeperConfig struct {
	/* Zookeeper hosts */
	ZookeeperConnect []string

	/** Zookeeper read timeout */
	ZookeeperTimeout time.Duration
}

func NewZookeeperConfig() *ZookeeperConfig {
	config := &ZookeeperConfig{}
	config.ZookeeperConnect = []string{"localhost"}
	config.ZookeeperTimeout = 1*time.Second

	return config
}

type zkGroupDirs struct {
	Group               string
	ConsumerDir         string
	ConsumerGroupDir    string
	ConsumerRegistryDir string
	ConsumerChangesDir	string
	ConsumerSyncDir	  string
}

func newZKGroupDirs(group string) *zkGroupDirs {
	consumerGroupDir := fmt.Sprintf("%s/%s", consumersPath, group)
	consumerRegistryDir := fmt.Sprintf("%s/ids", consumerGroupDir)
	consumerChangesDir := fmt.Sprintf("%s/changes", consumerGroupDir)
	consumerSyncDir := fmt.Sprintf("%s/sync", consumerGroupDir)
	return &zkGroupDirs {
		Group: group,
		ConsumerDir: consumersPath,
		ConsumerGroupDir: consumerGroupDir,
		ConsumerRegistryDir: consumerRegistryDir,
		ConsumerChangesDir: consumerChangesDir,
		ConsumerSyncDir: consumerSyncDir,
	}
}

type zkGroupTopicDirs struct {
	ZkGroupDirs *zkGroupDirs
	Topic             string
	ConsumerOffsetDir string
	ConsumerOwnerDir  string
}

func newZKGroupTopicDirs(group string, topic string) *zkGroupTopicDirs {
	zkGroupsDirs := newZKGroupDirs(group)
	return &zkGroupTopicDirs {
		ZkGroupDirs: zkGroupsDirs,
		Topic: topic,
		ConsumerOffsetDir: fmt.Sprintf("%s/%s/%s", zkGroupsDirs.ConsumerGroupDir, "offsets", topic),
		ConsumerOwnerDir: fmt.Sprintf("%s/%s/%s", zkGroupsDirs.ConsumerGroupDir, "owners", topic),
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
func (mzk *mockZookeeperCoordinator) RegisterConsumer(consumerid string, group string, topicCount TopicsToNumStreams) error { panic("Not implemented") }
func (mzk *mockZookeeperCoordinator) DeregisterConsumer(consumerid string, group string) error { panic("Not implemented") }
func (mzk *mockZookeeperCoordinator) GetConsumerInfo(consumerid string, group string) (*ConsumerInfo, error) { panic("Not implemented") }
func (mzk *mockZookeeperCoordinator) GetConsumersPerTopic(group string, excludeInternalTopics bool) (map[string][]ConsumerThreadId, error) { panic("Not implemented") }
func (mzk *mockZookeeperCoordinator) GetConsumersInGroup(group string) ([]string, error) { panic("Not implemented") }
func (mzk *mockZookeeperCoordinator) GetAllTopics() ([]string, error) { panic("Not implemented") }
func (mzk *mockZookeeperCoordinator) GetPartitionsForTopics(topics []string) (map[string][]int32, error) { panic("Not implemented") }
func (mzk *mockZookeeperCoordinator) GetAllBrokers() ([]*BrokerInfo, error) { panic("Not implemented") }
func (mzk *mockZookeeperCoordinator) GetOffsetForTopicPartition(group string, topicPartition *TopicAndPartition) (int64, error) { panic("Not implemented") }
func (mzk *mockZookeeperCoordinator) NotifyConsumerGroup(group string, consumerId string) error { panic("Not implemented") }
func (mzk *mockZookeeperCoordinator) SubscribeForChanges(group string) (<-chan bool, error) { panic("Not implemented") }
func (mzk *mockZookeeperCoordinator) Unsubscribe() { panic("Not implemented") }
func (mzk *mockZookeeperCoordinator) ClaimPartitionOwnership(group string, topic string, partition int32, consumerThreadId ConsumerThreadId) (bool, error) { panic("Not implemented") }
func (mzk *mockZookeeperCoordinator) ReleasePartitionOwnership(group string, topic string, partition int32) error { panic("Not implemented") }
func (mzk *mockZookeeperCoordinator) CommitOffset(group string, topicPartition *TopicAndPartition, offset int64) error {
	mzk.commitHistory[*topicPartition] = offset
	return nil
}
