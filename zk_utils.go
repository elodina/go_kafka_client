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
	"fmt"
	"encoding/json"
	"strconv"
	"path"
	"github.com/samuel/go-zookeeper/zk"
	"sort"
	"time"
)

var (
	ConsumersPath                      = "/consumers"
	BrokerIdsPath                      = "/brokers/ids"
	BrokerTopicsPath                   = "/brokers/topics"
	TopicConfigPath                    = "/config/topics"
	TopicConfigChangesPath             = "/config/changes"
	ControllerPath                     = "/controller"
	ControllerEpochPath                = "/controller_epoch"
	ReassignPartitionsPath             = "/admin/reassign_partitions"
	DeleteTopicsPath                   = "/admin/delete_topics"
	PreferredReplicaLeaderElectionPath = "/admin/preferred_replica_election"
)

func GetAllBrokersInCluster(zkConnection *zk.Conn) ([]*BrokerInfo, error) {
	Debug("zk", "Getting all brokers in cluster")
	brokerIds, _, err := zkConnection.Children(BrokerIdsPath)
	if (err != nil) {
		return nil, err
	}
	brokers := make([]*BrokerInfo, len(brokerIds))
	for i, brokerId := range brokerIds {
		brokerIdNum, err := strconv.Atoi(brokerId)
		if (err != nil) {
			return nil, err
		}

		brokers[i], err = GetBrokerInfo(zkConnection, int32(brokerIdNum))
		brokers[i].Id = int32(brokerIdNum)
		if (err != nil) {
			return nil, err
		}
	}

	return brokers, nil
}

func GetAllBrokersInClusterWatcher(zkConnection *zk.Conn) (<- chan zk.Event, error) {
	Debug("zk", "Subscribing for events from broker registry")
	_, _, watcher, err := zkConnection.ChildrenW(BrokerIdsPath)
	if (err != nil) {
		return nil, err
	}

	return watcher, nil
}

func GetBrokerInfo(zkConnection *zk.Conn, brokerId int32) (*BrokerInfo, error) {
	Debugf("zk", "Getting info for broker %d", brokerId)
	pathToBroker := fmt.Sprintf("%s/%d", BrokerIdsPath, brokerId)
	data, _, zkError := zkConnection.Get(pathToBroker)
	if (zkError != nil) {
		return nil, zkError
	}

	broker := &BrokerInfo{}
	mappingError := json.Unmarshal([]byte(data), broker)

	return broker, mappingError
}

func RegisterConsumer(zkConnection *zk.Conn, group string, consumerId string, consumerInfo *ConsumerInfo) error {
	Debugf("zk", "Trying to register consumer %s at group %s in Zookeeper", consumerId, group)
	pathToConsumer := fmt.Sprintf("%s/%s", NewZKGroupDirs(group).ConsumerRegistryDir, consumerId)
	data, mappingError := json.Marshal(consumerInfo)
	if mappingError != nil {
		return mappingError
	}

	Debugf("zk", "Path: %s", pathToConsumer)

	return CreateOrUpdatePathParentMayNotExist(zkConnection, pathToConsumer, data)
}

func DeregisterConsumer(zkConnection *zk.Conn, group string, consumerId string) error {
	Debugf("zk", "Trying to deregister consumer %s from group %s", consumerId, group)
	pathToConsumer := fmt.Sprintf("%s/%s", NewZKGroupDirs(group).ConsumerRegistryDir, consumerId)
	_, stat, err := zkConnection.Get(pathToConsumer)
	if (err != nil) {
		return err
	}
	return zkConnection.Delete(pathToConsumer, stat.Version)
}

func GetConsumersInGroup(zkConnection *zk.Conn, group string) ([]string, error) {
	Debugf("zk", "Getting consumers in group %s", group)
	consumers, _, err := zkConnection.Children(NewZKGroupDirs(group).ConsumerRegistryDir)
	if (err != nil) {
		return nil, err
	}

	return consumers, nil
}

func GetConsumersInGroupWatcher(zkConnection *zk.Conn, group string) (<- chan zk.Event, error) {
	Debugf("zk", "Getting consumer watcher for group %s", group)
	_, _, watcher, err := zkConnection.ChildrenW(NewZKGroupDirs(group).ConsumerRegistryDir)
	if (err != nil) {
		return nil, err
	}

	return watcher, nil
}

func GetConsumerGroupChangesWatcher(zkConnection *zk.Conn, group string) (<- chan zk.Event, error) {
	Debugf("zk", "Getting watcher for consumer group '%s' changes", group)
	_, _, watcher, err := zkConnection.ChildrenW(NewZKGroupDirs(group).ConsumerChangesDir)
	if (err != nil) {
		return nil, err
	}

	return watcher, nil
}

func GetConsumersPerTopic(zkConnection *zk.Conn, group string, excludeInternalTopics bool) (map[string][]*ConsumerThreadId, error) {
	consumers, err := GetConsumersInGroup(zkConnection, group)
	if (err != nil) {
		return nil, err
	}
	consumersPerTopicMap := make(map[string][]*ConsumerThreadId)
	for _, consumer := range consumers {
		topicsToNumStreams, err := NewTopicsToNumStreams(group, consumer, zkConnection, excludeInternalTopics)
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

func GetPartitionsForTopics(zkConnection *zk.Conn, topics []string) (map[string][]int32, error) {
	result := make(map[string][]int32)
	partitionAssignments, err := GetPartitionAssignmentsForTopics(zkConnection, topics)
	if (err != nil) {
		return nil, err
	}
	for topic, partitionAssignment := range partitionAssignments {
		for partition, _ := range partitionAssignment {
			result[topic] = append(result[topic], partition)
		}
	}

	return result, nil
}

func GetReplicaAssignmentsForTopics(zkConnection *zk.Conn, topics []string) (map[TopicAndPartition][]int32, error) {
	Debugf("zk", "Trying to get replica assignments for topics %v", topics)
	result := make(map[TopicAndPartition][]int32)
	for _, topic := range topics {
		topicInfo, err := GetTopicInfo(zkConnection, topic)
		if (err != nil) {
			return nil, err
		}
		for partition, replicaIds := range topicInfo.Partitions {
			partitionInt, err := strconv.Atoi(partition)
			if (err != nil) {
				return nil, err
			}
			topicAndPartition := TopicAndPartition{
				Topic: topic,
				Partition: int32(partitionInt),
			}
			result[topicAndPartition] = replicaIds
		}
	}

	return result, nil
}

func GetPartitionAssignmentsForTopics(zkConnection *zk.Conn, topics []string) (map[string]map[int32][]int32, error) {
	Debugf("zk", "Trying to get partition assignments for topics %v", topics)
	result := make(map[string]map[int32][]int32)
	for _, topic := range topics {
		topicInfo, err := GetTopicInfo(zkConnection, topic)
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

func GetTopicInfo(zkConnection *zk.Conn, topic string) (*TopicInfo, error) {
	data, _, err := zkConnection.Get(fmt.Sprintf("%s/%s", BrokerTopicsPath, topic))
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

func CreateOrUpdatePathParentMayNotExist(zkConnection *zk.Conn, pathToCreate string, data []byte) error {
	Debugf("zk", "Trying to create path %s in Zookeeper", pathToCreate)
	_, err := zkConnection.Create(pathToCreate, data, 0, zk.WorldACL(zk.PermAll))
	if (err != nil) {
		Warn("zk", err.Error())
		if (zk.ErrNodeExists == err) {
			if (len(data) > 0) {
				Debugf("zk", "Trying to update existing node %s", pathToCreate)
				return UpdateRecord(zkConnection, pathToCreate, data)
			} else {
				return nil
			}
		} else {
			parent, _ := path.Split(pathToCreate)
			err = CreateOrUpdatePathParentMayNotExist(zkConnection, parent[:len(parent)-1], make([]byte, 0))
			if (err != nil) {
				Error("zk", err.Error())
				return err
			} else {
				Debugf("zk", "Successfully created path %s", parent[:len(parent)-1])
			}

			Debugf("zk", "Trying again to create path %s in Zookeeper", pathToCreate)
			_, err = zkConnection.Create(pathToCreate, data, 0, zk.WorldACL(zk.PermAll))
		}
	}

	return err
}

func GetConsumer(zkConnection *zk.Conn, consumerGroup string, consumerId string) (*ConsumerInfo, error) {
	data, _, err := zkConnection.Get(fmt.Sprintf("%s/%s",
		NewZKGroupDirs(consumerGroup).ConsumerRegistryDir, consumerId))
	if (err != nil) {
		return nil, err
	}
	consumerInfo := &ConsumerInfo{}
	json.Unmarshal(data, consumerInfo)

	return consumerInfo, nil
}

func GetTopics(zkConnection *zk.Conn) (topics []string, err error) {
	topics, _, _, err = zkConnection.ChildrenW(BrokerTopicsPath)
	return
}

func GetTopicsWatcher(zkConnection *zk.Conn) (watcher <- chan zk.Event, err error) {
	_, _, watcher, err = zkConnection.ChildrenW(BrokerTopicsPath)
	return
}

func GetOffsetForTopicPartition(zkConnection *zk.Conn, group string, topicPartition *TopicAndPartition) (int64, error) {
	dirs := NewZKGroupTopicDirs(group, topicPartition.Topic)
	offset, _, err := zkConnection.Get(fmt.Sprintf("%s/%d", dirs.ConsumerOffsetDir, topicPartition.Partition))
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

func ClaimPartitionOwnership(zkConnection *zk.Conn, group string, topic string, partition int32, consumerThreadId *ConsumerThreadId) (bool, error) {
	pathToOwn := fmt.Sprintf("%s/%d", NewZKGroupTopicDirs(group, topic).ConsumerOwnerDir, partition)
	err := CreateOrUpdatePathParentMayNotExist(zkConnection, pathToOwn, []byte(consumerThreadId.String()))
	if (err != nil) {
		if (err == zk.ErrNodeExists) {
			Debugf(consumerThreadId, "waiting for the partition ownership to be deleted: %d", partition)
			return false, nil
		} else {
			Error(consumerThreadId, err)
			return false, err
		}
	}

	Debugf("zk", "Successfully claimed partition %d in topic %s for %s", partition, topic, consumerThreadId)

	return true, nil
}

func DeletePartitionOwnership(zkConnection *zk.Conn, group string, topic string, partition int32) error {
	pathToDelete := fmt.Sprintf("%s/%d", NewZKGroupTopicDirs(group, topic).ConsumerOwnerDir, partition)
	_, stat, err := zkConnection.Get(pathToDelete)
	if (err != nil) {
		return err
	}
	err = zkConnection.Delete(pathToDelete, stat.Version)
	if (err != nil) {
		return err
	}

	return nil
}

func IsConsumerGroupInSync(zkConnection *zk.Conn, group string) (bool, error) {
	Debugf("zk", "Trying to check consumer group '%s' lock existance", group)
	inSync, _, err := zkConnection.Exists(NewZKGroupDirs(group).ConsumerSyncDir)
	return inSync, err
}

func CreateConsumerGroupSync(zkConnection *zk.Conn, group string) error {
	Debugf("zk", "Creating lock for consumer group '%s'", group)
	return CreateOrUpdatePathParentMayNotExist(zkConnection, NewZKGroupDirs(group).ConsumerSyncDir, make([]byte, 0))
}

func DeleteConsumerGroupSync(zkConnection *zk.Conn, group string) error {
	Debugf("zk", "Trying to delete consumer group '%s' lock", group)
	pathToDelete := NewZKGroupDirs(group).ConsumerSyncDir
	_, stat, err := zkConnection.Get(pathToDelete)
	if err != nil {
		if err == zk.ErrNoNode {
			return nil
		} else {
			return err
		}
	}
	err = zkConnection.Delete(pathToDelete, stat.Version)
	if err != nil {
		return err
	}

	return nil
}

func NotifyConsumerGroup(zkConnection *zk.Conn, group string, consumerId string) error {
	path := fmt.Sprintf("%s/%s-%d", NewZKGroupDirs(group).ConsumerChangesDir, consumerId, time.Now().Nanosecond())
	Debugf("zk", "Sending notification to consumer group at %s", path)
	return CreateOrUpdatePathParentMayNotExist(zkConnection, path, make([]byte, 0))
}

func PurgeObsoleteConsumerGroupNotifications(zkConnection *zk.Conn, group string) error {
	Debugf("zk", "Trying to delete obsolete notifications from consumer group '%s'", group)
	notifications, _, err := zkConnection.Children(NewZKGroupDirs(group).ConsumerChangesDir)
	if err != nil {
		return err
	}

	for _, notification := range notifications {
		pathToDelete := fmt.Sprintf("%s/%s", NewZKGroupDirs(group).ConsumerChangesDir, notification)
		_, stat, err := zkConnection.Get(pathToDelete)
		if (err != nil) {
			return err
		}
		err = zkConnection.Delete(pathToDelete, stat.Version)
		if (err != nil) {
			return err
		}
	}

	return nil
}

func CommitOffset(zkConnection *zk.Conn, group string, topicPartition *TopicAndPartition, offset int64) error {
	dirs := NewZKGroupTopicDirs(group, topicPartition.Topic)
	return CreateOrUpdatePathParentMayNotExist(zkConnection, fmt.Sprintf("%s/%d", dirs.ConsumerOffsetDir, topicPartition.Partition), []byte(string(offset)))
}

func UpdateRecord(zkConnection *zk.Conn, pathToCreate string, dataToWrite []byte) error {
	Debugf("zk", "Trying to update path %s", pathToCreate)
	_, stat, _ := zkConnection.Get(pathToCreate)
	_, err := zkConnection.Set(pathToCreate, dataToWrite, stat.Version)

	return err
}

type ZKGroupDirs struct {
	Group               string
	ConsumerDir         string
	ConsumerGroupDir    string
	ConsumerRegistryDir string
	ConsumerChangesDir	string
	ConsumerSyncDir	  string
}

func NewZKGroupDirs(group string) *ZKGroupDirs {
	consumerGroupDir := fmt.Sprintf("%s/%s", ConsumersPath, group)
	consumerRegistryDir := fmt.Sprintf("%s/ids", consumerGroupDir)
	consumerChangesDir := fmt.Sprintf("%s/changes", consumerGroupDir)
	consumerSyncDir := fmt.Sprintf("%s/sync", consumerGroupDir)
	return &ZKGroupDirs {
		Group: group,
		ConsumerDir: ConsumersPath,
		ConsumerGroupDir: consumerGroupDir,
		ConsumerRegistryDir: consumerRegistryDir,
		ConsumerChangesDir: consumerChangesDir,
		ConsumerSyncDir: consumerSyncDir,
	}
}

type ZKGroupTopicDirs struct {
	ZkGroupDirs *ZKGroupDirs
	Topic             string
	ConsumerOffsetDir string
	ConsumerOwnerDir  string
}

func NewZKGroupTopicDirs(group string, topic string) *ZKGroupTopicDirs {
	zkGroupsDirs := NewZKGroupDirs(group)
	return &ZKGroupTopicDirs {
		ZkGroupDirs: zkGroupsDirs,
		Topic: topic,
		ConsumerOffsetDir: fmt.Sprintf("%s/%s/%s", zkGroupsDirs.ConsumerGroupDir, "offsets", topic),
		ConsumerOwnerDir: fmt.Sprintf("%s/%s/%s", zkGroupsDirs.ConsumerGroupDir, "owners", topic),
	}
}
