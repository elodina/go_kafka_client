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
	Logger.Printf("Getting all brokers in cluster\n")
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
		if (err != nil) {
			return nil, err
		}
	}

	return brokers, nil
}

func GetBrokerInfo(zkConnection *zk.Conn, brokerId int32) (*BrokerInfo, error) {
	Logger.Printf("Getting info for broker %d\n", brokerId)
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
	Logger.Printf("Trying to register consumer %s at group %s in Zookeeper\n", consumerId, group)
	pathToConsumer := fmt.Sprintf("%s/%s", NewZKGroupDirs(group).ConsumerRegistryDir, consumerId)
	data, mappingError := json.Marshal(consumerInfo)
	if mappingError != nil {
		return mappingError
	}

	Logger.Printf("Path: %s\n", pathToConsumer)

	return CreateOrUpdatePathParentMayNotExist(zkConnection, pathToConsumer, data)
}

func DeregisterConsumer(zkConnection *zk.Conn, group string, consumerId string) error {
	Logger.Printf("Trying to deregister consumer %s from group %s", consumerId, group)
	pathToConsumer := fmt.Sprintf("%s/%s", NewZKGroupDirs(group).ConsumerRegistryDir, consumerId)
	_, stat, err := zkConnection.Get(pathToConsumer)
	if (err != nil) {
		return err
	}
	return zkConnection.Delete(pathToConsumer, stat.Version)
}

func GetConsumersInGroup(zkConnection *zk.Conn, group string) ([]string, error) {
	Logger.Printf("Getting consumers in group %s\n", group)
	consumers, _, err := zkConnection.Children(NewZKGroupDirs(group).ConsumerRegistryDir)
	if (err != nil) {
		return nil, err
	}

	return consumers, nil
}

func GetConsumersInGroupWatcher(zkConnection *zk.Conn, group string) (<- chan zk.Event, error) {
	Logger.Printf("Getting consumers in group %s\n", group)
	_, _, watcher, err := zkConnection.ChildrenW(NewZKGroupDirs(group).ConsumerRegistryDir)
	if (err != nil) {
		return nil, err
	}

	return watcher, nil
}

func GetConsumersPerTopic(zkConnection *zk.Conn, group string, excludeInternalTopics bool) (map[string][]*ConsumerThreadId, error) {
	dirs := NewZKGroupDirs(group)
	consumers, _, err := zkConnection.Children(dirs.ConsumerRegistryDir)
	if (err != nil) {
		return nil, err
	}
	consumersPerTopicMap := make(map[string][]*ConsumerThreadId)
	for _, consumer := range consumers {
		topicsToNumStreams, err := NewTopicsToNumStreams(group, consumer, zkConnection, excludeInternalTopics)
		if (err != nil) {
			return nil, err
		}
		for topic := range topicsToNumStreams.GetConsumerThreadIdsPerTopic() {
			sort.Sort(ByName(consumersPerTopicMap[topic]))
		}
	}

	return consumersPerTopicMap, nil
}

func CreateOrUpdatePathParentMayNotExist(zkConnection *zk.Conn, pathToCreate string, data []byte) error {
	Logger.Printf("Trying to create path %s in Zookeeper", pathToCreate)
	_, err := zkConnection.Create(pathToCreate, data, 0, zk.WorldACL(zk.PermAll))
	if (err != nil) {
		Logger.Println(err)
		if (zk.ErrNodeExists == err) {
			if (len(data) > 0) {
				return UpdateRecord(zkConnection, pathToCreate, data)
			} else {
				return nil
			}
		} else {
			parent, _ := path.Split(pathToCreate)
			err = CreateOrUpdatePathParentMayNotExist(zkConnection, parent[:len(parent)-1], make([]byte, 0))
			if (err != nil) {
				return err
			} else {
				Logger.Printf("Successfully created path %s", parent[:len(parent)-1])
			}

			Logger.Printf("Trying again to create path %s in Zookeeper", pathToCreate)
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

func UpdateRecord(zkConnection *zk.Conn, pathToCreate string, dataToWrite []byte) error {
	Logger.Printf("Trying to updated entry at path %s", pathToCreate)
	_, stat, _ := zkConnection.Get(pathToCreate)
	_, err := zkConnection.Set(pathToCreate, dataToWrite, stat.Version)

	return err
}

type ZKGroupDirs struct {
	Group               string
	ConsumerDir         string
	ConsumerGroupDir    string
	ConsumerRegistryDir string
}

func NewZKGroupDirs(group string) *ZKGroupDirs {
	consumerGroupDir := fmt.Sprintf("%s/%s", ConsumersPath, group)
	consumerRegistryDir := fmt.Sprintf("%s/ids", consumerGroupDir)
	return &ZKGroupDirs {
		Group: group,
		ConsumerDir: ConsumersPath,
		ConsumerGroupDir: consumerGroupDir,
		ConsumerRegistryDir: consumerRegistryDir,
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
