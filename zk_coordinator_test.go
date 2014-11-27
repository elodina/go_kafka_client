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
	"testing"
	"fmt"
	"time"
	"github.com/samuel/go-zookeeper/zk"
	"encoding/json"
)

var (
	coordinator *ZookeeperCoordinator = nil
	zkConnection *zk.Conn   = nil
	consumerGroup           = "testGroup"
	consumerIdPattern       = "go-consumer-%d"
	broker                  = &BrokerInfo{
								Version: 1,
								Id: 0,
								Host: "localhost",
								Port: 9092,
							}
)

func TestZkAPI(t *testing.T) {
	WithZookeeper(t, func(zkServer *zk.TestServer) {
		coordinatorConfig := NewZookeeperConfig()
		coordinatorConfig.ZookeeperConnect = []string{fmt.Sprintf("127.0.0.1:%d", zkServer.Port)}
		coordinator = NewZookeeperCoordinator(coordinatorConfig)
		coordinator.Connect()
		zkConnection = coordinator.zkConn
		testCreatePathParentMayNotExist(t, brokerIdsPath)
		testCreatePathParentMayNotExist(t, brokerTopicsPath)
		testGetBrokerInfo(t)
		testGetAllBrokersInCluster(t)
		testRegisterConsumer(t)
		testGetConsumersInGroup(t)
		testDeregisterConsumer(t)
	})
}

func testCreatePathParentMayNotExist(t * testing.T, pathToCreate string) {
	err := coordinator.createOrUpdatePathParentMayNotExist(pathToCreate, make([]byte, 0))
	if (err != nil) {
		t.Fatal(err)
	}

	exists, _, existsErr := zkConnection.Exists(pathToCreate)
	if (existsErr != nil) {
		t.Fatal(err)
	}

	if (!exists) {
		t.Fatalf("Failed to create path %s in Zookeeper", pathToCreate)
	}
}

func testGetBrokerInfo(t *testing.T) {
	jsonBroker, _ := json.Marshal(broker)
	coordinator.createOrUpdatePathParentMayNotExist(fmt.Sprintf("%s/%d", brokerIdsPath, broker.Id), []byte(jsonBroker))
	brokerInfo, err := coordinator.getBrokerInfo(broker.Id)
	if (err != nil) {
		t.Error(err)
	}
	Assert(t, *brokerInfo, *broker)
}

func testGetAllBrokersInCluster(t *testing.T) {
	brokers, err := coordinator.GetAllBrokers()

	Assert(t, err, nil)
	Assert(t, len(brokers), 1)
}

func testRegisterConsumer(t *testing.T) {
	subscription := make(map[string]int)
	subscription["topic1"] = 1

	consumerInfo := &ConsumerInfo{
		Version : int16(1),
		Subscription : subscription,
		Pattern : WhiteListPattern,
		Timestamp : time.Now().Unix(),
	}

	topicCount := &WildcardTopicsToNumStreams{
		Coordinator : coordinator,
		ConsumerId : fmt.Sprintf(consumerIdPattern, 0),
		TopicFilter : NewWhiteList("topic1"),
		NumStreams : 1,
		ExcludeInternalTopics : true,
	}

	err := coordinator.RegisterConsumer(fmt.Sprintf(consumerIdPattern, 0), consumerGroup, topicCount)
	if (err != nil) {
		t.Error(err)
	}
	actualConsumerInfo, err := coordinator.GetConsumerInfo(fmt.Sprintf(consumerIdPattern, 0), consumerGroup)

	Assert(t, *actualConsumerInfo, *consumerInfo)
}

func testGetConsumersInGroup(t *testing.T) {
	consumers, err := coordinator.GetConsumersInGroup(consumerGroup)
	if (err != nil) {
		t.Error(err)
	}
	Assert(t, len(consumers), 1)
}

func testDeregisterConsumer(t *testing.T) {
	consumerId := fmt.Sprintf(consumerIdPattern, 0)
	coordinator.DeregisterConsumer(consumerId, consumerGroup)
	exists, _, err := zkConnection.Exists(fmt.Sprintf("%s/%s", newZKGroupDirs(consumerGroup).ConsumerRegistryDir, consumerId))
	if (err != nil) {
		t.Error(err)
	}
	Assert(t, exists, false)
}
