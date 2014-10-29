package go_kafka_client

import (
	"testing"
	"fmt"
	"time"
	"github.com/samuel/go-zookeeper/zk"
	"encoding/json"
)

var (
	cluster *zk.TestCluster = nil
	zkServer *zk.TestServer = nil
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

func before(t *testing.T) {
	testCluster, err := zk.StartTestCluster(1)
	if err != nil {
		t.Fatal(err)
	}

	cluster = testCluster
	zkServer = &testCluster.Servers[0]

	conn, _, err := zk.Connect([]string{fmt.Sprintf("127.0.0.1:%d", zkServer.Port)}, time.Second*30000)
	if (err != nil) {
		t.Fatal(err)
	}
	zkConnection = conn
}

func tearDown(t *testing.T) {
	cluster.Stop()
}

func TestAll(t *testing.T) {
	before(t)
	testCreatePathParentMayNotExist(t, BrokerIdsPath)
	testCreatePathParentMayNotExist(t, BrokerTopicsPath)
	testGetBrokerInfo(t)
	testGetAllBrokersInCluster(t)
	testRegisterConsumer(t)
	testGetConsumersInGroup(t)
	tearDown(t)
}

func testCreatePathParentMayNotExist(t * testing.T, pathToCreate string) {
	err := CreateOrUpdatePathParentMayNotExist(zkConnection, pathToCreate, make([]byte, 0))
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
	CreateOrUpdatePathParentMayNotExist(zkConnection, fmt.Sprintf("%s/%d", BrokerIdsPath, broker.Id), []byte(jsonBroker))
	brokerInfo, err := GetBrokerInfo(zkConnection, broker.Id)
	if (err != nil) {
		t.Error(err)
	}
	Assert(t, *brokerInfo, *broker)
}

func testGetAllBrokersInCluster(t *testing.T) {
	brokers, err := GetAllBrokersInCluster(zkConnection)

	Assert(t, err, nil)
	Assert(t, len(brokers), 1)
}

func testRegisterConsumer(t *testing.T) {
	subscription := make(map[string]int)
	subscription["topic1"] = 1

	consumerInfo := &ConsumerInfo{
		Version : int16(1),
		Subscription : subscription,
		Pattern : WhiteList,
		Timestamp : time.Now().Unix(),
	}
	err := RegisterConsumer(zkConnection, consumerGroup, fmt.Sprintf(consumerIdPattern, 0), consumerInfo)
	if (err != nil) {
		t.Error(err)
	}
	actualConsumerInfo, err := GetConsumer(zkConnection, consumerGroup, fmt.Sprintf(consumerIdPattern, 0))

	Assert(t, *actualConsumerInfo, *consumerInfo)
}

func testGetConsumersInGroup(t *testing.T) {
	consumers, err := GetConsumersInGroup(zkConnection, consumerGroup)
	if (err != nil) {
		t.Error(err)
	}
	Assert(t, len(consumers), 1)
}
