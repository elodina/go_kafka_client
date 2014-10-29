package go_kafka_client

import (
	"testing"
	"fmt"
	"time"
	"github.com/samuel/go-zookeeper/zk"
)

var (
	cluster *zk.TestCluster = nil
	zkServer *zk.TestServer = nil
)

func before(t *testing.T) {
	testCluster, err := zk.StartTestCluster(1)
	if err != nil {
		t.Fatal(err)
	}

	cluster = testCluster
	zkServer = &testCluster.Servers[0]
}

func tearDown(t *testing.T) {
	cluster.Stop()
}

func TestAll(t *testing.T) {
	before(t)
	testGetAllBrokersInCluster(t)
	tearDown(t)
}

func testGetAllBrokersInCluster(t *testing.T) {
	c, _, err := zk.Connect([]string{fmt.Sprintf("127.0.0.1:%d", zkServer.Port)}, time.Second * 30000)
	if (err != nil) {
		t.Error(err)
	}
	brokers, _ := GetAllBrokersInCluster(c)

	Assert(t, err, nil)

	for i := range brokers {
		fmt.Printf("id=%d, %s:%d", brokers[i].Id, brokers[i].Host, brokers[i].Port)
	}
}
