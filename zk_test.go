package go_kafka_client

import (
	"testing"
/*	"fmt"
	"time"
	"github.com/samuel/go-zookeeper/zk"
	"os"*/
)

func TestGetAllBrokersInCluster(t *testing.T) {
/*	os.Setenv("ZOOKEEPER_PATH", "F:\\zookeeper-3.4.6")
	cluster, err := zk.StartTestCluster(1)
	if (err != nil) {
		fmt.Println(err)
	}
	fmt.Println(cluster)
	port := cluster.Servers[0].Port
	c, _, err := zk.Connect([]string{fmt.Sprintf("127.0.0.1:%d", port)}, time.Second * 30000)
	if (err != nil) {
		fmt.Println(err)
	}
	brokers, _ := GetAllBrokersInCluster(c)
	for i := range brokers {
		fmt.Printf("id=%d, %s:%d", brokers[i].Id, brokers[i].Host, brokers[i].Port)
	}*/
}
