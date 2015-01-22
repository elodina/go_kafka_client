package main

import (
	kafka "github.com/stealthly/go_kafka_client"
	"flag"
	"os"
)

var zkConnect = flag.String("zookeeper", "", "zookeeper connection string host:port.")

var blueTopic = flag.String("blue.topic", "", "first topic name")
var blueGroup = flag.String("blue.group", "", "first consumer group name")
var bluePattern = flag.String("blue.pattern", "", "first consumer group name")

var greenTopic = flag.String("green.topic", "", "second topic name")
var greenGroup  = flag.String("green.group", "", "second consumer group name")
var greenPattern = flag.String("green.pattern", "", "second consumer pattern")

func main() {
	flag.Parse()
	if *zkConnect == "" || *blueTopic == "" || *blueGroup == "" || *bluePattern == "" || *greenTopic == "" || *greenGroup == "" || *greenPattern == "" {
		flag.Usage()
		os.Exit(1)
	}
	blue := kafka.BlueGreenDeployment{*blueTopic, *bluePattern, *blueGroup}
	green := kafka.BlueGreenDeployment{*greenTopic, *greenPattern, *greenGroup}

	zkConfig := kafka.NewZookeeperConfig()
	zkConfig.ZookeeperConnect = []string{*zkConnect}

	zk := kafka.NewZookeeperCoordinator(zkConfig)
	zk.Connect()

	zk.RequestBlueGreenDeployment(blue, green)
}
