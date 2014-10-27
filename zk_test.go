package go_kafka_client

import (
	"testing"
	"github.com/samuel/go-zookeeper/zk"
)

func TestZkDependencyResolved(_ *testing.T) {
	//this just tests our gpm installation works fine
	_ = zk.Event{}
}
