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
	"github.com/samuel/go-zookeeper/zk"
)

func WithZookeeper(t *testing.T, zookeeperWork func(zkServer *zk.TestServer)) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatal(r)
		}
	}()

	testCluster, err := zk.StartTestCluster(1)
	if err != nil {
		t.Fatal(err)
	}

	defer testCluster.Stop()

	zookeeperWork(&testCluster.Servers[0])
}

func WithKafka(t *testing.T, kafkaWork func(zkServer *zk.TestServer, kafkaServer *TestKafkaServer)) {
	WithZookeeper(t, func(zkServer *zk.TestServer) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatal(r)
			}
		}()

		cluster, err := StartTestKafkaCluster(1, zkServer.Port)
		if err != nil {
			panic(err)
		}
		defer cluster.Stop()

		kafkaWork(zkServer, cluster.Servers[0])
	})
}
