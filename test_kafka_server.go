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
	"runtime"
	"fmt"
	"io/ioutil"
	"math/rand"
	"path/filepath"
	"os"
	"os/exec"
	"time"
)

var kafkaPath = ""

type TestKafkaCluster struct {
	Path    string
	Servers []*TestKafkaServer
}

type TestKafkaServer struct {
	Host string
	Port int
	Path string
}

func (k *TestKafkaServer) Addr() string {
	return fmt.Sprintf("%s:%d", k.Host, k.Port)
}

func StartTestKafkaCluster(size int, zookeeperPort int) (*TestKafkaCluster, error) {
	kafkaPath = os.Getenv("KAFKA_PATH")
	if kafkaPath == "" {
		panic("Environment variable KAFKA_PATH not set!")
	}

	tmpPath, err := ioutil.TempDir("", "gokafka")
	if err != nil {
		panic(err)
	}

	startPort := int(rand.Int31n(6000) + 20000)
	success := false
	cluster := &TestKafkaCluster { Path: tmpPath }
	defer func() {
		if !success {
			cluster.Stop()
		}
	}()

	for i := 0; i < size; i++ {
		serverPath := filepath.Join(tmpPath, fmt.Sprintf("server%d", i))
		if err := os.Mkdir(serverPath, 0700); err != nil {
			return nil, err
		}
		port := startPort + i
		configPath := createServerConfig(i, port, zookeeperPort, serverPath)
		server, err := startTestKafkaServer(port, configPath)
		if err != nil {
			return nil, err
		}
		cluster.Servers = append(cluster.Servers, server)
	}

	success = true
	time.Sleep(1 * time.Second)
	return cluster, nil
}

func (c *TestKafkaCluster) Stop() {
	Info("KAFKA", "Stopping Kafka cluster")
	if runtime.GOOS == "windows" {
		c.stopTestServerWindows()
	} else {
		c.stopTestServerNix()
	}
}

func startTestKafkaServer(port int, configPath string) (*TestKafkaServer, error) {
	if runtime.GOOS == "windows" {
		return startTestKafkaServerWindows(port, configPath)
	} else {
		return startTestKafkaServerNix(port, configPath)
	}
}

func startTestKafkaServerNix(port int, configPath string) (*TestKafkaServer, error) {
	script := fmt.Sprintf("%s/bin/kafka-server-start.sh %s", kafkaPath, configPath)

	if err := exec.Command("sh", "-c", script).Start(); err != nil {
		return nil, err
	}

	return &TestKafkaServer {
		Host : "localhost",
		Port : port,
		Path : configPath,
	}, nil
}

func startTestKafkaServerWindows(port int, configPath string) (*TestKafkaServer, error) {
	script := fmt.Sprintf("%s/bin/windows/kafka-server-start.bat %s", kafkaPath, configPath)

	if err := exec.Command("cmd", "/C", script).Start(); err != nil {
		return nil, err
	}

	return &TestKafkaServer {
		Host : "localhost",
		Port : port,
		Path : configPath,
	}, nil
}

func (c *TestKafkaCluster) stopTestServerNix() {
	defer os.RemoveAll(c.Path)

	script := fmt.Sprintf("%s/bin/kafka-server-stop.sh", kafkaPath)

	if err := exec.Command("sh", "-c", script).Start(); err != nil {
		panic(err)
	}
}

func (c *TestKafkaCluster) stopTestServerWindows() {
	defer os.RemoveAll(c.Path)

	script := fmt.Sprintf("%s/bin/windows/kafka-server-stop.bat", kafkaPath)

	if err := exec.Command("sh", "-c", script).Start(); err != nil {
		panic(err)
	}
}

func createServerConfig(id int, port int, zookeeperPort int, serverPath string) string {
	fileName := "server.properties"

	contents := fmt.Sprintf(`broker.id=%d
port=%d
num.network.threads=2
num.io.threads=2
socket.send.buffer.bytes=1048576
socket.receive.buffer.bytes=1048576
socket.request.max.bytes=104857600
log.dirs=/tmp/kafka-logs
num.partitions=2
log.flush.interval.messages=10000
log.flush.interval.ms=1000
log.retention.hours=168
log.segment.bytes=536870912
log.cleanup.interval.mins=1
zookeeper.connect=localhost:%d
zookeeper.connection.timeout.ms=1000000`, id, port, zookeeperPort)

	configPath := fmt.Sprintf("%s/%s", serverPath, fileName)
	err := ioutil.WriteFile(configPath, []byte(contents), 0700)
	if err != nil {
		panic(err)
	}

	return configPath
}
