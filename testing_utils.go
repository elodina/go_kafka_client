/* Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. */

package go_kafka_client

import (
	"fmt"
	"github.com/samuel/go-zookeeper/zk"
	//	"github.com/stealthly/go-kafka/producer"
	"github.com/Shopify/sarama"
	"os"
	"os/exec"
	"reflect"
	"runtime"
	"testing"
	"time"
)

type logWriter struct {
	t *testing.T
	p string
}

func (lw logWriter) Write(b []byte) (int, error) {
	lw.t.Logf("%s%s", lw.p, string(b))
	return len(b), nil
}

func withZookeeper(t *testing.T, zookeeperWork func(zkServer *zk.TestServer)) {
	testCluster, err := zk.StartTestCluster(1, nil, logWriter{t: t, p: "[ZKERR] "})
	if err != nil {
		t.Fatal(err)
	}

	defer testCluster.Stop()

	zookeeperWork(&testCluster.Servers[0])
}

func assert(t *testing.T, actual interface{}, expected interface{}) {
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expected %v, actual %v", expected, actual)
	}
}

func assertNot(t *testing.T, actual interface{}, expected interface{}) {
	if reflect.DeepEqual(actual, expected) {
		t.Errorf("%v should not be %v", actual, expected)
	}
}

func receiveN(t *testing.T, n int, timeout time.Duration, from <-chan []*Message) {
	numMessages := 0
	for {
		select {
		case batch := <-from:
			{
				if numMessages+len(batch) > n {
					t.Error("Received more messages than expected")
					return
				}
				numMessages += len(batch)
				if numMessages == n {
					Infof("test", "Successfully consumed %d message[s]", n)
					return
				}
			}
		case <-time.After(timeout):
			{
				t.Errorf("Failed to receive a message within %d seconds", int(timeout.Seconds()))
				return
			}
		}
	}
}

func receiveNFromMultipleChannels(t *testing.T, n int, timeout time.Duration, from []<-chan []*Message) map[<-chan []*Message]int {
	numMessages := 0

	messageStats := make(map[<-chan []*Message]int)
	cases := make([]reflect.SelectCase, len(from)+1)
	for i, ch := range from {
		cases[i+1] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ch)}
	}
	cases[0] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(time.After(timeout))}

	remaining := len(cases)
	for remaining > 0 {
		chosen, value, ok := reflect.Select(cases)
		if !ok {
			// The chosen channel has been closed, so zero out the channel to disable the case
			cases[chosen].Chan = reflect.ValueOf(nil)
			remaining -= 1
			continue
		}

		if _, ok := value.Interface().(time.Time); ok {
			t.Errorf("Failed to receive %d messages within %d seconds", n, timeout.Seconds())
			return nil
		}

		batch := value.Interface().([]*Message)
		batchSize := len(batch)

		Infof("test", "Received %d messages from channel %d", batchSize, chosen-1)
		if numMessages+batchSize > n {
			t.Error("Received more messages than expected")
		}
		numMessages += batchSize
		messageStats[from[chosen-1]] = messageStats[from[chosen-1]] + batchSize
		if numMessages == n {
			Infof("test", "Successfully consumed %d message[s]", n)
			return messageStats
		}
	}

	return nil
}

func receiveNoMessages(t *testing.T, timeout time.Duration, from <-chan []*Message) {
	for {
		select {
		case batch := <-from:
			{
				t.Errorf("Received %d messages when should receive none", len(batch))
			}
		case <-time.After(timeout):
			{
				Info("test", "Received no messages as expected")
				return
			}
		}
	}
}

func produceN(t *testing.T, n int, topic string, brokerAddr string) {
	clientConfig := sarama.NewConfig()
	clientConfig.Producer.Timeout = 10 * time.Second
	client, err := sarama.NewClient([]string{brokerAddr}, clientConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	producer, err := sarama.NewAsyncProducerFromClient(client)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	for i := 0; i < n; i++ {
		producer.Input() <- &sarama.ProducerMessage{Topic: topic, Key: nil, Value: sarama.StringEncoder(fmt.Sprintf("test-kafka-message-%d", i))}
	}
	select {
	case e := <-producer.Errors():
		t.Fatalf("Failed to produce message: %s", e)
	case <-time.After(5 * time.Second):
	}
}

func produceNToTopicPartition(t *testing.T, n int, topic string, partition int, brokerAddr string) {
	clientConfig := sarama.NewConfig()
	partitionerFactory := &SaramaPartitionerFactory{NewFixedPartitioner}
	clientConfig.Producer.Partitioner = partitionerFactory.PartitionerConstructor
	clientConfig.Producer.Timeout = 10 * time.Second
	client, err := sarama.NewClient([]string{brokerAddr}, clientConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	producer, err := sarama.NewAsyncProducerFromClient(client)
	encoder := &Int32Encoder{}
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	for i := 0; i < n; i++ {
		key, _ := encoder.Encode(uint32(partition))
		producer.Input() <- &sarama.ProducerMessage{Topic: topic, Key: sarama.ByteEncoder(key), Value: sarama.StringEncoder(fmt.Sprintf("test-kafka-message-%d", i))}
	}
	select {
	case e := <-producer.Errors():
		t.Fatalf("Failed to produce message: %s", e)
	case <-time.After(5 * time.Second):
	}
}

func produce(t *testing.T, messages []string, topic string, brokerAddr string, compression sarama.CompressionCodec) {
	clientConfig := sarama.NewConfig()
	clientConfig.Producer.Compression = compression
	clientConfig.Producer.Timeout = 10 * time.Second
	client, err := sarama.NewClient([]string{brokerAddr}, clientConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	producer, err := sarama.NewAsyncProducerFromClient(client)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	for _, message := range messages {
		producer.Input() <- &sarama.ProducerMessage{Topic: topic, Key: nil, Value: sarama.StringEncoder(message)}
	}
	select {
	case e := <-producer.Errors():
		t.Fatalf("Failed to produce message: %s", e)
	case <-time.After(5 * time.Second):
	}
}

func closeWithin(t *testing.T, timeout time.Duration, consumer *Consumer) {
	select {
	case <-consumer.Close():
		{
			Info("test", "Successfully closed consumer")
		}
	case <-time.After(timeout):
		{
			t.Errorf("Failed to close a consumer within %d seconds", timeout.Seconds())
		}
	}
}

//Convenience utility to create a topic topicName with numPartitions partitions in Zookeeper located at zk (format should be host:port).
//Please note that this requires Apache Kafka 0.8.1 binary distribution available through KAFKA_PATH environment variable
func CreateMultiplePartitionsTopic(zk string, topicName string, numPartitions int) {
	if runtime.GOOS == "windows" {
		params := fmt.Sprintf("--create --zookeeper %s --replication-factor 1 --partitions %d --topic %s", zk, numPartitions, topicName)
		script := fmt.Sprintf("%s\\bin\\windows\\kafka-topics.bat %s", os.Getenv("KAFKA_PATH"), params)
		exec.Command("cmd", "/C", script).Output()
	} else {
		params := fmt.Sprintf("--create --zookeeper %s --replication-factor 1 --partitions %d --topic %s", zk, numPartitions, topicName)
		script := fmt.Sprintf("%s/bin/kafka-topics.sh %s", os.Getenv("KAFKA_PATH"), params)
		out, err := exec.Command("sh", "-c", script).Output()
		if err != nil {
			panic(err)
		}
		Debug("create topic", out)
	}
}

//blocks until the leader for every partition of a given topic appears
//this is used by tests only to avoid "In the middle of a leadership election, there is currently no leader for this partition and hence it is unavailable for writes"
func EnsureHasLeader(zkConnect string, topic string) {
	zkConfig := NewZookeeperConfig()
	zkConfig.ZookeeperConnect = []string{zkConnect}
	zookeeper := NewZookeeperCoordinator(zkConfig)
	zookeeper.Connect()
	hasLeader := false

	numPartitions := 0
	for !hasLeader {
		var topicInfo *TopicInfo
		var err error
		for i := 0; i < 3; i++ {
			topicInfo, err = zookeeper.getTopicInfo(topic)
			if topicInfo != nil {
				break
			}
		}
		if err != nil {
			continue
		}
		numPartitions = len(topicInfo.Partitions)

		hasLeader = true
		for partition, leaders := range topicInfo.Partitions {
			if len(leaders) == 0 {
				Warnf(zookeeper, "Partition %s has no leader, waiting...", partition)
				hasLeader = false
				break
			}
		}

		if !hasLeader {
			time.Sleep(1 * time.Second)
		}
	}
	time.Sleep(time.Duration(numPartitions) * time.Second)
}
