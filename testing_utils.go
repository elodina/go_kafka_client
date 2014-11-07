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
	"reflect"
	"time"
	"github.com/stealthly/go-kafka/producer"
	"fmt"
)

func WithZookeeper(t *testing.T, zookeeperWork func(zkServer *zk.TestServer)) {
	testCluster, err := zk.StartTestCluster(1)
	if err != nil {
		t.Fatal(err)
	}

	defer testCluster.Stop()

	zookeeperWork(&testCluster.Servers[0])
}

func WithKafka(t *testing.T, kafkaWork func(zkServer *zk.TestServer, kafkaServer *TestKafkaServer)) {
	WithZookeeper(t, func(zkServer *zk.TestServer) {
		cluster, err := StartTestKafkaCluster(1, zkServer.Port)
		if err != nil {
			panic(err)
		}
		defer cluster.Stop()

		kafkaWork(zkServer, cluster.Servers[0])
	})
}

func Assert(t *testing.T, actual interface{}, expected interface{}) {
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expected %v, actual %v", expected, actual)
	}
}

func AssertNot(t *testing.T, actual interface{}, expected interface{}) {
	if reflect.DeepEqual(actual, expected) {
		t.Errorf("%v should not be %v", actual, expected)
	}
}

func ReceiveN(t *testing.T, n int, timeout time.Duration, from <-chan []*Message) {
	numMessages := 0
	for {
		select {
		case batch := <-from: {
			if numMessages + len(batch) > n {
				t.Error("Received more messages than expected")
			}
			numMessages += len(batch)
			if numMessages == n {
				Debugf("test", "Successfully consumed %d message[s]", n)
				return
			}
		}
		case <-time.After(timeout): t.Errorf("Failed to receive a message within %d seconds", timeout.Seconds())
		}
	}
}

func ReceiveNFromMultipleChannels(t *testing.T, n int, timeout time.Duration, from []<-chan []*Message) map[<-chan []*Message]int {
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

		Debugf("test", "Received %d messages from channel %d", batchSize, chosen-1)
		if numMessages + batchSize > n {
			t.Error("Received more messages than expected")
		}
		numMessages += batchSize
		messageStats[from[chosen-1]] = messageStats[from[chosen-1]] + batchSize
		if numMessages == n {
			Debugf("test", "Successfully consumed %d message[s]", n)
			return messageStats
		}
	}

	return nil
}

func ProduceN(t *testing.T, n int, p *producer.KafkaProducer) {
	for i := 0; i < n; i++ {
		message := fmt.Sprintf("test-kafka-message-%d", n)
		if err := p.Send(message); err != nil {
			t.Fatalf("Failed to produce message %s", message)
		}
	}
}

func CloseWithin(t *testing.T, timeout time.Duration, consumer *Consumer) {
	select {
	case <-consumer.Close(): {
		Info("test", "Successfully closed consumer")
	}
	case <-time.After(timeout): {
		t.Errorf("Failed to close a consumer within %d seconds", timeout.Seconds())
	}
	}
}
