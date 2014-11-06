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
	"github.com/stealthly/go-kafka/producer"
	"fmt"
//	"time"
)

func TestConsumer(t *testing.T) {
	WithKafka(t, func(zkServer *zk.TestServer, kafkaServer *TestKafkaServer) {
		config := DefaultConsumerConfig()
		config.ZookeeperConnect = []string{fmt.Sprintf("localhost:%d", zkServer.Port)}
		consumer := NewConsumer(config)
		AssertNot(t, consumer.zkConn, nil)

		kafkaProducer := producer.NewKafkaProducer("test", []string{kafkaServer.Addr()}, nil)
		err := kafkaProducer.Send("test")
		if err != nil {
			t.Fatal(err)
		}

//		topics := map[string]int {"test": 1}
//		streams := consumer.CreateMessageStreams(topics)
//		select {
//		case event := <-streams["test"][0]: {
//			Infof(consumer, "Got a message: %s\n", event)
//		}
//		case <-time.After(5 * time.Second): {
//			t.Error("Failed to receive a message within 5 seconds")
//		}
//		}
//
//		select {
//		case <-consumer.Close(): {
//			Info(consumer, "Successfully closed consumer")
//		}
//		case <-time.After(5 * time.Second): {
//			t.Error("Failed to close a consumer within 5 seconds")
//		}
//		}
		//TODO other
	})
}
