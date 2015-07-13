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
	"net"
	"testing"
	"time"
)

var tcpAddr = "0.0.0.0:5140"
var rfc5424message = "<34>1 2003-10-11T22:14:15.003Z localhost.stealth.ly su - ID23 - a simple message"
var rfc3164message = `<34>Jan 12 06:30:00 1.2.3.4 some_server: 1.2.3.4 - - [12/Jan/2011:06:29:59 +0100] "GET /foo/bar.html HTTP/1.1" 301 96 "-" "Mozilla/5.0 (Windows; U; Windows NT 5.1; fr; rv:1.9.2.12) Gecko/20101026 Firefox/3.6.12 ( .NET CLR 3.5.30729)" PID 18904 Time Taken 0`

//TODO any parser for this? what kind of RFC log/syslog is using at all?
//<PRI>TIMESTAMP HOSTNAME TAG[PID]: MSG
//<132>2014-12-30T11:51:47+02:00 localhost go_kafka_client[13128]: woohoo!

func TestSyslogProducer(t *testing.T) {
	Logger = NewDefaultLogger(TraceLevel)
	topic := fmt.Sprintf("syslog-producer-%d", time.Now().Unix())

	consumeMessages := 100
	timeout := 1 * time.Minute
	consumeStatus := make(chan int)

	CreateMultiplePartitionsTopic(localZk, topic, 1)
	EnsureHasLeader(localZk, topic)

	config := NewSyslogProducerConfig()
	config.ProducerConfig = DefaultProducerConfig()
	config.BrokerList = localBroker
	config.ChannelSize = 5
	config.NumProducers = 1
	config.TCPAddr = tcpAddr
	config.Topic = topic
	syslogProducer := NewSyslogProducer(config)
	go syslogProducer.Start()

	time.Sleep(2 * time.Second)
	for i := 0; i < 100; i++ {
		logMessage()
	}

	consumerConfig := testConsumerConfig()
	consumerConfig.Strategy = newCountingStrategy(t, consumeMessages, timeout, consumeStatus)
	consumer := NewConsumer(consumerConfig)
	go consumer.StartStatic(map[string]int{topic: 1})
	if actual := <-consumeStatus; actual != consumeMessages {
		t.Errorf("Failed to consume %d messages within %s. Actual messages = %d", consumeMessages, timeout, actual)
	}
	closeWithin(t, 10*time.Second, consumer)
	syslogProducer.Stop()
}

//TODO use log/syslog somehow?
func logMessage() {
	serverAddr, _ := net.ResolveTCPAddr("tcp", tcpAddr)
	con, err := net.DialTCP("tcp", nil, serverAddr)
	if err != nil {
		panic(err)
	}
	con.Write([]byte(rfc5424message))
	con.Close()
}
