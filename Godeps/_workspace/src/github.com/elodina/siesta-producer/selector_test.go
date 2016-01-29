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

package producer

import (
	"github.com/elodina/siesta"
	"testing"
	"time"
)

func TestSelector(t *testing.T) {
	selectorConfig := DefaultSelectorConfig()
	selector := NewSelector(selectorConfig)

	link := siesta.NewBrokerLink(&siesta.Broker{ID: 1, Host: "localhost", Port: 9092},
		true,
		1*time.Minute,
		5)

	request1 := new(siesta.ProduceRequest)
	request1.RequiredAcks = 1
	request1.AckTimeoutMs = 2000
	request1.AddMessage("siesta", 0, &siesta.Message{MagicByte: 0, Value: []byte("hello world")})

	request2 := new(siesta.ProduceRequest)
	request2.RequiredAcks = 1
	request2.AckTimeoutMs = 2000
	request2.AddMessage("siesta", 0, &siesta.Message{MagicByte: 0, Value: []byte("hello world again")})

	responseChan1 := selector.Send(link, request1)
	responseChan2 := selector.Send(link, request2)

	//make sure we can read in other order than was sent
	response2 := <-responseChan2
	response1 := <-responseChan1

	assert(t, response1.err, nil)
	assert(t, response2.err, nil)
}
