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
	"io"
	"net"
	"time"
)

type ConnectionRequest struct {
	connection *net.TCPConn
	request    *NetworkRequest
}

//TODO proper config entry names that match upstream Kafka
type SelectorConfig struct {
	ClientID        string
	MaxRequests     int
	SendRoutines    int
	ReceiveRoutines int
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	RequiredAcks    int
}

func DefaultSelectorConfig() *SelectorConfig {
	return &SelectorConfig{
		ClientID:        "siesta",
		MaxRequests:     10,
		SendRoutines:    10,
		ReceiveRoutines: 10,
		ReadTimeout:     5 * time.Second,
		WriteTimeout:    5 * time.Second,
		RequiredAcks:    1,
	}
}

func NewSelectorConfig(producerConfig *ProducerConfig) *SelectorConfig {
	return &SelectorConfig{
		ClientID:        producerConfig.ClientID,
		MaxRequests:     producerConfig.MaxRequests,
		SendRoutines:    producerConfig.SendRoutines,
		ReceiveRoutines: producerConfig.ReceiveRoutines,
		ReadTimeout:     producerConfig.ReadTimeout,
		WriteTimeout:    producerConfig.WriteTimeout,
		RequiredAcks:    producerConfig.RequiredAcks,
	}
}

type Selector struct {
	config    *SelectorConfig
	requests  chan *NetworkRequest
	responses chan *ConnectionRequest
}

func NewSelector(config *SelectorConfig) *Selector {
	selector := &Selector{
		config:    config,
		requests:  make(chan *NetworkRequest, config.MaxRequests),
		responses: make(chan *ConnectionRequest, config.MaxRequests),
	}
	selector.Start()
	return selector
}

func (s *Selector) Start() {
	for i := 0; i < s.config.SendRoutines; i++ {
		go s.requestDispatcher()
	}

	if s.config.RequiredAcks > 0 {
		for i := 0; i < s.config.ReceiveRoutines; i++ {
			go s.responseDispatcher()
		}
	}
}

func (s *Selector) Close() {
	close(s.requests)
	close(s.responses)
}

func (s *Selector) Send(link siesta.BrokerLink, request siesta.Request) <-chan *rawResponseAndError {
	responseChan := make(chan *rawResponseAndError, 1) //make this buffered so we don't block if noone reads the response
	s.requests <- &NetworkRequest{link, request, responseChan}

	return responseChan
}

func (s *Selector) requestDispatcher() {
	for request := range s.requests {
		link := request.link
		id, conn, err := link.GetConnection()
		if err != nil {
			link.Failed()
			if s.config.RequiredAcks > 0 {
				request.responseChan <- &rawResponseAndError{nil, link, err}
			}
			continue
		}

		if err := s.send(id, conn, request.request); err != nil {
			link.Failed()
			if s.config.RequiredAcks > 0 {
				request.responseChan <- &rawResponseAndError{nil, link, err}
			} else {
				link.ReturnConnection(conn)
			}
			continue
		}

		if s.config.RequiredAcks > 0 {
			s.responses <- &ConnectionRequest{connection: conn, request: request}
		} else {
			link.Succeeded()
			link.ReturnConnection(conn)
		}
	}
}

func (s *Selector) responseDispatcher() {
	for connectionResponse := range s.responses {
		link := connectionResponse.request.link
		conn := connectionResponse.connection
		responseChan := connectionResponse.request.responseChan

		bytes, err := s.receive(conn)
		if err != nil {
			link.Failed()
			responseChan <- &rawResponseAndError{nil, link, err}
			link.ReturnConnection(conn)
			continue
		}

		link.Succeeded()
		link.ReturnConnection(conn)
		responseChan <- &rawResponseAndError{bytes, link, err}
	}
}

func (s *Selector) send(correlationID int32, conn *net.TCPConn, request siesta.Request) error {
	writer := siesta.NewRequestHeader(correlationID, s.config.ClientID, request)
	bytes := make([]byte, writer.Size())
	encoder := siesta.NewBinaryEncoder(bytes)
	writer.Write(encoder)

	conn.SetWriteDeadline(time.Now().Add(s.config.WriteTimeout))
	_, err := conn.Write(bytes)
	return err
}

func (s *Selector) receive(conn *net.TCPConn) ([]byte, error) {
	conn.SetReadDeadline(time.Now().Add(s.config.ReadTimeout))
	header := make([]byte, 8)
	_, err := io.ReadFull(conn, header)
	if err != nil {
		return nil, err
	}

	decoder := siesta.NewBinaryDecoder(header)
	length, err := decoder.GetInt32()
	if err != nil {
		return nil, err
	}
	response := make([]byte, length-4)
	_, err = io.ReadFull(conn, response)
	if err != nil {
		return nil, err
	}

	return response, nil
}

//TODO better struct name
type NetworkRequest struct {
	link         siesta.BrokerLink
	request      siesta.Request
	responseChan chan *rawResponseAndError
}

type rawResponseAndError struct {
	bytes []byte
	link  siesta.BrokerLink
	err   error
}
