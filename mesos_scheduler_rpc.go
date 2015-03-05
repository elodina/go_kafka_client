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
	"encoding/gob"
	"fmt"
	"net"
)

type MesosTCPEndpoint struct {
	addr    *net.TCPAddr
	handler func(*MesosRequest) MesosResponse
	close   chan bool
}

func NewMesosTCPEndpoint(addr *net.TCPAddr, handler func(*MesosRequest) MesosResponse) *MesosTCPEndpoint {
	endpoint := &MesosTCPEndpoint{
		addr:    addr,
		handler: handler,
		close:   make(chan bool, 1),
	}

	go endpoint.startTCPServer()

	return endpoint
}

func (this *MesosTCPEndpoint) String() string {
	return fmt.Sprintf("Mesos TCP Endpoint at %s", this.addr)
}

func (this *MesosTCPEndpoint) Close() {
	this.close <- true
}

func (this *MesosTCPEndpoint) startTCPServer() {
	Trace(this, "Starting TCP server")

	listener, err := net.ListenTCP("tcp", this.addr)
	if err != nil {
		panic(err)
	}

	go func() {
		for {
			select {
			case <-this.close:
				return
			default:
			}
			connection, err := listener.Accept()
			if err != nil {
				return
			}

			response := this.handleRequest(connection)
			this.respond(connection, response)
		}
	}()
	Infof(this, "Listening for messages at TCP %s", this.addr)
}

func (this *MesosTCPEndpoint) handleRequest(connection net.Conn) MesosResponse {
	decoder := gob.NewDecoder(connection)
	request := new(MesosRequest)
	err := decoder.Decode(request)
	if err != nil {
		return NewGenericResponse(err)
	}

	return this.handler(request)
}

func (this *MesosTCPEndpoint) respond(connection net.Conn, response MesosResponse) {
	encoder := gob.NewEncoder(connection)
	err := encoder.Encode(response)
	if err != nil {
		Errorf(this, "Failed to respond: %s", err)
	}
}

const (
	requestKey_Handshake int16 = 0
)

type MesosRequest struct {
	Key int16
}

func NewMesosRequest(key int16) *MesosRequest {
	return &MesosRequest{key}
}

type MesosResponse interface {
	Error() error
}

type GenericResponse struct {
	Err error
}

func NewGenericResponse(err error) *GenericResponse {
	return &GenericResponse{err}
}

func (this *GenericResponse) Error() error {
	return this.Err
}
