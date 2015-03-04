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
	"net"
	"time"
)

type Cli struct {
	addr *net.TCPAddr
}

func NewCli(addr string) (*Cli, error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return nil, err
	}

	cli := &Cli{tcpAddr}
	if err := cli.validateEndpoint(); err != nil {
		return nil, err
	}

	return cli, nil
}

func (this *Cli) HandleCommand(command string) string {
	//TODO actual handling
	return "Unknown command, please try again"
}

func (this *Cli) validateEndpoint() error {
	request := NewMesosRequest(requestKey_Handshake)
	response := &GenericResponse{}
	if err := this.sendAndReceive(request, response); err != nil {
		return err
	}

	return response.Error()
}

func (this *Cli) sendAndReceive(request *MesosRequest, response MesosResponse) error {
	conn, err := net.DialTCP("tcp", nil, this.addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	//TODO this should be configurable?
	conn.SetWriteDeadline(time.Now().Add(1 * time.Second))
	encoder := gob.NewEncoder(conn)
	if err = encoder.Encode(request); err != nil {
		return err
	}

	conn.SetReadDeadline(time.Now().Add(1 * time.Second))
	decoder := gob.NewDecoder(conn)
	if err = decoder.Decode(response); err != nil {
		return err
	}

	return nil
}
