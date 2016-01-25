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

package siesta

import (
	"testing"
	"time"
)

func TestConnectionPoolBorrowedIsConnected(t *testing.T) {
	request := randomBytes(10)
	resultChannel := make(chan []byte)
	listener := awaitForTCPRequestAndReturn(t, len(request), resultChannel)

	pool := newConnectionPool(listener.Addr().String(), 1, true, 1*time.Second)
	conn, err := pool.Borrow()
	if err != nil {
		t.Error(err)
	}
	go conn.Write(request)
	select {
	case result := <-resultChannel:
		{
			assert(t, result, request)
		}
	case <-time.After(3 * time.Second):
		t.Error("Timed out")
	}

	listener.Close()
}

func TestConnectionPoolReturnedIsReusable(t *testing.T) {
	listener := startTCPListener(t)
	pool := newConnectionPool(listener.Addr().String(), 1, true, 1*time.Second)
	conn, err := pool.Borrow()
	if err != nil {
		t.Error(err)
	}
	pool.Return(conn)

	conn, err = pool.Borrow()
	if err != nil {
		t.Error(err)
	}
	listener.Close()
}

func TestConnectionPoolBorrowMoreThanAllowed(t *testing.T) {
	listener := startTCPListener(t)
	pool := newConnectionPool(listener.Addr().String(), 2, true, 1*time.Second)
	conn, err := pool.Borrow()
	if err != nil {
		t.Error(err)
	}
	_, err = pool.Borrow()
	if err != nil {
		t.Error(err)
	}

	borrowed := make(chan bool)
	go func() {
		pool.Borrow()
		borrowed <- true
	}()

	select {
	case <-borrowed:
		t.Errorf("You shouldn't be able to borrow more connections from pool than you are allowed to (allowed: %d, taken: %d)", pool.size, pool.conns)
	case <-time.After(100 * time.Millisecond):
	}

	pool.Return(conn)

	select {
	case <-borrowed:
	case <-time.After(100 * time.Millisecond):
		t.Error("Unable to borrow connection from pool after it was returned")
	}
	listener.Close()
}

func TestConnectionPoolReturnMoreThanAllowed(t *testing.T) {
	listener := startTCPListener(t)
	size := 1
	pool := newConnectionPool(listener.Addr().String(), size, true, 1*time.Second)
	conn, err := pool.Borrow()
	if err != nil {
		t.Error(err)
	}

	pool.Return(conn)
	pool.Return(conn)

	assert(t, len(pool.connections), size)
	listener.Close()
}

func TestConnectionPoolBadAddresses(t *testing.T) {
	pool := newConnectionPool("localhost", 1, true, 1*time.Second)
	conn, err := pool.Borrow()
	if err == nil {
		t.Error("Should not be able to borrow connection to a host without a port")
		conn.Close()
	}

	addr := "localhost:0"
	pool = newConnectionPool(addr, 1, true, 1*time.Second)
	conn, err = pool.Borrow()
	if err == nil {
		t.Errorf("Should not be able to connect to %s", addr)
		conn.Close()
	}
}
