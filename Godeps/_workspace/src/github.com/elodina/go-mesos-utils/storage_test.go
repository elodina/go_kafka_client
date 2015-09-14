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

package utils

import (
	"fmt"
	"github.com/samuel/go-zookeeper/zk"
	"os"
	"strings"
	"testing"
	"time"
)

func TestFileStorage(t *testing.T) {
	file := "tmp_file_storage.txt"
	contents := "hello world"
	defer func() {
		os.Remove(file)
	}()

	storage := NewFileStorage(file)
	err := storage.Save([]byte(contents))
	if err != nil {
		t.Fatalf("Error while saving to file storage: %s", err)
	}
	loadedContents, err := storage.Load()
	if err != nil {
		t.Fatalf("Error while loading from file storage: %s", err)
	}

	if string(loadedContents) != contents {
		t.Errorf("Expected contents %s, actual %s", contents, string(loadedContents))
	}

	if storage.String() != file {
		t.Errorf("Expected string representation %s, actual %s", file, storage.String())
	}
}

func TestZKStorage(t *testing.T) {
	zkConnect := "localhost:2181"
	zpath := "/tmp/zk/storage"
	contents := "hello world"

	conn, _, err := zk.Connect([]string{zkConnect}, 30*time.Second)
	_, _, err = conn.Exists("/tmp") // check if zk is alive
	if err != nil {
		t.Skipf("localhost:2181 is not responding (error %s). To run this test please spin up ZK on localhost:2181", err)
	}
	defer conn.Close()

	storage, err := NewZKStorage(fmt.Sprintf("%s%s", zkConnect, zpath))
	if err != nil {
		t.Fatal(err)
	}

	err = storage.Save([]byte(contents))
	if err != nil {
		t.Fatal(err)
	}

	loadedContents, err := storage.Load()
	if err != nil {
		t.Fatal(err)
	}

	if string(loadedContents) != contents {
		t.Errorf("Expected contents %s, actual %s", contents, string(loadedContents))
	}

	err = zkDelete(conn, zpath)
	if err != nil {
		t.Fatal(err)
	}

	if storage.String() != fmt.Sprintf("%s%s", zkConnect, zpath) {
		t.Errorf("Expected string representation %s, actual %s", fmt.Sprintf("%s%s", zkConnect, zpath), storage.String())
	}
}

func zkDelete(conn *zk.Conn, zpath string) error {
	if zpath != "" {
		_, stat, _ := conn.Get(zpath)
		err := conn.Delete(zpath, stat.Version)
		if err != nil {
			return err
		}

		index := strings.LastIndex(zpath, "/")
		if index != -1 {
			return zkDelete(conn, zpath[:index])
		} else {
			return nil
		}
	} else {
		return nil
	}
}
