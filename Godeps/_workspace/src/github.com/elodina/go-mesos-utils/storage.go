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
	"errors"
	"fmt"
	"github.com/samuel/go-zookeeper/zk"
	"io/ioutil"
	"path"
	"strings"
	"time"
)

type Storage interface {
	Save([]byte) error
	Load() ([]byte, error)
}

type FileStorage struct {
	file string
}

func NewFileStorage(file string) *FileStorage {
	return &FileStorage{
		file: file,
	}
}

func (fs *FileStorage) Save(contents []byte) error {
	return ioutil.WriteFile(fs.file, contents, 0644)
}

func (fs *FileStorage) Load() ([]byte, error) {
	return ioutil.ReadFile(fs.file)
}

func (fs *FileStorage) String() string {
	return fmt.Sprintf("%s", fs.file)
}

type ZKStorage struct {
	zkConnect string
	zPath     string
}

func NewZKStorage(zk string) (*ZKStorage, error) {
	zkConnect := zk
	path := "/"
	chrootIdx := strings.Index(zk, "/")
	if chrootIdx != -1 {
		zkConnect = zk[:chrootIdx]
		path = zk[chrootIdx:]
	}

	storage := &ZKStorage{
		zkConnect: zkConnect,
		zPath:     path,
	}

	err := storage.createChrootIfRequired()
	return storage, err
}

func (zs *ZKStorage) Save(contents []byte) error {
	conn, err := zs.newZkClient()
	if err != nil {
		return err
	}
	defer conn.Close()

	_, stat, _ := conn.Get(zs.zPath)
	_, err = conn.Set(zs.zPath, contents, stat.Version)
	return err
}

func (zs *ZKStorage) Load() ([]byte, error) {
	conn, err := zs.newZkClient()
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	contents, _, err := conn.Get(zs.zPath)
	return contents, err
}

func (zs *ZKStorage) String() string {
	return fmt.Sprintf("%s%s", zs.zkConnect, zs.zPath)
}

func (zs *ZKStorage) createChrootIfRequired() error {
	if zs.zPath != "" {
		conn, err := zs.newZkClient()
		if err != nil {
			return err
		}
		defer conn.Close()

		err = zs.createZPath(conn, zs.zPath)
		if err != nil {
			return err
		}
	}

	return nil
}

func (zs *ZKStorage) newZkClient() (*zk.Conn, error) {
	conn, _, err := zk.Connect([]string{zs.zkConnect}, 30*time.Second)
	return conn, err
}

func (zs *ZKStorage) createZPath(conn *zk.Conn, zpath string) error {
	_, err := conn.Create(zpath, nil, 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		if zk.ErrNodeExists == err {
			return nil
		} else {
			parent, _ := path.Split(zpath)
			if len(parent) == 0 {
				return errors.New("Specified blank path")
			}
			err = zs.createZPath(conn, parent[:len(parent)-1])
			if err != nil {
				return err
			}

			_, err = conn.Create(zpath, nil, 0, zk.WorldACL(zk.PermAll))
			if err == zk.ErrNodeExists {
				err = nil
			}
		}
	}

	if zk.ErrNodeExists == err {
		return nil
	} else {
		return err
	}
}
