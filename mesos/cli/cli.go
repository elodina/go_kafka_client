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

package main

import (
	"flag"
	"fmt"
	kafka "github.com/stealthly/go_kafka_client"
	"golang.org/x/crypto/ssh/terminal"
	"io"
	"os"
)

type shell struct {
	reader io.Reader
	writer io.Writer
}

func newShell() *shell {
	return &shell{reader: os.Stdin, writer: os.Stdout}
}

func (this *shell) Read(data []byte) (n int, err error) {
	return this.reader.Read(data)
}
func (this *shell) Write(data []byte) (n int, err error) {
	return this.writer.Write(data)
}

func main() {
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		fmt.Println("Usage: cli <host:port>")
		os.Exit(1)
	}

	endpointAddr := args[0]
	fmt.Printf("Connecting to %s... ", endpointAddr)

	cli, err := kafka.NewCli(endpointAddr)
	if err != nil {
		fmt.Println("NOT OK!")
		fmt.Println(err)
		os.Exit(1)
	}
	fmt.Println("OK!")

	fd := int(os.Stdin.Fd())
	oldState, err := terminal.MakeRaw(fd)
	defer terminal.Restore(fd, oldState)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	term := terminal.NewTerminal(newShell(), "> ")

	for {
		command, err := term.ReadLine()
		if err != nil {
			break
		}

		response := cli.HandleCommand(command)

		fmt.Println(response)
	}
}
