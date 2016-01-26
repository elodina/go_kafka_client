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
	"github.com/elodina/go-avro"
	"io/ioutil"
	"os"
	"strings"
)

type schemas []string

func (i *schemas) String() string {
	return fmt.Sprintf("%s", *i)
}

func (i *schemas) Set(value string) error {
	*i = append(*i, value)
	return nil
}

var schema schemas
var output = flag.String("out", "", "Output file name.")

func main() {
	parseAndValidateArgs()

	schemas := make([]string, 0)
	for _, schema := range schema {
		contents, err := ioutil.ReadFile(schema)
		checkErr(err)
		schemas = append(schemas, string(contents))
	}

	gen := avro.NewCodeGenerator(schemas)
	code, err := gen.Generate()
	checkErr(err)

	createDirs()
	err = ioutil.WriteFile(*output, []byte(code), 0664)
	checkErr(err)
}

func parseAndValidateArgs() {
	flag.Var(&schema, "schema", "Path to avsc schema file.")
	flag.Parse()

	if len(schema) == 0 {
		fmt.Println("At least one --schema flag is required.")
		os.Exit(1)
	}

	if *output == "" {
		fmt.Println("--out flag is required.")
		os.Exit(1)
	}
}

func createDirs() {
	index := strings.LastIndex(*output, "/")
	if index != -1 {
		path := (*output)[:index]
		err := os.MkdirAll(path, 0777)
		checkErr(err)
	}
}

func checkErr(err error) {
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
