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

package avro

import (
	"io/ioutil"
	"strings"
)

const schemaExtension = ".avsc"

// Loads and parses a schema file or directory.
// Directory names MUST end with "/"
func LoadSchemas(path string) map[string]Schema {
	files := getFiles(path, make([]string, 0))

	schemas := make(map[string]Schema)

	if files != nil {
		for _, file := range files {
			if _, err := loadSchema(path, file, schemas); err != nil {
				return make(map[string]Schema)
			}
		}
	}

	return schemas
}

func getFiles(path string, files []string) []string {
	list, err := ioutil.ReadDir(path)
	if err != nil {
		return nil
	}

	for _, file := range list {
		if file.IsDir() {
			files = getFiles(path+file.Name()+"/", files)
			if files == nil {
				return nil
			}
		} else if file.Mode().IsRegular() {
			if strings.HasSuffix(file.Name(), schemaExtension) {
				files = addFile(path+file.Name(), files)
			}
		}
	}

	return files
}

func addFile(path string, files []string) []string {
	n := len(files)
	if n == cap(files) {
		newFiles := make([]string, len(files), 2*len(files)+1)
		copy(newFiles, files)
		files = newFiles
	}

	files = files[0 : n+1]
	files[n] = path

	return files
}

func loadSchema(basePath, avscPath string, schemas map[string]Schema) (Schema, error) {
	avscJson, err := ioutil.ReadFile(avscPath)

	if err != nil {
		return nil, err
	}

	var sch Schema
	for {
		sch, err = ParseSchemaWithRegistry(string(avscJson), schemas)

		if err != nil {
			text := err.Error()
			if strings.HasPrefix(text, "Undefined schema:") {
				typ := text[18:len(text)]
				path := basePath + strings.Replace(typ, ".", "/", -1) + schemaExtension

				_, errDep := loadSchema(basePath, path, schemas)

				if errDep != nil {
					return nil, errDep
				}

				continue
			}

			return nil, err
		}

		return sch, nil
	}
}
