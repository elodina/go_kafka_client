/* Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License") you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. */

package avro

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"

	avro "github.com/elodina/go-avro"
	"sync"
)

const (
	GET_SCHEMA_BY_ID             = "/schemas/ids/%d"
	GET_SUBJECTS                 = "/subjects"
	GET_SUBJECT_VERSIONS         = "/subjects/%s/versions"
	GET_SPECIFIC_SUBJECT_VERSION = "/subjects/%s/versions/%s"
	REGISTER_NEW_SCHEMA          = "/subjects/%s/versions"
	CHECK_IS_REGISTERED          = "/subjects/%s"
	TEST_COMPATIBILITY           = "/compatibility/subjects/%s/versions/%s"
	CONFIG                       = "/config"
)

type SchemaRegistryClient interface {
	Register(subject string, schema avro.Schema) (int32, error)
	GetByID(id int32) (avro.Schema, error)
	GetLatestSchemaMetadata(subject string) (*SchemaMetadata, error)
	GetVersion(subject string, schema avro.Schema) (int32, error)
}

type SchemaMetadata struct {
	Id      int32
	Version int32
	Schema  string
}

type CompatibilityLevel string

const (
	BackwardCompatibilityLevel CompatibilityLevel = "BACKWARD"
	ForwardCompatibilityLevel  CompatibilityLevel = "FORWARD"
	FullCompatibilityLevel     CompatibilityLevel = "FULL"
	NoneCompatibilityLevel     CompatibilityLevel = "NONE"
)

const (
	SCHEMA_REGISTRY_V1_JSON               = "application/vnd.schemaregistry.v1+json"
	SCHEMA_REGISTRY_V1_JSON_WEIGHTED      = "application/vnd.schemaregistry.v1+json"
	SCHEMA_REGISTRY_MOST_SPECIFIC_DEFAULT = "application/vnd.schemaregistry.v1+json"
	SCHEMA_REGISTRY_DEFAULT_JSON          = "application/vnd.schemaregistry+json"
	SCHEMA_REGISTRY_DEFAULT_JSON_WEIGHTED = "application/vnd.schemaregistry+json qs=0.9"
	JSON                                  = "application/json"
	JSON_WEIGHTED                         = "application/json qs=0.5"
	GENERIC_REQUEST                       = "application/octet-stream"
)

var PREFERRED_RESPONSE_TYPES = []string{SCHEMA_REGISTRY_V1_JSON, SCHEMA_REGISTRY_DEFAULT_JSON, JSON}

type ErrorMessage struct {
	Error_code int32
	Message    string
}

func (this *ErrorMessage) Error() string {
	return fmt.Sprintf("%s(error code: %d)", this.Message, this.Error_code)
}

type RegisterSchemaResponse struct {
	Id int32
}

type GetSchemaResponse struct {
	Schema string
}

type GetSubjectVersionResponse struct {
	Subject string
	Version int32
	Id      int32
	Schema  string
}

type CachedSchemaRegistryClient struct {
	registryURL  string
	schemaCache  map[string]map[avro.Schema]int32
	idCache      map[int32]avro.Schema
	versionCache map[string]map[avro.Schema]int32
	lock         sync.RWMutex
}

func NewCachedSchemaRegistryClient(registryURL string) *CachedSchemaRegistryClient {
	return &CachedSchemaRegistryClient{
		registryURL:  registryURL,
		schemaCache:  make(map[string]map[avro.Schema]int32),
		idCache:      make(map[int32]avro.Schema),
		versionCache: make(map[string]map[avro.Schema]int32),
	}
}

func (this *CachedSchemaRegistryClient) Register(subject string, schema avro.Schema) (int32, error) {
	var schemaIdMap map[avro.Schema]int32
	var exists bool

	this.lock.RLock()
	if schemaIdMap, exists = this.schemaCache[subject]; exists {
		var id int32
		if id, exists = schemaIdMap[schema]; exists {
			return id, nil
		}
	}
	this.lock.RUnlock()

	this.lock.Lock()
	defer this.lock.Unlock()
	if schemaIdMap, exists = this.schemaCache[subject]; !exists {
		schemaIdMap = make(map[avro.Schema]int32)
		this.schemaCache[subject] = schemaIdMap
	}

	request, err := this.newDefaultRequest("POST",
		fmt.Sprintf(REGISTER_NEW_SCHEMA, subject),
		strings.NewReader(fmt.Sprintf("{\"schema\": %s}", strconv.Quote(schema.String()))))
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return 0, err
	}

	if this.isOK(response) {
		decodedResponse := &RegisterSchemaResponse{}
		if err := this.handleSuccess(response, decodedResponse); err != nil {
			return 0, err
		}

		schemaIdMap[schema] = decodedResponse.Id
		this.idCache[decodedResponse.Id] = schema

		return decodedResponse.Id, err
	} else {
		return 0, this.handleError(response)
	}
}

func (this *CachedSchemaRegistryClient) GetByID(id int32) (avro.Schema, error) {
	var schema avro.Schema
	var exists bool
	if schema, exists = this.idCache[id]; exists {
		return schema, nil
	}

	request, err := this.newDefaultRequest("GET", fmt.Sprintf(GET_SCHEMA_BY_ID, id), nil)
	if err != nil {
		return nil, err
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, err
	}

	if this.isOK(response) {
		decodedResponse := &GetSchemaResponse{}
		if err := this.handleSuccess(response, decodedResponse); err != nil {
			return nil, err
		}
		schema, err := avro.ParseSchema(decodedResponse.Schema)
		this.idCache[id] = schema

		return schema, err
	} else {
		return nil, this.handleError(response)
	}
}

func (this *CachedSchemaRegistryClient) GetLatestSchemaMetadata(subject string) (*SchemaMetadata, error) {
	request, err := this.newDefaultRequest("GET", fmt.Sprintf(GET_SPECIFIC_SUBJECT_VERSION, subject, "latest"), nil)
	if err != nil {
		return nil, err
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, err
	}

	if this.isOK(response) {
		decodedResponse := &GetSubjectVersionResponse{}
		if err := this.handleSuccess(response, decodedResponse); err != nil {
			return nil, err
		}

		return &SchemaMetadata{decodedResponse.Id, decodedResponse.Version, decodedResponse.Schema}, err
	} else {
		return nil, this.handleError(response)
	}
}

func (this *CachedSchemaRegistryClient) GetVersion(subject string, schema avro.Schema) (int32, error) {
	var schemaVersionMap map[avro.Schema]int32
	var exists bool
	if schemaVersionMap, exists = this.versionCache[subject]; !exists {
		schemaVersionMap = make(map[avro.Schema]int32)
		this.versionCache[subject] = schemaVersionMap
	}

	var version int32
	if version, exists = schemaVersionMap[schema]; exists {
		return version, nil
	}

	request, err := this.newDefaultRequest("POST",
		fmt.Sprintf(CHECK_IS_REGISTERED, subject),
		strings.NewReader(fmt.Sprintf("{\"schema\": %s}", strconv.Quote(schema.String()))))
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return 0, err
	}

	if this.isOK(response) {
		decodedResponse := &GetSubjectVersionResponse{}
		if err := this.handleSuccess(response, decodedResponse); err != nil {
			return 0, err
		}
		schemaVersionMap[schema] = decodedResponse.Version

		return decodedResponse.Version, err
	} else {
		return 0, this.handleError(response)
	}
}

func (this *CachedSchemaRegistryClient) newDefaultRequest(method string, uri string, reader io.Reader) (*http.Request, error) {
	url := fmt.Sprintf("%s%s", this.registryURL, uri)
	request, err := http.NewRequest(method, url, reader)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Accept", SCHEMA_REGISTRY_V1_JSON)
	request.Header.Set("Content-Type", SCHEMA_REGISTRY_V1_JSON)
	return request, nil
}

func (this *CachedSchemaRegistryClient) isOK(response *http.Response) bool {
	return response.StatusCode >= 200 && response.StatusCode < 300
}

func (this *CachedSchemaRegistryClient) handleSuccess(response *http.Response, model interface{}) error {
	responseBytes, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return err
	}
	return json.Unmarshal(responseBytes, model)
}

func (this *CachedSchemaRegistryClient) handleError(response *http.Response) error {
	registryError := &ErrorMessage{}
	responseBytes, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return err
	}
	err = json.Unmarshal(responseBytes, registryError)
	if err != nil {
		return err
	}

	return registryError
}
