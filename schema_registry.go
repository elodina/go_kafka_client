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

package go_kafka_client

import (
	avro "github.com/stealthly/go-avro"
	"net/http"
)

type SchemaRegistryClient interface {
	Register(subject string, schema avro.Schema) int32
	GetByID(id int32) avro.Schema
	GetLatestSchemaMetadata(subject string) *SchemaMetadata
	GetVersion(subject string, schema avro.Schema) int32
}

type SchemaMetadata struct {
	Id      int32
	Version int32
	Schema  string
}

type CompatibilityLevel string

const (
	BackwardCompatibilityLevel CompatibilityLevel = "BACKWARD"
	ForwardCompatibilityLevel CompatibilityLevel  = "FORWARD"
	FullCompatibilityLevel CompatibilityLevel     = "FULL"
	NoneCompatibilityLevel CompatibilityLevel     = "NONE"
)

type Version string

const (
	SCHEMA_REGISTRY_V1_JSON Version               = "application/vnd.schemaregistry.v1+json"
	SCHEMA_REGISTRY_V1_JSON_WEIGHTED Version      = "application/vnd.schemaregistry.v1+json"
	SCHEMA_REGISTRY_MOST_SPECIFIC_DEFAULT Version = "application/vnd.schemaregistry.v1+json"
	SCHEMA_REGISTRY_DEFAULT_JSON Version          = "application/vnd.schemaregistry+json"
	SCHEMA_REGISTRY_DEFAULT_JSON_WEIGHTED Version = "application/vnd.schemaregistry+json qs=0.9"
	JSON Version                                  = "application/json"
	JSON_WEIGHTED Version                         = "application/json qs=0.5"
	GENERIC_REQUEST Version                       = "application/octet-stream"
)

var PREFERRED_RESPONSE_TYPES = [...]Version {SCHEMA_REGISTRY_V1_JSON, SCHEMA_REGISTRY_DEFAULT_JSON, JSON}

type ErrorMessage struct {
	ErrorCode int32
	Message   string
}

type Schema struct {
	Subject string
	Version int32
	Id      int32
	Schema  string
}

type CachedSchemaRegistryClient struct {
	RegistryURL string
	httpClient  *http.Client
}

func NewCachedSchemaRegistryClient(registryURL string) *CachedSchemaRegistryClient {
	return &CachedSchemaRegistryClient{
		RegistryURL: registryURL,
		httpClient: http.DefaultClient,
	}
}

func (this *CachedSchemaRegistryClient) Register(subject string, schema avro.Schema) int32 {
	return 0
}

func (this *CachedSchemaRegistryClient) GetByID(id int32) avro.Schema {
	return nil
}

func (this *CachedSchemaRegistryClient) GetLatestSchemaMetadata(subject string) *SchemaMetadata {
	return nil
}

func (this *CachedSchemaRegistryClient) GetVersion(subject string, schema avro.Schema) int32 {
	return 0
}
