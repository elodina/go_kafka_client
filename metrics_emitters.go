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
	"encoding/json"
	"fmt"
	"github.com/elodina/go-avro"
	kafkaavro "github.com/elodina/go-kafka-avro"
	avroline "github.com/elodina/go_kafka_client/avro"
	"github.com/elodina/siesta"
	"github.com/elodina/siesta-producer"
	"regexp"
	"strings"
)

type CodahaleKafkaReporter struct {
	topic    string
	producer producer.Producer
}

func NewCodahaleKafkaReporter(topic string, schemaRegistryUrl string, producerConfig *producer.ProducerConfig, connectorConfig *siesta.ConnectorConfig) (*CodahaleKafkaReporter, error) {
	encoder := kafkaavro.NewKafkaAvroEncoder(schemaRegistryUrl)
	connector, err := siesta.NewDefaultConnector(connectorConfig)
	if err != nil {
		return nil, err
	}

	return &CodahaleKafkaReporter{
		topic:    topic,
		producer: producer.NewKafkaProducer(producerConfig, producer.ByteSerializer, encoder.Encode, connector),
	}, nil
}

func (c *CodahaleKafkaReporter) Write(bytes []byte) (n int, err error) {
	metrics := make(map[string]interface{})
	if err := json.Unmarshal(bytes, &metrics); err != nil {
		return -1, err
	}

	schema, lookupNames := c.parseSchema(metrics)
	record := avro.NewGenericRecord(schema)
	c.fillRecord(record, schema.(*avro.RecordSchema), metrics, lookupNames)

	c.producer.Send(&producer.ProducerRecord{
		Topic: c.topic,
		Value: record,
	})

	return 0, nil
}

func (c *CodahaleKafkaReporter) parseSchema(metrics map[string]interface{}) (avro.Schema, map[string]string) {
	lookupNames := make(map[string]string)
	schema := &avro.RecordSchema{
		Name:      "Metrics",
		Namespace: "net.elodina",
		Fields:    make([]*avro.SchemaField, 0),
	}
	for name, v := range metrics {
		lookupName := c.getLookupName(name)
		lookupNames[lookupName] = name

		field := &avro.SchemaField{
			Name:    lookupName,
			Default: nil,
			Type: &avro.UnionSchema{
				Types: []avro.Schema{&avro.NullSchema{}, c.parseRecordField(name, v.(map[string]interface{}), lookupNames)},
			},
		}
		schema.Fields = append(schema.Fields, field)
	}
	schema.Fields = append(schema.Fields, &avro.SchemaField{
		Name: "logLine",
		Type: avroline.NewLogLine().Schema(),
	})

	return schema, lookupNames
}

func (c *CodahaleKafkaReporter) getLookupName(name string) string {
	lookupName := name
	pattern := regexp.MustCompile("[0-9]+")
	if pattern.MatchString(name) {
		lookupName = fmt.Sprintf("metric%s", name)
	}
	lookupName = strings.Replace(lookupName, ".", "", -1)
	lookupName = strings.Replace(lookupName, "%", "", -1)
	lookupName = strings.Replace(lookupName, "-", "", -1)

	return lookupName
}

func (c *CodahaleKafkaReporter) parseRecordField(name string, v map[string]interface{}, lookupNames map[string]string) *avro.RecordSchema {
	lookupRecordName := c.getLookupName(name)
	lookupNames[lookupRecordName] = name
	record := &avro.RecordSchema{
		Name:      lookupRecordName,
		Namespace: "net.elodina",
		Fields:    make([]*avro.SchemaField, 0),
	}

	for name, value := range v {
		lookupName := c.getLookupName(name)
		lookupNames[lookupName] = name

		var valueSchema avro.Schema
		switch value.(type) {
		case string:
			valueSchema = &avro.StringSchema{}
		case float64:
			valueSchema = &avro.DoubleSchema{}
		}

		field := &avro.SchemaField{
			Name:    lookupName,
			Default: nil,
			Type: &avro.UnionSchema{
				Types: []avro.Schema{&avro.NullSchema{}, valueSchema},
			},
		}
		record.Fields = append(record.Fields, field)
	}

	return record
}

func (c *CodahaleKafkaReporter) fillRecord(record *avro.GenericRecord, schema *avro.RecordSchema, metrics map[string]interface{}, lookupNames map[string]string) {
	for _, field := range schema.Fields {
		if field.Name == "logLine" {
			//TODO probably something more?
			logLine := avro.NewGenericRecord(field.Type)
			logLine.Set("source", "metrics")
			logLine.Set("logtypeid", MetricsLogTypeId)
			record.Set("logLine", logLine)
		} else {
			name := lookupNames[field.Name]
			switch fieldSchema := field.Type.(*avro.UnionSchema).Types[1].(type) {
			case *avro.RecordSchema:
				{
					fieldRecord := avro.NewGenericRecord(fieldSchema)
					c.fillRecord(fieldRecord, fieldSchema, metrics[name].(map[string]interface{}), lookupNames)
					record.Set(field.Name, fieldRecord)
				}
			default:
				{
					record.Set(field.Name, metrics[field.Name])
				}
			}
		}
	}
}

type KafkaMetricReporter struct {
	topic    string
	producer producer.Producer
}

func NewKafkaMetricReporter(topic string, producerConfig *producer.ProducerConfig, connectorConfig *siesta.ConnectorConfig) (*KafkaMetricReporter, error) {
	connector, err := siesta.NewDefaultConnector(connectorConfig)
	if err != nil {
		return nil, err
	}

	return &KafkaMetricReporter{
		topic:    topic,
		producer: producer.NewKafkaProducer(producerConfig, producer.ByteSerializer, producer.ByteSerializer, connector),
	}, nil
}

func (k *KafkaMetricReporter) Write(bytes []byte) (n int, err error) {
	k.producer.Send(&producer.ProducerRecord{
		Topic: k.topic,
		Value: bytes,
	})

	return len(bytes), nil
}
