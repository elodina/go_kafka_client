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
	"github.com/rcrowley/go-metrics"
	"github.com/stealthly/go-avro"
	avroline "github.com/stealthly/go_kafka_client/avro"
	"regexp"
	"strings"
	"time"
)

// MetricsEmitter is an interface that handles structured metrics data.
type MetricsEmitter interface {
	// ReportingInterval returns an interval to report metrics.
	ReportingInterval() time.Duration

	// Emit sends a single metrics entry to some destination.
	Emit([]byte)
}

// KafkaMetricsEmitterConfig provides multiple configuration entries for KafkaMetricsEmitter
type KafkaMetricsEmitterConfig struct {
	// Registry is a metrics registry to report.
	Registry metrics.Registry

	// ReportingInterval is an interval to report metrics.
	ReportingInterval time.Duration

	// Topic to emit logs to.
	Topic string

	// Confluent Avro schema registry URL.
	SchemaRegistryUrl string

	// Producer config that will be used by this emitter. Note that ValueEncoder WILL BE replaced by KafkaAvroEncoder.
	ProducerConfig *ProducerConfig
}

// NewKafkaMetricsEmitterConfig creates a new KafkaMetricsEmitterConfig with ReportingInterval set to 1 second.
func NewKafkaMetricsEmitterConfig() *KafkaMetricsEmitterConfig {
	return &KafkaMetricsEmitterConfig{
		ReportingInterval: 1 * time.Second,
	}
}

// KafkaMetricsEmitter implements MetricsEmitter and sends all metrics data to a Kafka topic encoded as Avro.
type KafkaMetricsEmitter struct {
	config   *KafkaMetricsEmitterConfig
	producer Producer
}

// NewKafkaMetricsEmitter creates a new KafkaMetricsEmitter with a provided KafkaMetricsEmitterConfig.
func NewKafkaMetricsEmitter(config *KafkaMetricsEmitterConfig) *KafkaMetricsEmitter {
	config.ProducerConfig.ValueEncoder = NewKafkaAvroEncoder(config.SchemaRegistryUrl)
	return &KafkaMetricsEmitter{
		config:   config,
		producer: NewSaramaProducer(config.ProducerConfig),
	}
}

// ReportingInterval returns an interval to report metrics.
func (m *KafkaMetricsEmitter) ReportingInterval() time.Duration {
	return m.config.ReportingInterval
}

// Emit sends a single metrics entry to a Kafka topic encoded as Avro.
func (m *KafkaMetricsEmitter) Emit(bytes []byte) {
	metrics := make(map[string]interface{})
	if err := json.Unmarshal(bytes, &metrics); err != nil {
		panic(err)
	}

	schema, lookupNames := m.parseSchema(metrics)
	record := avro.NewGenericRecord(schema)
	m.fillRecord(record, schema.(*avro.RecordSchema), metrics, lookupNames)

	m.producer.Input() <- &ProducerMessage{Topic: m.config.Topic, Value: record}
}

func (m *KafkaMetricsEmitter) parseSchema(metrics map[string]interface{}) (avro.Schema, map[string]string) {
	lookupNames := make(map[string]string)
	schema := &avro.RecordSchema{
		Name:      "Metrics",
		Namespace: "ly.stealth",
		Fields:    make([]*avro.SchemaField, 0),
	}
	for name, v := range metrics {
		lookupName := m.getLookupName(name)
		lookupNames[lookupName] = name

		field := &avro.SchemaField{
			Name:    lookupName,
			Default: nil,
			Type: &avro.UnionSchema{
				Types: []avro.Schema{&avro.NullSchema{}, m.parseRecordField(name, v.(map[string]interface{}), lookupNames)},
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

func (m *KafkaMetricsEmitter) getLookupName(name string) string {
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

func (m *KafkaMetricsEmitter) parseRecordField(name string, v map[string]interface{}, lookupNames map[string]string) *avro.RecordSchema {
	lookupRecordName := m.getLookupName(name)
	lookupNames[lookupRecordName] = name
	record := &avro.RecordSchema{
		Name:      lookupRecordName,
		Namespace: "ly.stealth",
		Fields:    make([]*avro.SchemaField, 0),
	}

	for name, value := range v {
		lookupName := m.getLookupName(name)
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

func (m *KafkaMetricsEmitter) fillRecord(record *avro.GenericRecord, schema *avro.RecordSchema, metrics map[string]interface{}, lookupNames map[string]string) {
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
					m.fillRecord(fieldRecord, fieldSchema, metrics[name].(map[string]interface{}), lookupNames)
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

// EmptyMetricsEmitter implements MetricsEmitter and ignores all incoming data. Used not to break anyone.
type EmptyMetricsEmitter struct{}

// NewEmptyMetricsEmitter creates a new EmptyMetricsEmitter.
func NewEmptyMetricsEmitter() *EmptyMetricsEmitter {
	return new(EmptyMetricsEmitter)
}

// Does nothing. Ignores given message.
func (*EmptyMetricsEmitter) Emit([]byte) {}

// Returns a reporting interval of 1 minute.
func (*EmptyMetricsEmitter) ReportingInterval() time.Duration {
	return time.Minute
}
