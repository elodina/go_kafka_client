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
	"fmt"
	"github.com/stealthly/go_kafka_client/avro"
	"time"
)

const (
	/* Logtypeid field value for LogLine indicating Trace log level */
	TraceLogTypeId int64 = 1

	/* Logtypeid field value for LogLine indicating Debug log level */
	DebugLogTypeId int64 = 2

	/* Logtypeid field value for LogLine indicating Info log level */
	InfoLogTypeId int64 = 3

	/* Logtypeid field value for LogLine indicating Warn log level */
	WarnLogTypeId int64 = 4

	/* Logtypeid field value for LogLine indicating Error log level */
	ErrorLogTypeId int64 = 5

	/* Logtypeid field value for LogLine indicating Critical log level */
	CriticalLogTypeId int64 = 6

	/* Logtypeid field value for LogLine indicating Metrics data */
	MetricsLogTypeId int64 = 7
)

var logLevels = map[LogLevel]int64{
	TraceLevel:    TraceLogTypeId,
	DebugLevel:    DebugLogTypeId,
	InfoLevel:     InfoLogTypeId,
	WarnLevel:     WarnLogTypeId,
	ErrorLevel:    ErrorLogTypeId,
	CriticalLevel: CriticalLogTypeId,
}

// LogEmitter is an interface that handles structured log data.
type LogEmitter interface {
	// Emit sends a single LogLine to some destination.
	Emit(*avro.LogLine)

	// Close closes the given emitter.
	Close()
}

// KafkaLogEmitterConfig provides multiple configuration entries for KafkaLogEmitter.
type KafkaLogEmitterConfig struct {
	// LogLevel to use for KafkaLogEmitter
	LogLevel LogLevel

	// LogLine event source. Used only when called as a Logger interface.
	Source string

	// LogLine tags. Used only when called as a logger interface.
	Tags map[string]string

	// Topic to emit logs to.
	Topic string

	// Confluent Avro schema registry URL.
	SchemaRegistryUrl string

	// Producer config that will be used by this emitter. Note that ValueEncoder WILL BE replaced by KafkaAvroEncoder.
	ProducerConfig *ProducerConfig
}

// NewKafkaLogEmitterConfig creates a new KafkaLogEmitterConfig with log level set to Info.
func NewKafkaLogEmitterConfig() *KafkaLogEmitterConfig {
	return &KafkaLogEmitterConfig{
		LogLevel: InfoLevel,
	}
}

// KafkaLogEmitter implements LogEmitter and KafkaLogger and sends all structured log data to a Kafka topic encoded as Avro.
type KafkaLogEmitter struct {
	config   *KafkaLogEmitterConfig
	producer Producer
}

// NewKafkaLogEmitter creates a new KafkaLogEmitter with a provided configuration.
func NewKafkaLogEmitter(config *KafkaLogEmitterConfig) *KafkaLogEmitter {
	config.ProducerConfig.ValueEncoder = NewKafkaAvroEncoder(config.SchemaRegistryUrl)
	emitter := &KafkaLogEmitter{
		config:   config,
		producer: NewSaramaProducer(config.ProducerConfig),
	}

	return emitter
}

// Emit emits a single entry to a given destination topic.
func (k *KafkaLogEmitter) Emit(logLine *avro.LogLine) {
	if logLine.Logtypeid.(int64) >= logLevels[k.config.LogLevel] {
		k.producer.Input() <- &ProducerMessage{Topic: k.config.Topic, Value: logLine}
	}
}

// Close closes the underlying producer. The KafkaLogEmitter won't be usable anymore after call to this.
func (k *KafkaLogEmitter) Close() {
	k.producer.Close()
}

// Trace formats a given message according to given params to log with level Trace and produces to a given Kafka topic.
func (k *KafkaLogEmitter) Trace(message string, params ...interface{}) {
	k.Emit(newLogLine(k.config.Source, TraceLogTypeId, fmt.Sprintf(message, params), k.config.Tags))
}

// Debug formats a given message according to given params to log with level Debug and produces to a given Kafka topic.
func (k *KafkaLogEmitter) Debug(message string, params ...interface{}) {
	k.Emit(newLogLine(k.config.Source, DebugLogTypeId, fmt.Sprintf(message, params), k.config.Tags))
}

// Info formats a given message according to given params to log with level Info and produces to a given Kafka topic.
func (k *KafkaLogEmitter) Info(message string, params ...interface{}) {
	k.Emit(newLogLine(k.config.Source, InfoLogTypeId, fmt.Sprintf(message, params), k.config.Tags))
}

// Warn formats a given message according to given params to log with level Warn and produces to a given Kafka topic.
func (k *KafkaLogEmitter) Warn(message string, params ...interface{}) {
	k.Emit(newLogLine(k.config.Source, WarnLogTypeId, fmt.Sprintf(message, params), k.config.Tags))
}

// Error formats a given message according to given params to log with level Error and produces to a given Kafka topic.
func (k *KafkaLogEmitter) Error(message string, params ...interface{}) {
	k.Emit(newLogLine(k.config.Source, ErrorLogTypeId, fmt.Sprintf(message, params), k.config.Tags))
}

// Critical formats a given message according to given params to log with level Critical and produces to a given Kafka topic.
func (k *KafkaLogEmitter) Critical(message string, params ...interface{}) {
	k.Emit(newLogLine(k.config.Source, CriticalLogTypeId, fmt.Sprintf(message, params), k.config.Tags))
}

// EmptyEmitter implements emitter and ignores all incoming messages. Used not to break anyone.
type EmptyEmitter string

// NewEmptyEmitter creates a new EmptyEmitter.
func NewEmptyEmitter() *EmptyEmitter {
	return new(EmptyEmitter)
}

// Does nothing. Ignores given message.
func (e *EmptyEmitter) Emit(*avro.LogLine) {}

// Does nothing.
func (e *EmptyEmitter) Close() {}

func newLogLine(source string, logtypeid int64, line string, tags map[string]string) *avro.LogLine {
	logLine := avro.NewLogLine()
	logLine.Source = source
	logLine.Logtypeid = logtypeid
	logLine.Line = line
	logLine.Tag = tags
	logLine.Timings = map[string]int64{"emitted": time.Now().Unix()}
	return logLine
}
