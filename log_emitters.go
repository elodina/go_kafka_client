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
	"github.com/elodina/go_kafka_client/avro"
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
