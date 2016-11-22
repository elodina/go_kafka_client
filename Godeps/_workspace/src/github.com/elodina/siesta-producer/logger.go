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

package producer

import (
	"fmt"

	log "gopkg.in/cihub/seelog.v2"
)

//Logger used by this client. Defaults to build-in logger with Info log level.
var Logger KafkaLogger = NewDefaultLogger(InfoLevel)

// KafkaLogger is a logger interface. Lets you plug-in your custom logging library instead of using built-in one.
type KafkaLogger interface {
	//Formats a given message according to given params to log with level Trace.
	Trace(message string, params ...interface{})

	//Formats a given message according to given params to log with level Debug.
	Debug(message string, params ...interface{})

	//Formats a given message according to given params to log with level Info.
	Info(message string, params ...interface{})

	//Formats a given message according to given params to log with level Warn.
	Warn(message string, params ...interface{})

	//Formats a given message according to given params to log with level Error.
	Error(message string, params ...interface{})

	//Formats a given message according to given params to log with level Critical.
	Critical(message string, params ...interface{})
}

// LogLevel represents a logging level.
type LogLevel string

const (
	// TraceLevel is used for debugging to find problems in functions, variables etc.
	TraceLevel LogLevel = "trace"

	// DebugLevel is used for detailed system reports and diagnostic messages.
	DebugLevel LogLevel = "debug"

	// InfoLevel is used for general information about a running application.
	InfoLevel LogLevel = "info"

	// WarnLevel is used to indicate small errors and failures that should not happen normally but are recovered automatically.
	WarnLevel LogLevel = "warn"

	// ErrorLevel is used to indicate severe errors that affect application workflow and are not handled automatically.
	ErrorLevel LogLevel = "error"

	// CriticalLevel is used to indicate fatal errors that may cause data corruption or loss.
	CriticalLevel LogLevel = "critical"
)

// Trace writes a given message with a given tag to log with level Trace.
func Trace(tag interface{}, message interface{}) {
	Logger.Trace(fmt.Sprintf("[%s] %s", tag, message))
}

// Tracef formats a given message according to given params with a given tag to log with level Trace.
func Tracef(tag interface{}, message interface{}, params ...interface{}) {
	Logger.Trace(fmt.Sprintf("[%s] %s", tag, message), params...)
}

// Debug writes a given message with a given tag to log with level Debug.
func Debug(tag interface{}, message interface{}) {
	Logger.Debug(fmt.Sprintf("[%s] %s", tag, message))
}

// Debugf formats a given message according to given params with a given tag to log with level Debug.
func Debugf(tag interface{}, message interface{}, params ...interface{}) {
	Logger.Debug(fmt.Sprintf("[%s] %s", tag, message), params...)
}

// Info writes a given message with a given tag to log with level Info.
func Info(tag interface{}, message interface{}) {
	Logger.Info(fmt.Sprintf("[%s] %s", tag, message))
}

// Infof formats a given message according to given params with a given tag to log with level Info.
func Infof(tag interface{}, message interface{}, params ...interface{}) {
	Logger.Info(fmt.Sprintf("[%s] %s", tag, message), params...)
}

// Warn writes a given message with a given tag to log with level Warn.
func Warn(tag interface{}, message interface{}) {
	Logger.Warn(fmt.Sprintf("[%s] %s", tag, message))
}

// Warnf formats a given message according to given params with a given tag to log with level Warn.
func Warnf(tag interface{}, message interface{}, params ...interface{}) {
	Logger.Warn(fmt.Sprintf("[%s] %s", tag, message), params...)
}

// Error writes a given message with a given tag to log with level Error.
func Error(tag interface{}, message interface{}) {
	Logger.Error(fmt.Sprintf("[%s] %s", tag, message))
}

// Errorf formats a given message according to given params with a given tag to log with level Error.
func Errorf(tag interface{}, message interface{}, params ...interface{}) {
	Logger.Error(fmt.Sprintf("[%s] %s", tag, message), params...)
}

// Critical writes a given message with a given tag to log with level Critical.
func Critical(tag interface{}, message interface{}) {
	Logger.Critical(fmt.Sprintf("[%s] %s", tag, message))
}

// Criticalf formats a given message according to given params with a given tag to log with level Critical.
func Criticalf(tag interface{}, message interface{}, params ...interface{}) {
	Logger.Critical(fmt.Sprintf("[%s] %s", tag, message), params...)
}

// DefaultLogger is a default implementation of KafkaLogger interface used in this client.
type DefaultLogger struct {
	logger log.LoggerInterface
}

// NewDefaultLogger creates a new DefaultLogger that is configured to write messages to console with minimum log level Level.
func NewDefaultLogger(Level LogLevel) *DefaultLogger {
	var config = fmt.Sprintf(`<seelog minlevel="%s">
    <outputs formatid="main">
        <console />
    </outputs>

    <formats>
        <format id="main" format="%%Date/%%Time [%%LEVEL] %%Msg%%n"/>
    </formats>
</seelog>`, Level)
	logger, _ := log.LoggerFromConfigAsBytes([]byte(config))
	return &DefaultLogger{logger}
}

// Trace formats a given message according to given params to log with level Trace.
func (dl *DefaultLogger) Trace(message string, params ...interface{}) {
	dl.logger.Tracef(message, params...)
}

// Debug formats a given message according to given params to log with level Debug.
func (dl *DefaultLogger) Debug(message string, params ...interface{}) {
	dl.logger.Debugf(message, params...)
}

// Info formats a given message according to given params to log with level Info.
func (dl *DefaultLogger) Info(message string, params ...interface{}) {
	dl.logger.Infof(message, params...)
}

// Warn formats a given message according to given params to log with level Warn.
func (dl *DefaultLogger) Warn(message string, params ...interface{}) {
	dl.logger.Warnf(message, params...)
}

// Error formats a given message according to given params to log with level Error.
func (dl *DefaultLogger) Error(message string, params ...interface{}) {
	dl.logger.Errorf(message, params...)
}

// Critical formats a given message according to given params to log with level Critical.
func (dl *DefaultLogger) Critical(message string, params ...interface{}) {
	dl.logger.Criticalf(message, params...)
}
