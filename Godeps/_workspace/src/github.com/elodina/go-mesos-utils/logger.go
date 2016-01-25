/* Licensed to Elodina Inc. under one or more
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

package utils

import (
	"fmt"

	log "github.com/cihub/seelog"
	"runtime"
	"strings"
)

//Logger used by this client. Defaults to build-in logger with Info log level.
var Logger Log = NewDefaultLogger(InfoLevel)

// Log is a logger interface. Lets you plug-in your custom logging library instead of using built-in one.
type Log interface {
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

// DefaultLogger is a default implementation of Log interface used in this client.
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
	dl.logger.Tracef(fmt.Sprintf("%s %s", caller(), message), params...)
}

// Debug formats a given message according to given params to log with level Debug.
func (dl *DefaultLogger) Debug(message string, params ...interface{}) {
	dl.logger.Debugf(fmt.Sprintf("%s %s", caller(), message), params...)
}

// Info formats a given message according to given params to log with level Info.
func (dl *DefaultLogger) Info(message string, params ...interface{}) {
	dl.logger.Infof(fmt.Sprintf("%s %s", caller(), message), params...)
}

// Warn formats a given message according to given params to log with level Warn.
func (dl *DefaultLogger) Warn(message string, params ...interface{}) {
	dl.logger.Warnf(fmt.Sprintf("%s %s", caller(), message), params...)
}

// Error formats a given message according to given params to log with level Error.
func (dl *DefaultLogger) Error(message string, params ...interface{}) {
	dl.logger.Errorf(fmt.Sprintf("%s %s", caller(), message), params...)
}

// Critical formats a given message according to given params to log with level Critical.
func (dl *DefaultLogger) Critical(message string, params ...interface{}) {
	dl.logger.Criticalf(fmt.Sprintf("%s %s", caller(), message), params...)
}

func caller() string {
	pc, file, line, ok := runtime.Caller(2)
	if ok {
		f := runtime.FuncForPC(pc)
		fileTokens := strings.Split(file, "/")
		file = fileTokens[len(fileTokens)-1]

		funcTokens := strings.Split(f.Name(), "/")
		fun := funcTokens[len(funcTokens)-1]
		return fmt.Sprintf("[%s:%d|%s]", file, line, fun[strings.Index(fun, ".")+1:])
	} else {
		return "???"
	}
}
