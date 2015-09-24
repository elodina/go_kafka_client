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

package framework

import (
	"fmt"
	"regexp"

	"time"

	log "github.com/cihub/seelog"
)

var Logger log.LoggerInterface

var Config *config = &config{
	FrameworkName:    "go_kafka_client",
	FrameworkRole:    "*",
	FrameworkTimeout: 30 * time.Minute,
	LogLevel:         "info",
	Storage:          "file:go_kafka_client_mesos.json",
}

var executorMask = regexp.MustCompile("executor.*")

type config struct {
	Api              string
	Master           string
	FrameworkName    string
	FrameworkRole    string
	FrameworkTimeout time.Duration
	User             string
	LogLevel         string
	Storage          string
}

func (c *config) String() string {
	return fmt.Sprintf(`api:                 %s
master:              %s
framework name:      %s
framework role:      %s
framework timeout    %s
user:                %s
log level:           %s
storage:             %s
`, c.Api, c.Master, c.FrameworkName, c.FrameworkRole, c.FrameworkTimeout, c.User, c.LogLevel, c.Storage)
}

func InitLogging(level string) error {
	config := fmt.Sprintf(`<seelog minlevel="%s">
    <outputs formatid="main">
        <console />
    </outputs>

    <formats>
        <format id="main" format="%%Date/%%Time [%%LEVEL] %%Msg%%n"/>
    </formats>
</seelog>`, level)

	logger, err := log.LoggerFromConfigAsBytes([]byte(config))
	Config.LogLevel = level
	Logger = logger

	return err
}
