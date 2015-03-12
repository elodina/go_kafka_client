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

var availableAPIs = []ConsumerGroupApi{BlueGreenDeploymentAPI, Rebalance}

type ConsumerGroupApi string

const (
	BlueGreenDeploymentAPI ConsumerGroupApi = "blue_green"
	Rebalance              ConsumerGroupApi = "rebalance"
)

// DeployedTopics contain information needed to do a successful blue-green deployment.
// It contains a comma-separated list of new topics to consume if the pattern is static and regex for wildcard consuming.
type BlueGreenDeployment struct {
	// Comma separated list of topics to consume from
	Topics string
	// Either black_list, white_list or static
	Pattern string
	//Consumer group to switch to
	Group string
}
