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

// This function will be called each time the executor launches a new task.
// This is a place where you should provide your consumer behaviour, like fetch size, consumer strategy, timeouts, callbacks etc.
// Coordinator and GroupId will be overridden after this, so no need to set them.
func SetupConsumerConfig(config *ConsumerConfig) {
	config.AutoOffsetReset = SmallestOffset

	config.Strategy = func(_ *Worker, msg *Message, id TaskId) WorkerResult {
		Debugf("Strategy", "Got message: %s\n", string(msg.Value))
		return NewSuccessfulResult(id)
	}

	config.WorkerFailedAttemptCallback = func(_ *Task, _ WorkerResult) FailedDecision {
		return CommitOffsetAndContinue
	}

	config.WorkerFailureCallback = func(_ *WorkerManager) FailedDecision {
		return DoNotCommitOffsetAndStop
	}
}

// This function will be called each time the executor launches a new task.
// This is a place where you set your Zookeeper settings.
// ZookeeperConnect setting will be overridden after this, so no need to set it.
func SetupZookeeperConfig(config *ZookeeperConfig) {
	config.MaxRequestRetries = 5
}
