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
	"sync"
	"time"
)

type messageBuffer struct {
	OutputChannel  chan []*Message
	Messages       []*Message
	Config         *ConsumerConfig
	Timer          *time.Timer
	MessageLock    sync.Mutex
	Close          chan bool
	stopSending    bool
	TopicPartition TopicAndPartition
	askNextBatch   chan TopicAndPartition
}

func newMessageBuffer(topicPartition TopicAndPartition, outputChannel chan []*Message, config *ConsumerConfig) *messageBuffer {
	buffer := &messageBuffer{
		OutputChannel:  outputChannel,
		Messages:       make([]*Message, 0),
		Config:         config,
		Timer:          time.NewTimer(config.FetchBatchTimeout),
		Close:          make(chan bool),
		TopicPartition: topicPartition,
	}

	return buffer
}

func (mb *messageBuffer) String() string {
	return fmt.Sprintf("%s-MessageBuffer", &mb.TopicPartition)
}

func (mb *messageBuffer) autoFlush() {
	for {
		select {
		case <-mb.Close:
			return
		case <-mb.Timer.C:
			{
				go inLock(&mb.MessageLock, func() {
					if !mb.stopSending {
						if Logger.IsAllowed(TraceLevel) {
							Trace(mb, "Batch accumulation timed out. Flushing...")
						}
						mb.Timer.Reset(mb.Config.FetchBatchTimeout)
						mb.flush()
					}
				})
			}
		}
	}
}

func (mb *messageBuffer) flush() {
	if len(mb.Messages) > 0 {
		if Logger.IsAllowed(TraceLevel) {
			Trace(mb, "Flushing")
		}
		mb.Timer.Reset(mb.Config.FetchBatchTimeout)
	flushLoop:
		for {
			timeout := time.NewTimer(200 * time.Millisecond)
			select {
			case mb.OutputChannel <- mb.Messages:
				timeout.Stop()
				break flushLoop
			case <-timeout.C:
				if mb.stopSending {
					return
				}
			}
		}
		if Logger.IsAllowed(TraceLevel) {
			Trace(mb, "Flushed")
		}
		mb.Messages = make([]*Message, 0)
	}
}

func (mb *messageBuffer) start(fetcherChannel chan TopicAndPartition) {
	mb.askNextBatch = fetcherChannel
	go mb.autoFlush()
}

func (mb *messageBuffer) stop() {
	if !mb.stopSending {
		mb.stopSending = true
		Info(mb, "Trying to stop message buffer")
		inLock(&mb.MessageLock, func() {
			Info(mb, "Stopping message buffer")
			mb.Close <- true
			Info(mb, "Stopped message buffer")
		})
	}
}

func (mb *messageBuffer) addBatch(messages []*Message) {
	if Logger.IsAllowed(TraceLevel) {
		Tracef(mb, "Adding batch of messages to message buffer %d", len(messages))
	}
	inLock(&mb.MessageLock, func() {
		if Logger.IsAllowed(TraceLevel) {
			Trace(mb, "Trying to add messages to message buffer")
		}
		if mb.stopSending {
			Debug(mb, "Message buffer has been stopped, batch shall not be added.")
			return
		}

		for _, message := range messages {
			if Logger.IsAllowed(TraceLevel) {
				Tracef(mb, "Adding message to message buffer %v", message)
			}
			mb.add(message)
		}

		if Logger.IsAllowed(TraceLevel) {
			Trace(mb, "Added messages")
		}

	askNextLoop:
		for !mb.stopSending {
			timeout := time.NewTimer(mb.Config.RequeueAskNextBackoff)
			select {
			case mb.askNextBatch <- mb.TopicPartition:
				{
					timeout.Stop()
					if Logger.IsAllowed(TraceLevel) {
						Trace(mb, "Asking for next batch")
					}
					break askNextLoop
				}
			case <-timeout.C:
			}
		}
	})
}

func (mb *messageBuffer) add(msg *Message) {
	if Logger.IsAllowed(TraceLevel) {
		Tracef(mb, "Added message: %s", msg)
	}
	mb.Messages = append(mb.Messages, msg)
	if len(mb.Messages) == mb.Config.FetchBatchSize {
		if Logger.IsAllowed(TraceLevel) {
			Trace(mb, "Batch is ready. Flushing")
		}
		mb.flush()
	}
}
