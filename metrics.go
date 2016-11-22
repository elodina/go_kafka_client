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
	"io"
	"strings"
	"time"

	metrics "github.com/rcrowley/go-metrics"
	"sync"
)

type ConsumerMetrics struct {
	registry     metrics.Registry
	consumerName string
	prefix       string

	numFetchRoutinesCounter metrics.Counter
	fetchersIdleTimer       metrics.Timer
	fetchDurationTimer      metrics.Timer

	numWorkerManagersGauge metrics.Gauge
	activeWorkersCounter   metrics.Counter
	pendingWMsTasksCounter metrics.Counter
	taskTimeoutCounter     metrics.Counter
	wmsBatchDurationTimer  metrics.Timer
	wmsIdleTimer           metrics.Timer

	fetcherNetworkErrorCounter metrics.Counter
	consumerResetCounter       metrics.Counter
	numFetchedMessagesCounter  metrics.Counter
	numConsumedMessagesCounter metrics.Counter
	numAcksCounter             metrics.Counter
	topicPartitionLag          map[TopicAndPartition]metrics.Gauge

	metricLock            sync.Mutex
	reportingStopChannels []chan struct{}
}

func newConsumerMetrics(consumerName, prefix string) *ConsumerMetrics {
	kafkaMetrics := &ConsumerMetrics{
		registry: metrics.DefaultRegistry,
	}

	// Ensure prefix ends with a dot (.) so it plays nice with statsd/graphite
	prefix = strings.Trim(prefix, " ")
	if prefix != "" && prefix[len(prefix)-1:] != "." {
		prefix += "."
	}
	kafkaMetrics.consumerName = consumerName
	kafkaMetrics.prefix = prefix

	kafkaMetrics.fetchersIdleTimer = metrics.NewRegisteredTimer(fmt.Sprintf("%sFetchersIdleTime-%s", prefix, consumerName), kafkaMetrics.registry)
	kafkaMetrics.fetchDurationTimer = metrics.NewRegisteredTimer(fmt.Sprintf("%sFetchDuration-%s", prefix, consumerName), kafkaMetrics.registry)

	kafkaMetrics.numWorkerManagersGauge = metrics.NewRegisteredGauge(fmt.Sprintf("%sNumWorkerManagers-%s", prefix, consumerName), kafkaMetrics.registry)
	kafkaMetrics.activeWorkersCounter = metrics.NewRegisteredCounter(fmt.Sprintf("%sWMsActiveWorkers-%s", prefix, consumerName), kafkaMetrics.registry)
	kafkaMetrics.pendingWMsTasksCounter = metrics.NewRegisteredCounter(fmt.Sprintf("%sWMsPendingTasks-%s", prefix, consumerName), kafkaMetrics.registry)
	kafkaMetrics.taskTimeoutCounter = metrics.NewRegisteredCounter(fmt.Sprintf("%sTaskTimeouts-%s", prefix, consumerName), kafkaMetrics.registry)
	kafkaMetrics.fetcherNetworkErrorCounter = metrics.NewRegisteredCounter(fmt.Sprintf("%sFetcherNetworkError-%s", prefix, consumerName), kafkaMetrics.registry)
	kafkaMetrics.consumerResetCounter = metrics.NewRegisteredCounter(fmt.Sprintf("%sConsumerReset-%s", prefix, consumerName), kafkaMetrics.registry)
	kafkaMetrics.wmsBatchDurationTimer = metrics.NewRegisteredTimer(fmt.Sprintf("%sWMsBatchDuration-%s", prefix, consumerName), kafkaMetrics.registry)
	kafkaMetrics.wmsIdleTimer = metrics.NewRegisteredTimer(fmt.Sprintf("%sWMsIdleTime-%s", prefix, consumerName), kafkaMetrics.registry)

	kafkaMetrics.numFetchedMessagesCounter = metrics.NewRegisteredCounter(fmt.Sprintf("%sFetchedMessages-%s", prefix, consumerName), kafkaMetrics.registry)
	kafkaMetrics.numConsumedMessagesCounter = metrics.NewRegisteredCounter(fmt.Sprintf("%sConsumedMessages-%s", prefix, consumerName), kafkaMetrics.registry)
	kafkaMetrics.numAcksCounter = metrics.NewRegisteredCounter(fmt.Sprintf("%sAcks-%s", prefix, consumerName), kafkaMetrics.registry)
	kafkaMetrics.topicPartitionLag = make(map[TopicAndPartition]metrics.Gauge)

	kafkaMetrics.reportingStopChannels = make([]chan struct{}, 0)

	return kafkaMetrics
}

func (this *ConsumerMetrics) fetchersIdle() metrics.Timer {
	return this.fetchersIdleTimer
}

func (this *ConsumerMetrics) fetchDuration() metrics.Timer {
	return this.fetchDurationTimer
}

func (this *ConsumerMetrics) numWorkerManagers() metrics.Gauge {
	return this.numWorkerManagersGauge
}

func (this *ConsumerMetrics) wMsIdle() metrics.Timer {
	return this.wmsIdleTimer
}

func (this *ConsumerMetrics) wMsBatchDuration() metrics.Timer {
	return this.wmsBatchDurationTimer
}

func (this *ConsumerMetrics) pendingWMsTasks() metrics.Counter {
	return this.pendingWMsTasksCounter
}

func (this *ConsumerMetrics) taskTimeouts() metrics.Counter {
	return this.taskTimeoutCounter
}

func (this *ConsumerMetrics) activeWorkers() metrics.Counter {
	return this.activeWorkersCounter
}

func (this *ConsumerMetrics) fetcherNetworkErrors() metrics.Counter {
	return this.fetcherNetworkErrorCounter
}

func (this *ConsumerMetrics) consumerResets() metrics.Counter {
	return this.consumerResetCounter
}

func (this *ConsumerMetrics) numFetchedMessages() metrics.Counter {
	return this.numFetchedMessagesCounter
}

func (this *ConsumerMetrics) numConsumedMessages() metrics.Counter {
	return this.numConsumedMessagesCounter
}

func (this *ConsumerMetrics) numAcks() metrics.Counter {
	return this.numAcksCounter
}

func (this *ConsumerMetrics) topicAndPartitionLag(topic string, partition int32) metrics.Gauge {
	topicAndPartition := TopicAndPartition{Topic: topic, Partition: partition}
	lag, ok := this.topicPartitionLag[topicAndPartition]
	if !ok {
		inLock(&this.metricLock, func() {
			lag, ok = this.topicPartitionLag[topicAndPartition]
			if !ok {
				this.topicPartitionLag[topicAndPartition] = metrics.NewRegisteredGauge(fmt.Sprintf("%sLag-%s-%s", this.prefix, this.consumerName, &topicAndPartition), this.registry)
				lag = this.topicPartitionLag[topicAndPartition]
			}
		})
	}
	return lag
}

func (this *ConsumerMetrics) Stats() map[string]map[string]float64 {
	metricsMap := make(map[string]map[string]float64)
	this.registry.Each(func(name string, metric interface{}) {
		metricsMap[name] = make(map[string]float64)
		switch entry := metric.(type) {
		case metrics.Counter:
			{
				metricsMap[name]["count"] = float64(entry.Count())
			}
		case metrics.Gauge:
			{
				metricsMap[name]["value"] = float64(entry.Value())
			}
		case metrics.Histogram:
			{
				metricsMap[name]["count"] = float64(entry.Count())
				metricsMap[name]["max"] = float64(entry.Max())
				metricsMap[name]["min"] = float64(entry.Min())
				metricsMap[name]["mean"] = entry.Mean()
				metricsMap[name]["stdDev"] = entry.StdDev()
				metricsMap[name]["sum"] = float64(entry.Sum())
				metricsMap[name]["variance"] = entry.Variance()
			}
		case metrics.Meter:
			{
				metricsMap[name]["count"] = float64(entry.Count())
				metricsMap[name]["rate1"] = entry.Rate1()
				metricsMap[name]["rate5"] = entry.Rate5()
				metricsMap[name]["rate15"] = entry.Rate15()
				metricsMap[name]["rateMean"] = entry.RateMean()
			}
		case metrics.Timer:
			{
				metricsMap[name]["count"] = float64(entry.Count())
				metricsMap[name]["max"] = float64(entry.Max())
				metricsMap[name]["min"] = float64(entry.Min())
				metricsMap[name]["mean"] = entry.Mean()
				metricsMap[name]["rate1"] = entry.Rate1()
				metricsMap[name]["rate5"] = entry.Rate5()
				metricsMap[name]["rate15"] = entry.Rate15()
				metricsMap[name]["rateMean"] = entry.RateMean()
				metricsMap[name]["stdDev"] = entry.StdDev()
				metricsMap[name]["sum"] = float64(entry.Sum())
				metricsMap[name]["variance"] = entry.Variance()
			}
		}
	})

	return metricsMap
}

func (this *ConsumerMetrics) WriteJSON(reportingInterval time.Duration, writer io.Writer) {
	tick := time.Tick(reportingInterval)
	stop := make(chan struct{})

	inLock(&this.metricLock, func() {
		this.reportingStopChannels = append(this.reportingStopChannels, stop)
	})

	for {
		select {
		case <-tick:
			metrics.WriteJSONOnce(this.registry, writer)
		case <-stop:
			return
		}
	}
}

func (this *ConsumerMetrics) close() {
	for _, ch := range this.reportingStopChannels {
		ch <- struct{}{}
	}
	this.registry.UnregisterAll()
}
