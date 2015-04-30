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
	metrics "github.com/rcrowley/go-metrics"
	"io"
	"time"
)

type ConsumerMetrics struct {
	registry metrics.Registry

	numFetchRoutinesCounter metrics.Counter
	fetchersIdleTimer       metrics.Timer
	fetchDurationTimer      metrics.Timer

	numWorkerManagersGauge metrics.Gauge
	activeWorkersCounter   metrics.Counter
	pendingWMsTasksCounter metrics.Counter
	wmsBatchDurationTimer  metrics.Timer
	wmsIdleTimer           metrics.Timer
}

func newConsumerMetrics(consumerName string) *ConsumerMetrics {
	kafkaMetrics := &ConsumerMetrics{
		registry: metrics.NewRegistry(),
	}

	kafkaMetrics.fetchersIdleTimer = metrics.NewRegisteredTimer(fmt.Sprintf("FetchersIdleTime-%s", consumerName), kafkaMetrics.registry)
	kafkaMetrics.fetchDurationTimer = metrics.NewRegisteredTimer(fmt.Sprintf("FetchDuration-%s", consumerName), kafkaMetrics.registry)

	kafkaMetrics.numWorkerManagersGauge = metrics.NewRegisteredGauge(fmt.Sprintf("NumWorkerManagers-%s", consumerName), kafkaMetrics.registry)
	kafkaMetrics.activeWorkersCounter = metrics.NewRegisteredCounter(fmt.Sprintf("WMsActiveWorkers-%s", consumerName), kafkaMetrics.registry)
	kafkaMetrics.pendingWMsTasksCounter = metrics.NewRegisteredCounter(fmt.Sprintf("WMsPendingTasks-%s", consumerName), kafkaMetrics.registry)
	kafkaMetrics.wmsBatchDurationTimer = metrics.NewRegisteredTimer(fmt.Sprintf("WMsBatchDuration-%s", consumerName), kafkaMetrics.registry)
	kafkaMetrics.wmsIdleTimer = metrics.NewRegisteredTimer(fmt.Sprintf("WMsIdleTime-%s", consumerName), kafkaMetrics.registry)

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

func (this *ConsumerMetrics) activeWorkers() metrics.Counter {
	return this.activeWorkersCounter
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
	metrics.WriteJSON(this.registry, reportingInterval, writer)
}

func (this *ConsumerMetrics) close() {
	this.registry.UnregisterAll()
}
