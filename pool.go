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

import "sync"

type RoutinePool struct {
	wg sync.WaitGroup
	ch chan func()
}

func NewRoutinePool(size int) *RoutinePool {
	rp := &RoutinePool{
		ch: make(chan func()),
	}

	for i := 0; i < size; i++ {
		go func() {
			for f := range rp.ch {
				rp.doWork(f)
			}
		}()
	}

	return rp
}

func (rp *RoutinePool) doWork(f func()) {
	defer rp.wg.Done()
	f()
}

func (rp *RoutinePool) Do(f func()) {
	rp.wg.Add(1)
	rp.ch <- f
}

func (rp *RoutinePool) Stop() {
	rp.wg.Wait()
	close(rp.ch)
}
