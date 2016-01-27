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

package utils

import (
	mesos "github.com/mesos/mesos-go/mesosproto"
	util "github.com/mesos/mesos-go/mesosutil"
	"github.com/mesos/mesos-go/scheduler"
	"time"
)

type Reconciler struct {
	ReconcileDelay    time.Duration
	ReconcileMaxTries int

	tasks         Tasks
	reconcileTime time.Time
	reconciles    int
}

func NewReconciler(tasks Tasks) *Reconciler {
	return &Reconciler{
		ReconcileDelay:    10 * time.Second,
		ReconcileMaxTries: 3,
		reconcileTime:     time.Unix(0, 0),
		tasks:             tasks,
	}
}

func (r *Reconciler) ImplicitReconcile(driver scheduler.SchedulerDriver) {
	r.reconcile(driver, true)
}

func (r *Reconciler) ExplicitReconcile(driver scheduler.SchedulerDriver) {
	r.reconcile(driver, false)
}

func (r *Reconciler) reconcile(driver scheduler.SchedulerDriver, implicit bool) {
	if time.Now().Sub(r.reconcileTime) >= r.ReconcileDelay {
		if !r.tasks.IsReconciling() {
			r.reconciles = 0
		}
		r.reconciles++
		r.reconcileTime = time.Now()

		if r.reconciles > r.ReconcileMaxTries {
			for _, task := range r.tasks.GetWithFilter(func(task Task) bool {
				return task.Data().State == TaskStateReconciling
			}) {
				if task.Data().TaskID != "" {
					Logger.Info("Reconciling exceeded %d tries for task %s, sending killTask for task %s", r.ReconcileMaxTries, task.Data().ID, task.Data().TaskID)
					driver.KillTask(util.NewTaskID(task.Data().TaskID))

					task.Data().Reset()
				}
			}
		} else {
			if implicit {
				driver.ReconcileTasks(nil)
			} else {
				statuses := make([]*mesos.TaskStatus, 0)
				for _, task := range r.tasks.GetAll() {
					if task.Data().TaskID != "" {
						task.Data().State = TaskStateReconciling
						Logger.Info("Reconciling %d/%d task state for id %s, task id %s", r.reconciles, r.ReconcileMaxTries, task.Data().ID, task.Data().TaskID)
						statuses = append(statuses, util.NewTaskStatus(util.NewTaskID(task.Data().TaskID), mesos.TaskState_TASK_STAGING))
					}
				}
				driver.ReconcileTasks(statuses)
			}
		}
	}
}
