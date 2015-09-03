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
    "os"
    "github.com/mesos/mesos-go/executor"
    mesos "github.com/mesos/mesos-go/mesosproto"
    "time"
)

type MirrorMakerExecutor struct {
}

func (e *MirrorMakerExecutor) Registered(driver executor.ExecutorDriver, executor *mesos.ExecutorInfo, framework *mesos.FrameworkInfo, slave *mesos.SlaveInfo) {
    Logger.Infof("[Registered] framework: %s slave: %s", framework.GetId().GetValue(), slave.GetId().GetValue())
}

func (e *MirrorMakerExecutor) Reregistered(driver executor.ExecutorDriver, slave *mesos.SlaveInfo) {
    Logger.Infof("[Reregistered] slave: %s", slave.GetId().GetValue())
}

func (e *MirrorMakerExecutor) Disconnected(executor.ExecutorDriver) {
    Logger.Info("[Disconnected]")
}

func (e *MirrorMakerExecutor) LaunchTask(driver executor.ExecutorDriver, task *mesos.TaskInfo) {
    Logger.Infof("[LaunchTask] %s", task)

    runStatus := &mesos.TaskStatus{
        TaskId: task.GetTaskId(),
        State:  mesos.TaskState_TASK_RUNNING.Enum(),
    }

    if _, err := driver.SendStatusUpdate(runStatus); err != nil {
        Logger.Errorf("Failed to send status update: %s", runStatus)
        os.Exit(1) //TODO not sure if we should exit in this case, but probably yes
    }

    go func() {
        time.Sleep(1 * time.Minute)

        // finish task
        Logger.Infof("Finishing task %s", task.GetName())
        finStatus := &mesos.TaskStatus{
            TaskId: task.GetTaskId(),
            State:  mesos.TaskState_TASK_FINISHED.Enum(),
        }
        if _, err := driver.SendStatusUpdate(finStatus); err != nil {
            Logger.Errorf("Failed to send status update: %s", finStatus)
            os.Exit(1)
        }
        Logger.Infof("Task %s has finished", task.GetName())
    }()
}

func (e *MirrorMakerExecutor) KillTask(driver executor.ExecutorDriver, id *mesos.TaskID) {
    Logger.Infof("[KillTask] %s", id.GetValue())
}

func (e *MirrorMakerExecutor) FrameworkMessage(driver executor.ExecutorDriver, message string) {
    Logger.Infof("[FrameworkMessage] %s", message)
}

func (e *MirrorMakerExecutor) Shutdown(driver executor.ExecutorDriver) {
    Logger.Infof("[Shutdown]")
}

func (e *MirrorMakerExecutor) Error(driver executor.ExecutorDriver, message string) {
    Logger.Errorf("[Error] %s", message)
}
