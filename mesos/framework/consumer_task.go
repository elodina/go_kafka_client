package framework

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"github.com/golang/protobuf/proto"
	mesos "github.com/mesos/mesos-go/mesosproto"
	util "github.com/mesos/mesos-go/mesosutil"
)

type ConsumerTask struct {
	*CommonTask
}

func NewConsumerTask(id string, queryParams url.Values) (*ConsumerTask, error) {
	taskData := &TaskData{
		ID:     id,
		State:  TaskStateInactive,
		Config: TaskConfig{},
	}

	err := taskData.Update(queryParams)
	if err != nil {
		return nil, err
	}

	return &ConsumerTask{&CommonTask{TaskData: taskData}}, nil
}

func (ct *ConsumerTask) NewTaskInfo(offer *mesos.Offer) *mesos.TaskInfo {
	taskName := fmt.Sprintf("consumer-%s", ct.ID)
	taskId := &mesos.TaskID{
		Value: proto.String(fmt.Sprintf("%s-%s", taskName, uuid())),
	}

	data, err := json.Marshal(ct.Config)
	if err != nil {
		panic(err)
	}

	taskInfo := &mesos.TaskInfo{
		Name:     proto.String(taskName),
		TaskId:   taskId,
		SlaveId:  offer.GetSlaveId(),
		Executor: ct.createExecutor(),
		Resources: []*mesos.Resource{
			util.NewScalarResource("cpus", ct.Cpu),
			util.NewScalarResource("mem", ct.Mem),
		},
		Data: data,
	}

	return taskInfo
}

func (ct *ConsumerTask) MarshalJSON() ([]byte, error) {
	fields := make(map[string]interface{})
	fields["type"] = TaskTypeConsumer
	fields["data"] = ct.TaskData

	return json.Marshal(fields)
}

func (ct *ConsumerTask) String() string {
	response := "    type: consumer\n"
	response += ct.TaskData.String()

	return response
}

func (ct *ConsumerTask) createExecutor() *mesos.ExecutorInfo {
	executor, err := ct.Config.GetString("executor")
	if err != nil || executor == "" {
		fmt.Println("Executor name required")
		return nil
	}
	id := fmt.Sprintf("consumer-%s", ct.ID)
	uris := []*mesos.CommandInfo_URI{
		&mesos.CommandInfo_URI{
			Value:      proto.String(fmt.Sprintf("%s/resource/%s", Config.Api, executor)),
			Executable: proto.Bool(true),
		},
	}

	for _, consumerConfig := range strings.Split(ct.Config["consumer.config"], ",") {
		uris = append(uris, toURI(consumerConfig))
	}

	return &mesos.ExecutorInfo{
		ExecutorId: util.NewExecutorID(id),
		Name:       proto.String("kafka-consumer"),
		Command: &mesos.CommandInfo{
			Value: proto.String(fmt.Sprintf("./%s --log.level %s --type %s", executor, Config.LogLevel, TaskTypeConsumer)),
			Uris:  uris,
		},
	}
}

func (ct *ConsumerTask) Matches(offer *mesos.Offer) string {
	return ct.commonMatches(offer)
}
