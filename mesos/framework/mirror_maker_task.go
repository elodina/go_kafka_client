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

type MirrorMakerTask struct {
	*CommonTask
}

func NewMirrorMakerTask(id string, queryParams url.Values) (*MirrorMakerTask, error) {
	taskData := &TaskData{
		ID:    id,
		State: TaskStateInactive,
		Config: TaskConfig{
			"num.producers": "1",
			"num.streams":   "1",
			"queue.size":    "10000",
		},
	}

	err := taskData.Update(queryParams)
	if err != nil {
		return nil, err
	}

	return &MirrorMakerTask{
		&CommonTask{TaskData: taskData},
	}, nil
}

func (mm *MirrorMakerTask) NewTaskInfo(offer *mesos.Offer) *mesos.TaskInfo {
	taskName := fmt.Sprintf("mirrormaker-%s", mm.ID)
	taskId := &mesos.TaskID{
		Value: proto.String(fmt.Sprintf("%s-%s", taskName, uuid())),
	}

	data, err := json.Marshal(mm.Config)
	if err != nil {
		panic(err)
	}

	taskInfo := &mesos.TaskInfo{
		Name:     proto.String(taskName),
		TaskId:   taskId,
		SlaveId:  offer.GetSlaveId(),
		Executor: mm.createExecutor(),
		Resources: []*mesos.Resource{
			util.NewScalarResource("cpus", mm.Cpu),
			util.NewScalarResource("mem", mm.Mem),
		},
		Data: data,
	}

	return taskInfo
}

func (mm *MirrorMakerTask) String() string {
	response := "    type: mirrormaker\n"
	response += mm.TaskData.String()

	return response
}

func (mm *MirrorMakerTask) MarshalJSON() ([]byte, error) {
	fields := make(map[string]interface{})
	fields["type"] = TaskTypeMirrorMaker
	fields["data"] = mm.TaskData

	return json.Marshal(fields)
}

func (mm *MirrorMakerTask) createExecutor() *mesos.ExecutorInfo {
	executor, err := mm.Config.GetString("executor")
	if err != nil || executor == "" {
		fmt.Println("Executor name required")
		return nil
	}

	id := fmt.Sprintf("mirrormaker-%s", mm.ID)
	uris := []*mesos.CommandInfo_URI{
		&mesos.CommandInfo_URI{
			Value:      proto.String(fmt.Sprintf("%s/resource/%s", Config.Api, executor)),
			Executable: proto.Bool(true),
		},
		toURI(mm.Config["producer.config"]),
	}

	for _, consumerConfig := range strings.Split(mm.Config["consumer.config"], ",") {
		uris = append(uris, toURI(consumerConfig))
	}

	return &mesos.ExecutorInfo{
		ExecutorId: util.NewExecutorID(id),
		Name:       proto.String(id),
		Command: &mesos.CommandInfo{
			Value: proto.String(fmt.Sprintf("./%s --log.level %s --type %s", executor, Config.LogLevel, TaskTypeMirrorMaker)),
			Uris:  uris,
		},
	}
}

func (mm *MirrorMakerTask) Matches(offer *mesos.Offer) string {
	if common := mm.commonMatches(offer); common != "" {
		return common
	}
	if mm.Config["producer.config"] == "" {
		return "producer.config not set"
	}
	return ""
}
