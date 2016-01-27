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

package pretty

import (
	"github.com/golang/protobuf/proto"
	mesos "github.com/mesos/mesos-go/mesosproto"
	util "github.com/mesos/mesos-go/mesosutil"
	"strings"
	"testing"
)

func TestSuffix(t *testing.T) {
	sfx := suffix("asdqwe", 0)
	if sfx != "" {
		t.Errorf(`suffix("asdqwe", 0) != ""; actual %s`, sfx)
	}

	sfx = suffix("asdqwe", 3)
	if sfx != "qwe" {
		t.Errorf(`suffix("asdqwe", 3) != "qwe"; actual %s`, sfx)
	}

	sfx = suffix("asdqwe", 6)
	if sfx != "asdqwe" {
		t.Errorf(`suffix("asdqwe", 6) != "asdqwe"; actual %s`, sfx)
	}

	sfx = suffix("asdqwe", 10)
	if sfx != "asdqwe" {
		t.Errorf(`suffix("asdqwe", 10) != "asdqwe"; actual %s`, sfx)
	}
}

func TestID(t *testing.T) {
	id := ID("487c73d8-9951-f23c-34bd-8085bfd30c49")
	if id != "#30c49" {
		t.Errorf(`ID("487c73d8-9951-f23c-34bd-8085bfd30c49") != "#30c49"; actual %s`, id)
	}
}

func TestResource(t *testing.T) {
	mem := Resource(util.NewScalarResource("mem", 512))
	if mem != "mem:512.00" {
		t.Errorf(`Resource(util.NewScalarResource("mem", 512)) != "mem:512.00"; actual %s`, mem)
	}

	ports := Resource(util.NewRangesResource("ports", []*mesos.Value_Range{util.NewValueRange(31000, 32000)}))
	if ports != "ports:[31000..32000]" {
		t.Errorf(`Resource(util.NewRangesResource("ports", []*mesos.Value_Range{util.NewValueRange(31000, 32000)})) != "ports:[31000..32000]"; actual %s`, ports)
	}

	ports = Resource(util.NewRangesResource("ports", []*mesos.Value_Range{util.NewValueRange(4000, 7000), util.NewValueRange(31000, 32000)}))
	if ports != "ports:[4000..7000][31000..32000]" {
		t.Errorf(`Resource(util.NewRangesResource("ports", []*mesos.Value_Range{util.NewValueRange(4000, 7000), util.NewValueRange(31000, 32000)})) != "ports:[4000..7000][31000..32000]"; actual %s`, ports)
	}
}

func TestResources(t *testing.T) {
	resources := Resources([]*mesos.Resource{util.NewScalarResource("cpus", 4), util.NewScalarResource("mem", 512), util.NewRangesResource("ports", []*mesos.Value_Range{util.NewValueRange(31000, 32000)})})
	if !strings.Contains(resources, "cpus") {
		t.Errorf(`%s does not contain "cpus"`, resources)
	}
	if !strings.Contains(resources, "mem") {
		t.Errorf(`%s does not contain "mem"`, resources)
	}
	if !strings.Contains(resources, "ports") {
		t.Errorf(`%s does not contain "ports"`, resources)
	}
}

func TestAttribute(t *testing.T) {
	attr := Attribute(&mesos.Attribute{
		Name:   proto.String("rack"),
		Type:   mesos.Value_SCALAR.Enum(),
		Scalar: &mesos.Value_Scalar{Value: proto.Float64(2)},
	})
	if attr != "rack:2.00" {
		t.Errorf(`Attribute(&mesos.Attribute{
        Name: proto.String("rack"),
        Type: mesos.Value_SCALAR.Enum(),
        Scalar: &mesos.Value_Scalar{Value: proto.Float64(2)},
    }) != "rack:2.00"; actual %s`, attr)
	}

	attr = Attribute(&mesos.Attribute{
		Name: proto.String("datacenter"),
		Type: mesos.Value_TEXT.Enum(),
		Text: &mesos.Value_Text{Value: proto.String("DC-1")},
	})
	if attr != "datacenter:DC-1" {
		t.Errorf(`Attribute(&mesos.Attribute{
        Name: proto.String("datacenter"),
        Type: mesos.Value_TEXT.Enum(),
        Text: proto.String("DC-1"),
    }) != "datacenter:DC-1"; actual %s`, attr)
	}
}

func TestAttributes(t *testing.T) {
	attributes := Attributes([]*mesos.Attribute{&mesos.Attribute{
		Name:   proto.String("rack"),
		Type:   mesos.Value_SCALAR.Enum(),
		Scalar: &mesos.Value_Scalar{Value: proto.Float64(2)},
	}, &mesos.Attribute{
		Name:   proto.String("floor"),
		Type:   mesos.Value_SCALAR.Enum(),
		Scalar: &mesos.Value_Scalar{Value: proto.Float64(1)},
	}})
	if !strings.Contains(attributes, "rack") {
		t.Errorf(`%s does not contain "rack"`, attributes)
	}

	if !strings.Contains(attributes, "floor") {
		t.Errorf(`%s does not contain "floor"`, attributes)
	}
}

func TestOffer(t *testing.T) {
	offer := util.NewOffer(util.NewOfferID("487c73d8-9951-f23c-34bd-8085bfd30c49"), util.NewFrameworkID("20150903-065451-84125888-5050-10715-0053"),
		util.NewSlaveID("20150903-065451-84125888-5050-10715-S1"), "slave0")

	if Offer(offer) != "slave0#30c49" {
		t.Errorf(`util.NewOffer(util.NewOfferID("487c73d8-9951-f23c-34bd-8085bfd30c49"), util.NewFrameworkID("20150903-065451-84125888-5050-10715-0053"), util.NewSlaveID("20150903-065451-84125888-5050-10715-S1"), "slave0") != "slave0#30c49"; actual %s`, Offer(offer))
	}

	offer.Resources = []*mesos.Resource{util.NewScalarResource("cpus", 4), util.NewScalarResource("mem", 512), util.NewRangesResource("ports", []*mesos.Value_Range{util.NewValueRange(31000, 32000)})}
	if Offer(offer) != "slave0#30c49 cpus:4.00 mem:512.00 ports:[31000..32000]" {
		t.Errorf("Expected slave0#30c49 cpus:4.00 mem:512.00 ports:[31000..32000]; actual %s", Offer(offer))
	}

	offer.Attributes = []*mesos.Attribute{&mesos.Attribute{
		Name:   proto.String("rack"),
		Type:   mesos.Value_SCALAR.Enum(),
		Scalar: &mesos.Value_Scalar{Value: proto.Float64(2)},
	}}
	if Offer(offer) != "slave0#30c49 cpus:4.00 mem:512.00 ports:[31000..32000] rack:2.00" {
		t.Errorf("Expected slave0#30c49 cpus:4.00 mem:512.00 ports:[31000..32000] rack:2.00; actual %s", Offer(offer))
	}

	offer.Resources = nil
	if Offer(offer) != "slave0#30c49 rack:2.00" {
		t.Errorf("Expected slave0#30c49 rack:2.00; actual %s", Offer(offer))
	}
}

func TestOffers(t *testing.T) {
	offer1 := util.NewOffer(util.NewOfferID("487c73d8-9951-f23c-34bd-8085bfd30c49"), util.NewFrameworkID("20150903-065451-84125888-5050-10715-0053"),
		util.NewSlaveID("20150903-065451-84125888-5050-10715-S1"), "slave0")
	offer1.Resources = []*mesos.Resource{util.NewScalarResource("cpus", 4), util.NewScalarResource("mem", 512), util.NewRangesResource("ports", []*mesos.Value_Range{util.NewValueRange(31000, 32000)})}

	offer2 := util.NewOffer(util.NewOfferID("26d5b34c-ef81-638d-5ad5-32c743c9c033"), util.NewFrameworkID("20150903-065451-84125888-5050-10715-0037"),
		util.NewSlaveID("20150903-065451-84125888-5050-10715-S0"), "master")
	offer2.Resources = []*mesos.Resource{util.NewScalarResource("cpus", 2), util.NewScalarResource("mem", 1024), util.NewRangesResource("ports", []*mesos.Value_Range{util.NewValueRange(4000, 7000)})}
	offer2.Attributes = []*mesos.Attribute{&mesos.Attribute{
		Name:   proto.String("rack"),
		Type:   mesos.Value_SCALAR.Enum(),
		Scalar: &mesos.Value_Scalar{Value: proto.Float64(2)},
	}}

	offers := Offers([]*mesos.Offer{offer1, offer2})
	if len(strings.Split(offers, "\n")) != 2 {
		t.Errorf("Offers([]*mesos.Offer{offer1, offer2}) should contain two offers split by new line, actual: %s", offers)
	}
}

func TestStatus(t *testing.T) {
	status := util.NewTaskStatus(util.NewTaskID("task-1"), mesos.TaskState_TASK_FINISHED)
	if !strings.Contains(Status(status), "task-1 TASK_FINISHED reason") {
		t.Errorf(`Task status should contain "task-1 TASK_FINISHED reason"; actual %s`, Status(status))
	}
	status.SlaveId = util.NewSlaveID("20150903-065451-84125888-5050-10715-S1")
	if !strings.Contains(Status(status), "task-1 TASK_FINISHED slave: #15-S1") {
		t.Errorf(`Task status should contain "task-1 TASK_FINISHED slave: #15-S1"; actual %s`, Status(status))
	}
	status.State = mesos.TaskState_TASK_RUNNING.Enum()
	if strings.Contains(Status(status), "reason") {
		t.Errorf(`Task status with running state should not contain "reason"; actual %s`, Status(status))
	}
	status.State = mesos.TaskState_TASK_LOST.Enum()
	status.Reason = mesos.TaskStatus_REASON_EXECUTOR_TERMINATED.Enum()
	if !strings.Contains(Status(status), "task-1 TASK_LOST slave: #15-S1 reason: REASON_EXECUTOR_TERMINATED") {
		t.Errorf(`Task status should contain "task-1 TASK_LOST slave: #15-S1 reason: REASON_EXECUTOR_TERMINATED"; actual %s`, Status(status))
	}

	status.Message = proto.String("boom!")
	if !strings.Contains(Status(status), "message: boom!") {
		t.Errorf(`Task status should contain "message: boom!"; actual %s`, Status(status))
	}
}
