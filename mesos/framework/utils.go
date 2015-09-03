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
	crand "crypto/rand"
	"fmt"
	mesos "github.com/mesos/mesos-go/mesosproto"
)

func uuid() string {
	b := make([]byte, 16)
	crand.Read(b)
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

func suffix(str string, maxLen int) string {
	if len(str) < maxLen {
		return str
	}

	return str[len(str)-maxLen:]
}

func idString(id string) string {
	return fmt.Sprintf("#%s", suffix(id, 5))
}

func offersString(offers []*mesos.Offer) string {
	var s string

	for _, offer := range offers {
		s = s + offerString(offer)
	}

	return s
}

func offerString(offer *mesos.Offer) string {
	return fmt.Sprintf("\n%s%s %s %s", offer.GetHostname(), idString(offer.GetId().GetValue()), resourcesString(offer.GetResources()), attributesString(offer.GetAttributes()))
}

func resourcesString(resources []*mesos.Resource) string {
	var s string

	for _, resource := range resources {
		if len(s) != 0 {
			s += " "
		}
		s += resource.GetName() + ":"
		if resource.GetScalar() != nil {
			s += fmt.Sprintf("%.2f", resource.GetScalar().GetValue())
		}
		if resource.GetRanges() != nil {
			for _, r := range resource.GetRanges().GetRange() {
				s += fmt.Sprintf("[%d..%d]", r.GetBegin(), r.GetEnd())
			}
		}
	}

	return s
}

func attributesString(attributes []*mesos.Attribute) string {
	var s string

	for _, attr := range attributes {
		if len(s) != 0 {
			s += ";"
		}
		s += attr.GetName() + ":"
		if attr.GetText() != nil {
			s += attr.GetText().GetValue()
		}
		if attr.GetScalar() != nil {
			s += fmt.Sprintf("%.2f", attr.GetScalar().GetValue())
		}
	}

	return s
}

func statusString(status *mesos.TaskStatus) string {
	s := fmt.Sprintf("%s %s slave: %s", status.GetTaskId().GetValue(), status.GetState().String(), idString(status.GetSlaveId().GetValue()))

	if status.GetState() != mesos.TaskState_TASK_RUNNING {
		s += " reason: " + status.GetReason().String()
	}

	if status.GetMessage() != "" {
		s += " message: " + status.GetMessage()
	}

	return s
}
