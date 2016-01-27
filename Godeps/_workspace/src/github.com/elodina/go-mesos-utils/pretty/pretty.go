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

/* Package pretty provides human-readable representations for some general Mesos entities. */
package pretty

import (
	"bytes"
	"fmt"
	mesos "github.com/mesos/mesos-go/mesosproto"
	"strings"
)

func suffix(str string, maxLen int) string {
	if len(str) < maxLen {
		return str
	}

	return str[len(str)-maxLen:]
}

func ID(id string) string {
	return fmt.Sprintf("#%s", suffix(id, 5))
}

func Offers(offers []*mesos.Offer) string {
	var offerStrings = make([]string, len(offers))

	for idx, offer := range offers {
		offerStrings[idx] = Offer(offer)
	}

	return strings.Join(offerStrings, "\n")
}

func Offer(offer *mesos.Offer) string {
	var buffer bytes.Buffer

	buffer.WriteString(offer.GetHostname())
	buffer.WriteString(ID(offer.GetId().GetValue()))
	resources := Resources(offer.GetResources())
	if resources != "" {
		buffer.WriteString(" ")
		buffer.WriteString(resources)
	}
	attributes := Attributes(offer.GetAttributes())
	if attributes != "" {
		buffer.WriteString(" ")
		buffer.WriteString(attributes)
	}

	return buffer.String()
}

func Resources(resources []*mesos.Resource) string {
	var buffer bytes.Buffer

	for _, resource := range resources {
		if buffer.Len() != 0 {
			buffer.WriteString(" ")
		}
		buffer.WriteString(Resource(resource))
	}

	return buffer.String()
}

func Resource(resource *mesos.Resource) string {
	var buffer bytes.Buffer

	buffer.WriteString(resource.GetName())
	buffer.WriteString(":")
	if resource.GetScalar() != nil {
		buffer.WriteString(fmt.Sprintf("%.2f", resource.GetScalar().GetValue()))
	}
	if resource.GetRanges() != nil {
		for _, r := range resource.GetRanges().GetRange() {
			buffer.WriteString(fmt.Sprintf("[%d..%d]", r.GetBegin(), r.GetEnd()))
		}
	}

	return buffer.String()
}

func Attributes(attributes []*mesos.Attribute) string {
	var buffer bytes.Buffer

	for _, attr := range attributes {
		if buffer.Len() != 0 {
			buffer.WriteString(";")
		}
		buffer.WriteString(Attribute(attr))
	}

	return buffer.String()
}

func Attribute(attribute *mesos.Attribute) string {
	var buffer bytes.Buffer

	buffer.WriteString(attribute.GetName())
	buffer.WriteString(":")
	if attribute.GetText() != nil {
		buffer.WriteString(attribute.GetText().GetValue())
	}
	if attribute.GetScalar() != nil {
		buffer.WriteString(fmt.Sprintf("%.2f", attribute.GetScalar().GetValue()))
	}

	return buffer.String()
}

func Status(status *mesos.TaskStatus) string {
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("%s %s", status.GetTaskId().GetValue(), status.GetState().String()))
	if status.GetSlaveId().GetValue() != "" {
		buffer.WriteString(" slave: ")
		buffer.WriteString(ID(status.GetSlaveId().GetValue()))
	}

	if status.GetState() != mesos.TaskState_TASK_RUNNING {
		buffer.WriteString(" reason: ")
		buffer.WriteString(status.GetReason().String())
	}

	if status.GetMessage() != "" {
		buffer.WriteString(" message: ")
		buffer.WriteString(status.GetMessage())
	}

	return buffer.String()
}
