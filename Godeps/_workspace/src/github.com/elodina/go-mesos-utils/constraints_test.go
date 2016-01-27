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
	"fmt"
	"github.com/golang/protobuf/proto"
	mesos "github.com/mesos/mesos-go/mesosproto"
	"strings"
	"testing"
)

func TestConstraintParse(t *testing.T) {
	// Like
	constraint, err := ParseConstraint("like:1")
	assertFatal(t, err, nil)
	like, ok := constraint.(*Like)
	assertFatal(t, ok, true)
	assert(t, like.regex, "1")

	// Unlike
	constraint, err = ParseConstraint("unlike:1")
	assertFatal(t, err, nil)
	unlike, ok := constraint.(*Unlike)
	assertFatal(t, ok, true)
	assert(t, unlike.regex, "1")

	// Unique
	constraint, err = ParseConstraint("unique")
	assertFatal(t, err, nil)
	_, ok = constraint.(*Unique)
	assertFatal(t, ok, true)

	// Cluster
	constraint, err = ParseConstraint("cluster")
	assertFatal(t, err, nil)
	cluster, ok := constraint.(*Cluster)
	assertFatal(t, ok, true)
	assert(t, cluster.value, "")

	constraint, err = ParseConstraint("cluster:123")
	assertFatal(t, err, nil)
	cluster123, ok := constraint.(*Cluster)
	assertFatal(t, ok, true)
	assert(t, cluster123.value, "123")

	// Group By
	constraint, err = ParseConstraint("groupBy")
	assertFatal(t, err, nil)
	groupBy, ok := constraint.(*GroupBy)
	assertFatal(t, ok, true)
	assert(t, groupBy.groups, 1)

	constraint, err = ParseConstraint("groupBy:3")
	assertFatal(t, err, nil)
	groupBy3, ok := constraint.(*GroupBy)
	assertFatal(t, ok, true)
	assert(t, groupBy3.groups, 3)

	// Unsupported
	constraint, err = ParseConstraint("unsupported")
	assertNot(t, err, nil)
	if !strings.Contains(err.Error(), "Unsupported constraint") {
		t.Errorf("Error message should contain Unsupported constraint, actual %s", err)
	}
}

func TestConstraintMatches(t *testing.T) {
	testConstraint(t, MustParseConstraint("like:^abc$"), "abc", nil, true)
	testConstraint(t, MustParseConstraint("like:^abc$"), "abc1", nil, false)

	testConstraint(t, MustParseConstraint("like:a.*"), "abc", nil, true)
	testConstraint(t, MustParseConstraint("like:a.*"), "bc", nil, false)

	testConstraint(t, MustParseConstraint("unique"), "a", nil, true)
	testConstraint(t, MustParseConstraint("unique"), "a", []string{"a"}, false)

	testConstraint(t, MustParseConstraint("cluster"), "a", nil, true)
	testConstraint(t, MustParseConstraint("cluster"), "a", []string{"b"}, false)

	testConstraint(t, MustParseConstraint("groupBy"), "a", []string{"a"}, true)
	testConstraint(t, MustParseConstraint("groupBy"), "a", []string{"b"}, false)
}

func testConstraint(t *testing.T, constraint Constraint, value string, values []string, shouldMatch bool) {
	if shouldMatch != constraint.Matches(value, values) {
		t.Errorf("%s match to %s with values %s should be %t, actual %t", constraint, value, values, shouldMatch, !shouldMatch)
	}
}

func TestConstraintString(t *testing.T) {
	assert(t, fmt.Sprintf("%s", MustParseConstraint("like:abc")), "like:abc")
	assert(t, fmt.Sprintf("%s", MustParseConstraint("unlike:abc")), "unlike:abc")
	assert(t, fmt.Sprintf("%s", MustParseConstraint("unique")), "unique")
	assert(t, fmt.Sprintf("%s", MustParseConstraint("cluster")), "cluster")
	assert(t, fmt.Sprintf("%s", MustParseConstraint("cluster:123")), "cluster:123")
	assert(t, fmt.Sprintf("%s", MustParseConstraint("groupBy")), "groupBy")
	assert(t, fmt.Sprintf("%s", MustParseConstraint("groupBy:3")), "groupBy:3")
}

func TestMatchesLike(t *testing.T) {
	like := MustParseConstraint("like:^1.*2$")
	assert(t, like.Matches("12", nil), true)
	assert(t, like.Matches("1a2", nil), true)
	assert(t, like.Matches("1ab2", nil), true)

	assert(t, like.Matches("a1a2", nil), false)
	assert(t, like.Matches("1a2a", nil), false)
}

func TestMatchesUnlike(t *testing.T) {
	unlike := MustParseConstraint("unlike:1")
	assert(t, unlike.Matches("1", nil), false)
	assert(t, unlike.Matches("2", nil), true)
}

func TestMatchesUnique(t *testing.T) {
	unique := MustParseConstraint("unique")
	assert(t, unique.Matches("1", nil), true)
	assert(t, unique.Matches("2", []string{"1"}), true)
	assert(t, unique.Matches("3", []string{"1", "2"}), true)

	assert(t, unique.Matches("1", []string{"1", "2"}), false)
	assert(t, unique.Matches("2", []string{"1", "2"}), false)
}

func TestMatchesCluster(t *testing.T) {
	cluster := MustParseConstraint("cluster")
	assert(t, cluster.Matches("1", nil), true)
	assert(t, cluster.Matches("2", nil), true)

	assert(t, cluster.Matches("1", []string{"1"}), true)
	assert(t, cluster.Matches("1", []string{"1", "1"}), true)
	assert(t, cluster.Matches("2", []string{"1"}), false)

	cluster3 := MustParseConstraint("cluster:3")
	assert(t, cluster3.Matches("3", nil), true)
	assert(t, cluster3.Matches("2", nil), false)

	assert(t, cluster3.Matches("3", []string{"3"}), true)
	assert(t, cluster3.Matches("3", []string{"3", "3"}), true)
	assert(t, cluster3.Matches("2", []string{"3"}), false)
}

func TestMatchesGroupBy(t *testing.T) {
	groupBy := MustParseConstraint("groupBy")
	assert(t, groupBy.Matches("1", nil), true)
	assert(t, groupBy.Matches("1", []string{"1"}), true)
	assert(t, groupBy.Matches("1", []string{"1", "1"}), true)
	assert(t, groupBy.Matches("1", []string{"2"}), false)

	groupBy2 := MustParseConstraint("groupBy:2")
	assert(t, groupBy2.Matches("1", nil), true)
	assert(t, groupBy2.Matches("1", []string{"1"}), false)
	assert(t, groupBy2.Matches("1", []string{"1", "1"}), false)
	assert(t, groupBy2.Matches("2", []string{"1"}), true)

	assert(t, groupBy2.Matches("1", []string{"1", "2"}), true)
	assert(t, groupBy2.Matches("2", []string{"1", "2"}), true)

	assert(t, groupBy2.Matches("1", []string{"1", "1", "2"}), false)
	assert(t, groupBy2.Matches("2", []string{"1", "1", "2"}), true)
}

func TestMatchesAttributes(t *testing.T) {
	task1 := &TestConstrained{
		constraints: map[string][]Constraint{
			"rack": []Constraint{MustParseConstraint("like:^1-.*")},
		},
		attributes: map[string]string{
			"rack": "1-1",
		},
	}

	offer := &mesos.Offer{
		Attributes: []*mesos.Attribute{
			&mesos.Attribute{
				Name: proto.String("rack"),
				Text: &mesos.Value_Text{
					Value: proto.String("1-1"),
				},
			},
		},
	}

	assert(t, CheckConstraints(offer, task1.Constraints(), []Constrained{task1}), "")

	offer.Attributes[0].Text.Value = proto.String("2-1")
	assert(t, CheckConstraints(offer, task1.Constraints(), []Constrained{task1}), "rack doesn't match like:^1-.*")

	task2 := &TestConstrained{
		constraints: map[string][]Constraint{
			"floor": []Constraint{MustParseConstraint("unique")},
		},
		attributes: map[string]string{
			"rack":  "1-1",
			"floor": "1",
		},
	}

	offer.Attributes[0].Name = proto.String("floor")
	offer.Attributes[0].Text.Value = proto.String("1")
	assert(t, CheckConstraints(offer, task2.Constraints(), []Constrained{task2}), "floor doesn't match unique")
}

type TestConstrained struct {
	constraints map[string][]Constraint
	attributes  map[string]string
}

func (tc *TestConstrained) Constraints() map[string][]Constraint {
	return tc.constraints
}

func (tc *TestConstrained) Attribute(name string) string {
	return tc.attributes[name]
}
